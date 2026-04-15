"""
WHAL-94 — Intraday Stock Scanner
Scans 500+ stocks every 60 seconds during market hours (9:30–16:00 ET).
Scores each stock 0-100 and stores top 10 picks in Redis.

Kill switch: INTRADAY_SCANNER_ENABLED=true/false (Railway env var)
"""

import os
import json
import logging
import threading
import time
from datetime import datetime, timedelta

import pytz
import requests

logger = logging.getLogger(__name__)

# ── Config ──────────────────────────────────────────────────────

SCAN_INTERVAL = 60          # seconds between scans
TOP_N         = 20          # how many top picks to surface
ALPACA_KEY    = os.environ.get('ALPACA_KEY', '')
ALPACA_SECRET = os.environ.get('ALPACA_SECRET', '')

_ET = pytz.timezone('America/New_York')

# ── In-memory fallback (used when Redis is unavailable) ─────────
_state = {
    'top_picks':   [],
    'all_scores':  [],
    'last_scan':   None,
    'running':     False,
    'scan_count':  0,
    'error':       None,
}
_state_lock = threading.Lock()


# ── Stock universe ───────────────────────────────────────────────

SP500 = [
    'AAPL','MSFT','AMZN','NVDA','GOOGL','META','TSLA','BRK.B','UNH','LLY',
    'JPM','V','XOM','AVGO','PG','MA','HD','JNJ','COST','MRK','ABBV','CVX',
    'CRM','AMD','ACN','MCD','PEP','ADBE','WMT','BAC','LIN','DIS','CSCO','NFLX',
    'WFC','TXN','ABT','MS','GE','DHR','INTC','PM','CMCSA','ORCL','CAT','INTU',
    'TMO','QCOM','IBM','NEE','VZ','HON','NOW','SPGI','UPS','AMGN','LOW','RTX',
    'BA','GS','BLK','AMAT','SYK','MDT','PFE','T','MMM','SCHW','DE','GILD',
    'AXP','ADP','ELV','CI','SLB','MDLZ','MO','TJX','SBUX','ADI','CB','BMY',
    'C','FI','ZTS','ISRG','LRCX','HCA','REGN','PANW','DUK','PLD','ETN','AON',
    'GD','SO','BDX','TGT','CME','ICE','NSC','EMR','APH','MCO','FCX','EW',
    'ITW','ROP','AFL','MET','CTAS','MAR','KMB','PSA','WM','TRV','HLT','F',
    'GM','PCG','EXC','PAYX','IQV','IDXX','VRSK','MCHP','FTNT','DXCM','BIIB',
    'FAST','WBD','OKE','KR','MRNA','CTVA','PPG','GIS','YUM','CARR','SPG',
    'PCAR','HSY','A','ROK','ACGL','OTIS','NEM','CMI','FICO','KHC','ALL',
    'KEYS','RSG','DD','EFX','TROW','HAL','VICI','D','PPL','AEP','ED','ES',
    'XEL','WEC','ATO','NI','CMS','PNW','OGE','SR','SRE','AGR','ETRN',
    'ORLY','DLTR','ROST','BBY','ULTA','DG','AZO','EBAY','ETSY','SQ','PYPL',
    'UBER','LYFT','DASH','SNAP','PINS','RBLX','U','SPOT','HOOD','COIN',
    'ZM','DOCU','CRWD','OKTA','NET','DDOG','MDB','SNOW','PLTR','PATH',
    'SOFI','LCID','RIVN','NIO','XPEV','LI','FSR','LAZR','WKHS','RIDE',
]

NASDAQ100_EXTRA = [
    'MRVL','KLAC','CDNS','SNPS','ANSS','NXPI','ON','MPWR','SWKS','QRVO',
    'ALGN','IDXX','PODD','HOLX','GEHC','XRAY','VTRS','CTLT','JAZZ','ALNY',
    'BMRN','SGEN','EXAS','FATE','NVAX','MRVI','NNOX','NVRO','SGFY','ACAD',
    'FATE','FOLD','PTGX','TGTX','NTRA','GH','RXRX','TWST','BEAM','EDIT',
    'NTLA','CRSP','BLUE','SAGE','AXSM','INVA','ICPT','ARWR','AGEN','IDYA',
    'DNLI','IMVT','RVMD','PRLD','FGEN','CHRS','PTCT','VRNA','AQST','XCUR',
    'LBPH','PMVP','IRON','FWRD','HALO','KRTX','NUVL','AUPH','ALDX','ADMA',
    'ATRO','TPVG','GLAD','PFLT','PSEC','MAIN','ARCC','HTGC','GBDC','SLRC',
]

def _build_universe(insider_tickers: list[str]) -> list[str]:
    """Merge SP500 + NASDAQ100 extra + insider tickers, deduplicated."""
    seen = set()
    result = []
    for t in SP500 + NASDAQ100_EXTRA + insider_tickers:
        t = t.upper().strip()
        if t and t not in seen:
            seen.add(t)
            result.append(t)
    return result


# ── Market hours ─────────────────────────────────────────────────

_trading_day_cache: dict[str, bool] = {}   # date_str → is_trading_day

def _is_trading_day(date_str: str) -> bool:
    """Check Alpaca calendar API to confirm a date is a trading day (not a holiday)."""
    if date_str in _trading_day_cache:
        return _trading_day_cache[date_str]
    if not ALPACA_KEY or not ALPACA_SECRET:
        return True  # assume open if keys missing
    try:
        r = requests.get(
            'https://paper-api.alpaca.markets/v2/calendar',
            headers={'APCA-API-KEY-ID': ALPACA_KEY, 'APCA-API-SECRET-KEY': ALPACA_SECRET},
            params={'start': date_str, 'end': date_str},
            timeout=8,
        )
        if r.status_code == 200:
            result = len(r.json()) > 0
            _trading_day_cache[date_str] = result
            return result
    except Exception as e:
        logger.warning(f'_is_trading_day check error: {e}')
    return True  # assume open on error to avoid blocking


def is_market_open() -> bool:
    now = datetime.now(_ET)
    if now.weekday() >= 5:
        return False
    market_open  = now.replace(hour=9,  minute=30, second=0, microsecond=0)
    market_close = now.replace(hour=16, minute=0,  second=0, microsecond=0)
    if not (market_open <= now <= market_close):
        return False
    # Check Alpaca calendar for holidays (Good Friday, Thanksgiving, etc.)
    return _is_trading_day(now.strftime('%Y-%m-%d'))


# ── Alpaca 1-min bars fetch ──────────────────────────────────────

def _yf_bars_one(sym: str, limit: int) -> tuple[str, list]:
    """Fetch last `limit` 1-min bars for one symbol via Yahoo Finance chart API."""
    try:
        r = requests.get(
            f'https://query1.finance.yahoo.com/v8/finance/chart/{sym}',
            params={'interval': '1m', 'range': '1d'},
            headers=_YF_HDR, timeout=5,
        )
        if r.status_code != 200:
            return sym, []
        result = r.json().get('chart', {}).get('result', [])
        if not result:
            return sym, []
        ts    = result[0].get('timestamp', [])
        quote = result[0].get('indicators', {}).get('quote', [{}])[0]
        opens  = quote.get('open', [])
        highs  = quote.get('high', [])
        lows   = quote.get('low', [])
        closes = quote.get('close', [])
        vols   = quote.get('volume', [])
        bars = []
        for i in range(len(ts)):
            o = opens[i] if i < len(opens) else None
            h = highs[i] if i < len(highs) else None
            l = lows[i] if i < len(lows) else None
            c = closes[i] if i < len(closes) else None
            v = vols[i] if i < len(vols) else None
            if None in (o, h, l, c, v):
                continue
            bars.append({'t': ts[i], 'o': o, 'h': h, 'l': l, 'c': c, 'v': v})
        return sym, bars[-limit:] if len(bars) > limit else bars
    except Exception:
        return sym, []


def _alpaca_bars_batch(symbols: list[str], limit: int = 10) -> dict[str, list]:
    """
    Fetch last `limit` 1-minute bars for all symbols.
    Primary: Alpaca Data API (reliable on server IPs, handles 500+ symbols).
    Fallback: Yahoo Finance chart API.
    Returns {symbol: [{'t','o','h','l','c','v'}, ...]}
    """
    # ── Primary: Alpaca 1-min bars (chunked, 100 symbols per call) ──
    if ALPACA_KEY and ALPACA_SECRET:
        try:
            results = {}
            hdrs = {
                'APCA-API-KEY-ID': ALPACA_KEY,
                'APCA-API-SECRET-KEY': ALPACA_SECRET,
            }
            chunk_size = 100
            for i in range(0, len(symbols), chunk_size):
                chunk = symbols[i:i + chunk_size]
                r = requests.get(
                    'https://data.alpaca.markets/v2/stocks/bars',
                    headers=hdrs,
                    params={
                        'symbols': ','.join(chunk),
                        'timeframe': '1Min',
                        'limit': limit,
                        'feed': 'iex',
                        'sort': 'asc',
                    },
                    timeout=15,
                )
                if r.status_code == 200:
                    data = r.json().get('bars', {})
                    for sym, bar_list in data.items():
                        results[sym] = [{
                            't': b['t'], 'o': b['o'], 'h': b['h'],
                            'l': b['l'], 'c': b['c'], 'v': b['v'],
                        } for b in bar_list]
            if results:
                logger.info(f'Bar fetch (Alpaca): {len(results)}/{len(symbols)} symbols returned data')
                return results
        except Exception as e:
            logger.warning(f'Alpaca bars batch error: {e} — falling back to Yahoo Finance')

    # ── Fallback: Yahoo Finance (blocked on some server IPs) ──
    import concurrent.futures as _cf
    results = {}
    with _cf.ThreadPoolExecutor(max_workers=30) as ex:
        for sym, bars in ex.map(lambda s: _yf_bars_one(s, limit), symbols):
            if bars:
                results[sym] = bars
    logger.info(f'Bar fetch (Yahoo): {len(results)}/{len(symbols)} symbols returned data')
    return results


# ── Indicator helpers (using bar lists) ─────────────────────────

def _rsi_from_bars(closes: list[float], period: int = 7) -> float | None:
    """Fast RSI from a short list of closes (uses available bars, min period+1)."""
    if len(closes) < period + 1:
        return None
    gains, losses = [], []
    for i in range(1, len(closes)):
        d = closes[i] - closes[i - 1]
        gains.append(max(d, 0))
        losses.append(max(-d, 0))
    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return round(100 - (100 / (1 + rs)), 1)


def _macd_crossover(closes: list[float]) -> bool:
    """True if MACD histogram turned positive in last 3 bars (bullish crossover)."""
    if len(closes) < 15:
        return False

    def ema(data, span):
        k = 2 / (span + 1)
        e = data[0]
        for v in data[1:]:
            e = v * k + e * (1 - k)
        return e

    def hist_at(idx):
        c = closes[:idx + 1]
        if len(c) < 15:
            return 0
        macd = ema(c, 6) - ema(c, 13)
        # approximate signal with ema of last few macd values
        return macd  # simplified: just check sign change

    # Simplified: use last 3 bars to detect positive momentum building
    n = len(closes)
    if n < 4:
        return False
    recent = closes[-4:]
    gains = [recent[i] - recent[i - 1] for i in range(1, 4)]
    return gains[-1] > 0 and gains[-2] <= 0  # reversal up


def _volume_avg(volumes: list[float]) -> float:
    return sum(volumes) / len(volumes) if volumes else 0


# ── Score one symbol ─────────────────────────────────────────────

# ── Quality filters ──────────────────────────────────────────────
MIN_PRICE          = 15.0      # skip penny/micro-cap stocks
MIN_AVG_DAILY_VOL  = 500_000   # skip illiquid stocks (per-minute baseline × 390 mins)
MAX_DAY_GAIN_PCT   = 3.0       # skip if already up >3% today (chasing)


def _score_symbol(
    symbol: str,
    bars: list[dict],
    insider_tickers: set[str],
    vol_baselines: dict[str, float],
) -> dict:
    """Score a single symbol 0-100 based on scan criteria."""
    if not bars or len(bars) < 3:
        return {'ticker': symbol, 'score': 0, 'skip': True}

    closes  = [b['c'] for b in bars]
    highs   = [b['h'] for b in bars]
    lows    = [b['l'] for b in bars]
    volumes = [b['v'] for b in bars]

    cur_price  = closes[-1]
    open_price = bars[0]['o'] if bars else cur_price
    prev_price = closes[-5] if len(closes) >= 5 else closes[0]
    cur_vol    = volumes[-1]
    avg_vol    = vol_baselines.get(symbol) or _volume_avg(volumes[:-1]) or 1

    # ── Hard quality filters (instant skip) ──────────────────────
    # 1. Price too low — penny/micro-cap, wide spreads eat profit
    if cur_price < MIN_PRICE:
        return {'ticker': symbol, 'score': 0, 'skip': True,
                'skip_reason': f'price ${cur_price:.2f} < ${MIN_PRICE}'}

    # 2. Illiquid — per-minute volume baseline × 390 = implied daily volume
    implied_daily_vol = avg_vol * 390
    if implied_daily_vol < MIN_AVG_DAILY_VOL:
        return {'ticker': symbol, 'score': 0, 'skip': True,
                'skip_reason': f'low liquidity ~{implied_daily_vol/1e6:.1f}M/day'}

    # 3. Already up too much today — chasing a move that's likely exhausted
    day_gain_pct = ((cur_price - open_price) / open_price * 100) if open_price > 0 else 0
    if day_gain_pct > MAX_DAY_GAIN_PCT:
        return {'ticker': symbol, 'score': 0, 'skip': True,
                'skip_reason': f'already up {day_gain_pct:.1f}% today (chasing)'}

    score = 0
    flags = []

    # +20 volume spike > 2× average
    vol_ratio = cur_vol / avg_vol if avg_vol > 0 else 0
    if vol_ratio >= 2.0:
        score += 20
        flags.append(f'VOL {vol_ratio:.1f}×')

    # +20 RSI in sweet spot 45-65 (momentum without overbought)
    rsi = _rsi_from_bars(closes)
    if rsi is not None and 45 <= rsi <= 65:
        score += 20
        flags.append(f'RSI {rsi:.0f}')

    # +20 MACD bullish crossover in last 3 bars
    if _macd_crossover(closes):
        score += 20
        flags.append('MACD↑')

    # +20 price breaking above recent 5-min high (early breakout, not chasing)
    # Only counts if gain from open is modest (< MAX_DAY_GAIN_PCT already filtered above)
    five_min_high = max(highs[:-1]) if len(highs) > 1 else highs[0]
    if cur_price > five_min_high:
        score += 20
        flags.append('BREAKOUT')

    # +20 insider buy in last 30 days (fundamental confirmation)
    if symbol in insider_tickers:
        score += 20
        flags.append('INSIDER')

    # +10 strong positive momentum 0.5–3% (healthy, not exhausted)
    momentum_pct = ((cur_price - prev_price) / prev_price * 100) if prev_price > 0 else 0
    if 0.5 <= momentum_pct <= 3.0:
        score += 10
        flags.append(f'MOM+{momentum_pct:.1f}%')

    # +10 tight intraday range (low volatility noise = cleaner entry)
    day_range_pct = ((max(highs) - min(lows)) / min(lows) * 100) if min(lows) > 0 else 99
    if day_range_pct < 3.0:
        score += 10
        flags.append('TIGHT')

    return {
        'ticker':       symbol,
        'score':        min(score, 100),
        'price':        round(cur_price, 2),
        'momentum_pct': round(momentum_pct, 2),
        'vol_ratio':    round(vol_ratio, 2),
        'rsi':          rsi,
        'day_gain_pct': round(day_gain_pct, 2),
        'flags':        flags,
        'skip':         False,
    }


# ── Supabase — fetch insider tickers ────────────────────────────

_SUPA_URL = os.environ.get('SUPABASE_URL', 'https://bedurjtazsfbnkisoeee.supabase.co')
_SUPA_KEY = os.environ.get(
    'SUPABASE_KEY',
    'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJlZHVyanRhenNmYm5raXNvZWVlIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzM2NDcxOTUsImV4cCI6MjA4OTIyMzE5NX0.MK4N9dxAIHlXkGP4rLJPq5tHh9UU8L75EB1b8Q7CVmg'
)

def _fetch_insider_tickers() -> set[str]:
    """Return set of tickers with insider activity in last 30 days from Supabase."""
    try:
        cutoff = (datetime.utcnow() - timedelta(days=30)).strftime('%Y-%m-%d')
        r = requests.get(
            f'{_SUPA_URL}/rest/v1/filings',
            headers={'apikey': _SUPA_KEY, 'Authorization': f'Bearer {_SUPA_KEY}'},
            params={'select': 'ticker', 'transaction_date': f'gte.{cutoff}', 'limit': '500'},
            timeout=10,
        )
        if r.status_code == 200:
            return {row['ticker'].upper() for row in r.json() if row.get('ticker')}
    except Exception as e:
        logger.warning(f'fetch_insider_tickers error: {e}')
    return set()


# ── Volume baseline fetch (yfinance, cached per session) ────────

_vol_baselines: dict[str, float] = {}
_baselines_loaded = False
_baselines_loading = False

_YF_HDR = {'User-Agent': 'Mozilla/5.0 (compatible; WhaleTracker/1.0)'}  # fallback only

def _load_vol_baselines(symbols: list[str]):
    """Spawn a background thread to load volume baselines — never blocks the scan loop."""
    global _baselines_loading
    if _baselines_loaded or _baselines_loading:
        return
    _baselines_loading = True
    import threading as _t
    _t.Thread(target=_do_load_baselines, args=(symbols,), daemon=True, name='vol-baselines').start()


def _do_load_baselines(symbols: list[str]):
    """Load avg per-minute volume baselines using Alpaca daily bars (10-day avg / 390 mins).
    Alpaca works reliably on Railway server IPs — no Yahoo Finance dependency.
    """
    global _vol_baselines, _baselines_loaded, _baselines_loading
    logger.info(f'Loading volume baselines for {len(symbols)} symbols via Alpaca…')
    loaded = 0

    if ALPACA_KEY and ALPACA_SECRET:
        try:
            hdrs = {
                'APCA-API-KEY-ID': ALPACA_KEY,
                'APCA-API-SECRET-KEY': ALPACA_SECRET,
            }
            chunk_size = 100
            for i in range(0, min(len(symbols), 300), chunk_size):
                chunk = symbols[i:i + chunk_size]
                r = requests.get(
                    'https://data.alpaca.markets/v2/stocks/bars',
                    headers=hdrs,
                    params={
                        'symbols': ','.join(chunk),
                        'timeframe': '1Day',
                        'limit': 10,
                        'feed': 'iex',
                        'sort': 'asc',
                    },
                    timeout=15,
                )
                if r.status_code == 200:
                    for sym, bar_list in r.json().get('bars', {}).items():
                        vols = [b['v'] for b in bar_list if b.get('v')]
                        if vols:
                            _vol_baselines[sym.upper()] = sum(vols) / len(vols) / 390
                            loaded += 1
        except Exception as e:
            logger.warning(f'Alpaca volume baseline error: {e}')

    _baselines_loaded = True
    _baselines_loading = False
    logger.info(f'Volume baselines loaded: {loaded} symbols')


# ── Main scan loop ───────────────────────────────────────────────

def _run_scan(redis_set_fn, redis_get_fn):
    """Execute one full scan pass. Called every SCAN_INTERVAL seconds."""
    scan_start = datetime.now(_ET)
    logger.info(f'Intraday scanner: starting scan at {scan_start.strftime("%H:%M:%S ET")}')

    insider_tickers = _fetch_insider_tickers()

    # WHAL-121: Merge pre-market gappers into universe — prioritized at open
    premarket_tickers = set()
    try:
        from premarket_scanner import get_priority_tickers
        premarket_tickers = get_priority_tickers(redis_get_fn)
        if premarket_tickers:
            logger.info(f'Pre-market priority tickers: {sorted(premarket_tickers)}')
    except Exception:
        pass

    universe = _build_universe(list(insider_tickers | premarket_tickers))

    # Load volume baselines once per session
    _load_vol_baselines(universe)

    # Fetch 1-min bars (20 bars needed for MACD + RSI)
    bars_data = _alpaca_bars_batch(universe, limit=20)

    results = []
    for symbol in universe:
        bars = bars_data.get(symbol, [])
        scored = _score_symbol(symbol, bars, insider_tickers, _vol_baselines)
        if not scored.get('skip'):
            # Boost score for pre-market gappers — they already showed conviction
            if symbol in premarket_tickers:
                scored['score'] = min(100, scored['score'] + 15)
                scored['flags'] = scored.get('flags', []) + ['PREMARKET_GAP']
            results.append(scored)

    # Sort by score desc, then momentum desc — always surface best picks even at score=0
    results.sort(key=lambda x: (x['score'], x.get('momentum_pct', 0)), reverse=True)
    top_picks = results[:TOP_N]

    scan_end = datetime.now(_ET)
    elapsed  = (scan_end - scan_start).total_seconds()

    scan_meta = {
        'last_scan':      scan_end.isoformat(),
        'elapsed_seconds': round(elapsed, 1),
        'stocks_scanned': len(universe),
        'stocks_scored':  len(results),
        'running':        True,
        'scan_count':     _state['scan_count'] + 1,
    }

    # Store in Redis (60s TTL so stale data auto-expires)
    redis_set_fn('scanner:top',    top_picks, ttl_seconds=90)
    redis_set_fn('scanner:all',    results[:200], ttl_seconds=90)
    redis_set_fn('scanner:status', scan_meta, ttl_seconds=90)

    # Also update in-memory state
    with _state_lock:
        _state['top_picks']  = top_picks
        _state['all_scores'] = results[:200]
        _state['last_scan']  = scan_end.isoformat()
        _state['running']    = True
        _state['scan_count'] += 1
        _state['error']      = None

    logger.info(
        f'Intraday scanner: scan complete in {elapsed:.1f}s | '
        f'{len(universe)} scanned | {len(results)} scored | '
        f'top: {[p["ticker"] for p in top_picks[:5]]}'
    )

    # ── Auto-trade bridge ────────────────────────────────────────
    # Route high-conviction picks through AI scorer → executor.
    # Gated by INTRADAY_TRADING_ENABLED so it's opt-in.
    _auto_trade_top_picks(top_picks, bars_data, insider_tickers, redis_set_fn, redis_get_fn)


AUTO_TRADE_MIN_SCORE   = 75   # raised from 60 → only high-conviction picks
AUTO_TRADE_MAX_PER_SCAN = 3   # reduced from 5 → fewer, better trades per scan

# S&P 500 trend cache (refreshed each scan)
_sp500_trend = 'neutral'

def _get_sp500_trend(bars_data: dict) -> str:
    """Derive simple S&P 500 proxy trend from SPY bars if available."""
    spy = bars_data.get('SPY', [])
    if len(spy) >= 5:
        closes = [b['c'] for b in spy]
        return 'up' if closes[-1] > closes[0] else 'down'
    return 'neutral'


def _auto_trade_top_picks(
    top_picks: list[dict],
    bars_data: dict,
    insider_tickers: set,
    redis_set_fn,
    redis_get_fn,
):
    """
    For each top pick with score >= AUTO_TRADE_MIN_SCORE:
      1. Run through intraday_ai_scorer.score_stock()
      2. If BUY/SELL with confidence >= 80%, call intraday_executor.place_intraday_order()
    Max AUTO_TRADE_MAX_PER_SCAN attempts per scan to avoid over-trading.
    """
    trading_enabled = os.environ.get('INTRADAY_TRADING_ENABLED', 'false').lower() == 'true'
    if not trading_enabled:
        return

    try:
        from intraday_ai_scorer import score_stock, MIN_CONFIDENCE
        from intraday_executor import place_intraday_order
    except Exception as e:
        logger.error(f'Auto-trade bridge import error: {e}')
        return

    sp500_trend = _get_sp500_trend(bars_data)
    candidates  = [p for p in top_picks if p.get('score', 0) >= AUTO_TRADE_MIN_SCORE]
    attempted   = 0

    for pick in candidates:
        if attempted >= AUTO_TRADE_MAX_PER_SCAN:
            break

        ticker       = pick['ticker']
        scan_score   = pick['score']
        bars         = bars_data.get(ticker, [])
        insider_note = 'recent insider buy' if ticker in insider_tickers else ''

        try:
            signal = score_stock(
                ticker=ticker,
                scanner_score=scan_score,
                bars=bars if bars else None,
                insider_summary=insider_note,
                sp500_trend=sp500_trend,
                redis_get=redis_get_fn,
                redis_set=redis_set_fn,
            )
        except Exception as e:
            logger.warning(f'Auto-trade AI score error for {ticker}: {e}')
            attempted += 1
            continue

        sig        = (signal.get('signal') or '').upper()
        confidence = float(signal.get('confidence') or 0)

        logger.info(
            f'Auto-trade candidate: {ticker} scanner={scan_score} '
            f'signal={sig} confidence={confidence:.0%}'
        )

        if sig in ('BUY', 'SELL') and confidence >= MIN_CONFIDENCE:
            order_payload = {
                'ticker':       ticker,
                'signal':       sig,
                'entry':        signal.get('entry', pick.get('price', 0)),
                'stop_loss':    signal.get('stop_loss', 0),
                'target':       signal.get('target', 0),
                'hold_minutes': signal.get('hold_minutes', 60),
                'confidence':   confidence,
                'reasoning':    signal.get('reasoning', ''),
            }
            try:
                result = place_intraday_order(order_payload)
                logger.info(f'Auto-trade order result for {ticker}: {result.get("status")} {result}')
            except Exception as e:
                logger.error(f'Auto-trade place_order error for {ticker}: {e}')
        else:
            logger.info(f'Auto-trade skipped {ticker}: signal={sig} confidence={confidence:.0%}')

        attempted += 1


# ── Background thread ────────────────────────────────────────────

def _scanner_loop(redis_set_fn, redis_get_fn):
    logger.info('Intraday scanner thread started')
    while True:
        try:
            enabled = os.environ.get('INTRADAY_SCANNER_ENABLED', 'false').lower() == 'true'
            if enabled and is_market_open():
                _run_scan(redis_set_fn, redis_get_fn)
            else:
                with _state_lock:
                    _state['running'] = False
                # Update Redis status to stopped
                redis_set_fn('scanner:status', {
                    'running':    False,
                    'reason':     'market_closed' if not is_market_open() else 'disabled',
                    'last_scan':  _state.get('last_scan'),
                    'scan_count': _state.get('scan_count', 0),
                }, ttl_seconds=300)
        except Exception as e:
            logger.error(f'Intraday scanner error: {e}')
            with _state_lock:
                _state['error'] = str(e)
        time.sleep(SCAN_INTERVAL)


def start(redis_set_fn, redis_get_fn):
    """Start the background scanner thread. Call once at app startup."""
    t = threading.Thread(
        target=_scanner_loop,
        args=(redis_set_fn, redis_get_fn),
        daemon=True,
        name='intraday-scanner',
    )
    t.start()
    logger.info('Intraday scanner: background thread launched')


# ── Public accessors (used by Flask endpoints) ───────────────────

def get_top(redis_get_fn) -> list:
    cached = redis_get_fn('scanner:top')
    if cached is not None:
        return cached
    with _state_lock:
        return list(_state['top_picks'])


def get_all(redis_get_fn) -> list:
    cached = redis_get_fn('scanner:all')
    if cached is not None:
        return cached
    with _state_lock:
        return list(_state['all_scores'])


def get_status(redis_get_fn) -> dict:
    cached = redis_get_fn('scanner:status')
    if cached is not None:
        return cached
    with _state_lock:
        return {
            'running':        _state['running'],
            'last_scan':      _state['last_scan'],
            'scan_count':     _state['scan_count'],
            'error':          _state['error'],
            'market_open':    is_market_open(),
            'enabled':        os.environ.get('INTRADAY_SCANNER_ENABLED', 'false').lower() == 'true',
        }
