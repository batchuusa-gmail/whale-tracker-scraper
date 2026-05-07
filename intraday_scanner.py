"""
WHAL-94 — Intraday Stock Scanner
Scans 500+ stocks every 60 seconds during market hours (9:30–16:00 ET).
Scores each stock 0-100 and stores top 10 picks in Redis.

Kill switch: INTRADAY_SCANNER_ENABLED=true/false (Railway env var)
"""
from __future__ import annotations

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


# ── Stock universe — top 50 liquid stocks (Yahoo Finance compatible) ─
# Reduced from 503 to 50 to avoid rate limits on free-tier data sources.
# These are the highest-volume, most-traded US stocks — best for intraday signals.

SP500 = [
    'AAPL','MSFT','NVDA','AMZN','META','GOOGL','TSLA','AVGO','BRK.B','JPM',
    'V','UNH','XOM','LLY','MA','COST','PG','HD','JNJ','WMT',
    'BAC','ABBV','MRK','NFLX','CRM','AMD','ORCL','CVX','ACN','TMO',
    'ADBE','IBM','QCOM','TXN','NOW','INTU','CSCO','MCD','PEP','MS',
    'GS','AMGN','ISRG','AMAT','PANW','LRCX','CRWD','NET','PLTR','COIN',
    # Market regime indicators (not scored for trades — used for S&P trend)
    'SPY', 'QQQ',
]

NASDAQ100_EXTRA = []  # emptied — using top-52 focused universe only

def _build_universe(insider_tickers: list[str]) -> list[str]:
    """Merge SP500 top-50 + any active insider tickers, deduplicated."""
    seen = set()
    result = []
    for t in SP500 + NASDAQ100_EXTRA + insider_tickers:
        t = t.upper().strip()
        if t and t not in seen:
            seen.add(t)
            result.append(t)
    return result


# ── Market hours ─────────────────────────────────────────────────

# ── US market holidays (hardcoded — no API dependency, no failure modes) ──
# NOTE: Update this set each year. Alpaca calendar was removed because a single
# transient [] response at boot would cache False permanently and kill the scanner.
_US_MARKET_HOLIDAYS = {
    # 2025
    '2025-01-01', '2025-01-20', '2025-02-17', '2025-04-18',
    '2025-05-26', '2025-06-19', '2025-07-04', '2025-09-01',
    '2025-11-27', '2025-12-25',
    # 2026
    '2026-01-01', '2026-01-19', '2026-02-16', '2026-04-03',
    '2026-05-25', '2026-06-19', '2026-07-03', '2026-09-07',
    '2026-11-26', '2026-12-25',
}

def _is_trading_day(date_str: str) -> bool:
    """Return True if date_str is a US market trading day (not a federal holiday)."""
    return date_str not in _US_MARKET_HOLIDAYS


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
    """Fetch last `limit` 1-min bars for one symbol via Yahoo Finance chart API with crumb auth."""
    try:
        sess, crumb = _get_yf_session_crumb()
        params = {'interval': '1m', 'range': '1d'}
        if crumb:
            params['crumb'] = crumb
        r = sess.get(
            f'https://query2.finance.yahoo.com/v8/finance/chart/{sym}',
            params=params,
            timeout=8,
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
    Primary: Alpaca Data API with feed=iex (reliable on Railway server IPs).
    Fallback: Yahoo Finance chart API for any symbols Alpaca misses.
    Returns {symbol: [{'t','o','h','l','c','v'}, ...]}
    """
    results: dict[str, list] = {}

    # ── Primary: Alpaca IEX (server-IP reliable, handles batches of 100) ──
    if ALPACA_KEY and ALPACA_SECRET:
        try:
            hdrs = {
                'APCA-API-KEY-ID':     ALPACA_KEY,
                'APCA-API-SECRET-KEY': ALPACA_SECRET,
            }
            chunk_size = 50
            for i in range(0, len(symbols), chunk_size):
                chunk = symbols[i:i + chunk_size]
                r = requests.get(
                    'https://data.alpaca.markets/v2/stocks/bars',
                    headers=hdrs,
                    params={
                        'symbols':   ','.join(chunk),
                        'timeframe': '1Min',
                        'limit':     limit,
                        'feed':      'iex',
                        'sort':      'asc',
                    },
                    timeout=10,
                )
                if r.status_code == 200:
                    for sym, bar_list in r.json().get('bars', {}).items():
                        bars = [
                            {'t': b['t'], 'o': b['o'], 'h': b['h'],
                             'l': b['l'], 'c': b['c'], 'v': b['v']}
                            for b in bar_list if all(k in b for k in ('o', 'h', 'l', 'c', 'v'))
                        ]
                        if bars:
                            results[sym.upper()] = bars
                elif r.status_code == 403:
                    logger.warning('Alpaca IEX 403 — SIP feed blocked on free tier (expected)')
                else:
                    logger.warning(f'Alpaca bars HTTP {r.status_code}: {r.text[:120]}')
        except Exception as e:
            logger.warning(f'Alpaca bars error: {e}')

    alpaca_count = len(results)

    # ── Fallback: Yahoo Finance for anything Alpaca missed ──
    missing = [s for s in symbols if s not in results]
    if missing:
        import concurrent.futures as _cf
        yf_results = {}
        with _cf.ThreadPoolExecutor(max_workers=20) as ex:
            for sym, bars in ex.map(lambda s: _yf_bars_one(s, limit), missing):
                if bars:
                    yf_results[sym] = bars
        results.update(yf_results)
        logger.info(
            f'Bar fetch: Alpaca={alpaca_count} Yahoo={len(yf_results)} '
            f'total={len(results)}/{len(symbols)} coverage={len(results)*100//len(symbols) if symbols else 0}%'
        )
    else:
        logger.info(
            f'Bar fetch: Alpaca={alpaca_count}/{len(symbols)} coverage=100% (no Yahoo fallback needed)'
        )

    return results


# ── Indicator helpers (using bar lists) ─────────────────────────

def _rsi_from_bars(closes: list[float], period: int = 5) -> float | None:
    """Fast RSI from a short list of closes. Period=5 works with just 6 bars (IEX-friendly)."""
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
    """Bullish momentum signal. Works with as few as 4 bars (IEX-friendly).
    Uses short EMA crossover (3 vs 6) when < 8 bars, full MACD (6 vs 13) otherwise.
    """
    n = len(closes)
    if n < 4:
        return False

    def ema(data, span):
        k = 2 / (span + 1)
        e = data[0]
        for v in data[1:]:
            e = v * k + e * (1 - k)
        return e

    if n >= 8:
        # Full short MACD: EMA(6) - EMA(13) turning positive
        macd_now  = ema(closes, 6) - ema(closes, 13)
        macd_prev = ema(closes[:-1], 6) - ema(closes[:-1], 13) if n > 8 else 0
        return macd_now > 0 and macd_prev <= 0
    else:
        # Simplified reversal: price turned up after 1+ down bars (works with 4-7 bars)
        recent = closes[-4:]
        gains = [recent[i] - recent[i - 1] for i in range(1, 4)]
        return gains[-1] > 0 and gains[-2] <= 0  # reversal up


def _volume_avg(volumes: list[float]) -> float:
    return sum(volumes) / len(volumes) if volumes else 0


# ── Score one symbol ─────────────────────────────────────────────

# ── Quality filters ──────────────────────────────────────────────
MIN_PRICE          = 10.0      # skip extreme penny stocks (was 15 — too strict)
# IEX covers ~3-5% of total market volume. Per-minute IEX volume for a stock with
# 500K real daily volume ≈ 38-65 shares/minute on IEX. Set to 2,000 implied daily
# (≈5 shares/min) to avoid skipping real stocks that simply have thin IEX coverage.
MIN_AVG_DAILY_VOL  = 2_000     # IEX-adjusted (was 20K → still too strict for many stocks)
MAX_DAY_GAIN_PCT   = 8.0       # skip if already up >8% today (was 3% — killed real moves)


def _score_symbol(
    symbol: str,
    bars: list[dict],
    insider_tickers: set[str],
    vol_baselines: dict[str, float],
) -> dict:
    """Score a single symbol 0-100 based on scan criteria."""
    if not bars or len(bars) < 2:
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

# ── Yahoo Finance crumb session (same pattern as options_fetcher) ────────────
_yf_sess_state: dict = {'session': None, 'crumb': None, 'ts': 0}
_YF_SESSION_TTL = 1800  # refresh every 30 min

def _get_yf_session_crumb():
    """Return (requests.Session, crumb_str) with cookie/crumb auth for Yahoo Finance."""
    state = _yf_sess_state
    if state['session'] and state['crumb'] and (time.time() - state['ts']) < _YF_SESSION_TTL:
        return state['session'], state['crumb']
    sess = requests.Session()
    sess.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                      '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
    })
    try:
        sess.get('https://fc.yahoo.com', timeout=8)
    except Exception:
        pass
    try:
        r = sess.get('https://query1.finance.yahoo.com/v1/test/getcrumb', timeout=8)
        crumb = r.text.strip() if r.status_code == 200 else ''
    except Exception:
        crumb = ''
    state['session'] = sess
    state['crumb']   = crumb
    state['ts']      = time.time()
    logger.info(f'Yahoo Finance session refreshed — crumb={crumb[:8]}…' if crumb else 'YF crumb empty')
    return sess, crumb


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
                        'feed': 'sip',
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
    skip_reasons: dict[str, int] = {}
    no_bars_count = 0
    for symbol in universe:
        bars = bars_data.get(symbol, [])
        if not bars:
            no_bars_count += 1
            continue
        scored = _score_symbol(symbol, bars, insider_tickers, _vol_baselines)
        if scored.get('skip'):
            reason = scored.get('skip_reason', 'no_bars')
            skip_reasons[reason[:40]] = skip_reasons.get(reason[:40], 0) + 1
        else:
            # Boost score for pre-market gappers — they already showed conviction
            if symbol in premarket_tickers:
                scored['score'] = min(100, scored['score'] + 15)
                scored['flags'] = scored.get('flags', []) + ['PREMARKET_GAP']
            results.append(scored)

    # Sort by score desc, then momentum desc — always surface best picks even at score=0
    results.sort(key=lambda x: (x['score'], x.get('momentum_pct', 0)), reverse=True)
    top_picks = results[:TOP_N]

    logger.info(
        f'Scan breakdown: universe={len(universe)} iex_coverage={len(bars_data)} '
        f'no_bars={no_bars_count} skipped={sum(skip_reasons.values())} scored={len(results)} '
        f'top_score={top_picks[0]["score"] if top_picks else 0} '
        f'auto_trade_eligible={len([p for p in top_picks if p["score"] >= AUTO_TRADE_MIN_SCORE])} '
        f'skip_reasons={skip_reasons}'
    )

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


AUTO_TRADE_MIN_SCORE   = 50   # lowered: IEX data sparsity limits realistic max to 60-80
AUTO_TRADE_MAX_PER_SCAN = 3   # max per scan to avoid over-trading

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
        logger.info('Auto-trade: INTRADAY_TRADING_ENABLED is not true — skipping')
        return

    try:
        from intraday_ai_scorer import score_stock, MIN_CONFIDENCE
        from intraday_executor import place_intraday_order
    except Exception as e:
        logger.error(f'Auto-trade bridge import error: {e}')
        return

    # Read live config — Redis (highest priority) → Supabase → hardcoded defaults
    app_config: dict = {
        'enabled':            True,
        'min_score':          AUTO_TRADE_MIN_SCORE,
        'min_confidence':     int(MIN_CONFIDENCE * 100),
        'max_trades_per_day': AUTO_TRADE_MAX_PER_SCAN,
        'hold_minutes':       390,   # one trading session default
        'max_positions':      5,
    }
    try:
        import requests as _req
        supa_url = os.environ.get('SUPABASE_URL', 'https://bedurjtazsfbnkisoeee.supabase.co')
        supa_key = os.environ.get('SUPABASE_KEY', '')
        r = _req.get(
            f'{supa_url}/rest/v1/algo_config?limit=1',
            headers={'apikey': supa_key, 'Authorization': f'Bearer {supa_key}'},
            timeout=3,
        )
        if r.status_code == 200 and r.json():
            for k, v in r.json()[0].items():
                if k in app_config and v is not None:
                    app_config[k] = v
    except Exception as e:
        logger.warning(f'algo_config Supabase fetch failed: {e}')

    # Redis overrides everything (set via POST /api/scanner/config from the app)
    try:
        if redis_get:
            redis_cfg = redis_get('algo:live_config')
            if redis_cfg and isinstance(redis_cfg, dict):
                app_config.update(redis_cfg)
                logger.info(f'Loaded live config from Redis: {redis_cfg}')
    except Exception as e:
        logger.warning(f'algo_config Redis fetch failed: {e}')

    if app_config.get('enabled') is False:
        logger.info('Auto-trade disabled via app config')
        return

    min_score_threshold  = int(app_config.get('min_score', AUTO_TRADE_MIN_SCORE))
    min_conf_threshold   = float(app_config.get('min_confidence', int(MIN_CONFIDENCE * 100))) / 100
    max_per_scan         = int(app_config.get('max_trades_per_day', AUTO_TRADE_MAX_PER_SCAN))
    hold_minutes         = int(app_config.get('hold_minutes', 390))
    max_positions        = int(app_config.get('max_positions', 5))

    logger.info(
        f'Auto-trade config: min_score={min_score_threshold} '
        f'min_conf={min_conf_threshold:.0%} max_per_scan={max_per_scan} '
        f'hold_min={hold_minutes} max_pos={max_positions}'
    )

    sp500_trend = _get_sp500_trend(bars_data)
    candidates  = [p for p in top_picks if p.get('score', 0) >= min_score_threshold]

    logger.info(
        f'Auto-trade: trend={sp500_trend} top_picks={len(top_picks)} '
        f'candidates_above_{min_score_threshold}={len(candidates)} '
        f'bars_available={len(bars_data)}'
    )

    if not candidates:
        logger.info('Auto-trade: no candidates met score threshold — no orders this scan')
        return

    attempted   = 0

    for pick in candidates:
        if attempted >= max_per_scan:
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

        if sig not in ('BUY', 'SELL'):
            logger.info(f'Auto-trade skip {ticker}: signal={sig} (not BUY/SELL)')
        elif confidence < min_conf_threshold:
            logger.info(f'Auto-trade skip {ticker}: confidence={confidence:.0%} < threshold={min_conf_threshold:.0%}')

        if sig in ('BUY', 'SELL') and confidence >= min_conf_threshold:
            logger.info(f'Auto-trade FIRING order: {ticker} {sig} confidence={confidence:.0%}')
            order_payload = {
                'ticker':       ticker,
                'signal':       sig,
                'entry':        signal.get('entry', pick.get('price', 0)),
                'stop_loss':    signal.get('stop_loss', 0),
                'target':       signal.get('target', 0),
                'hold_minutes': hold_minutes,   # from live app config
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
    _last_status_log = 0
    while True:
        try:
            enabled = os.environ.get('INTRADAY_SCANNER_ENABLED', 'false').lower() == 'true'
            market_open = is_market_open()
            if enabled and market_open:
                _run_scan(redis_set_fn, redis_get_fn)
            else:
                with _state_lock:
                    _state['running'] = False
                reason = 'market_closed' if not market_open else 'disabled'
                # Log status every 30 minutes to confirm thread is alive
                now_ts = time.time()
                if now_ts - _last_status_log > 1800:
                    now_et = datetime.now(_ET)
                    logger.info(
                        f'Scanner idle — reason={reason} '
                        f'enabled={enabled} market_open={market_open} '
                        f'ET={now_et.strftime("%H:%M")} '
                        f'weekday={now_et.weekday()} '
                        f'date={now_et.strftime("%Y-%m-%d")} '
                        f'is_holiday={now_et.strftime("%Y-%m-%d") in _US_MARKET_HOLIDAYS} '
                        f'scan_count={_state.get("scan_count",0)}'
                    )
                    _last_status_log = now_ts
                # Update Redis status to stopped
                redis_set_fn('scanner:status', {
                    'running':    False,
                    'reason':     reason,
                    'last_scan':  _state.get('last_scan'),
                    'scan_count': _state.get('scan_count', 0),
                }, ttl_seconds=300)
        except Exception as e:
            logger.error(f'Intraday scanner error: {e}', exc_info=True)
            with _state_lock:
                _state['error'] = str(e)
        time.sleep(SCAN_INTERVAL)


def _scanner_watchdog(redis_set_fn, redis_get_fn):
    """Watchdog: restarts the scanner thread if it dies unexpectedly."""
    while True:
        time.sleep(90)  # check every 90s
        alive = any(t.name == 'intraday-scanner' and t.is_alive()
                    for t in threading.enumerate())
        if not alive:
            logger.error('Intraday scanner thread died — restarting...')
            t = threading.Thread(
                target=_scanner_loop,
                args=(redis_set_fn, redis_get_fn),
                daemon=True,
                name='intraday-scanner',
            )
            t.start()


def start(redis_set_fn, redis_get_fn):
    """Start the background scanner thread + watchdog. Call once at app startup."""
    t = threading.Thread(
        target=_scanner_loop,
        args=(redis_set_fn, redis_get_fn),
        daemon=True,
        name='intraday-scanner',
    )
    t.start()
    # Watchdog restarts scanner if it ever dies
    threading.Thread(
        target=_scanner_watchdog,
        args=(redis_set_fn, redis_get_fn),
        daemon=True,
        name='scanner-watchdog',
    ).start()
    logger.info('Intraday scanner: background thread + watchdog launched')


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
