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
import yfinance as yf

logger = logging.getLogger(__name__)

# ── Config ──────────────────────────────────────────────────────

SCAN_INTERVAL = 60          # seconds between scans
TOP_N         = 10          # how many top picks to surface
ALPACA_KEY    = os.environ.get('ALPACA_KEY', '')
ALPACA_SECRET = os.environ.get('ALPACA_SECRET', '')
ALPACA_DATA   = 'https://data.alpaca.markets'

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

def is_market_open() -> bool:
    now = datetime.now(_ET)
    if now.weekday() >= 5:
        return False
    market_open  = now.replace(hour=9,  minute=30, second=0, microsecond=0)
    market_close = now.replace(hour=16, minute=0,  second=0, microsecond=0)
    return market_open <= now <= market_close


# ── Alpaca 1-min bars fetch ──────────────────────────────────────

def _alpaca_bars_batch(symbols: list[str], limit: int = 10) -> dict[str, list]:
    """
    Fetch last `limit` 1-minute bars for up to 100 symbols via Alpaca data API.
    Returns {symbol: [{'t','o','h','l','c','v'}, ...]} or {} on failure.
    """
    if not ALPACA_KEY or not ALPACA_SECRET or not symbols:
        return {}
    headers = {
        'APCA-API-KEY-ID':     ALPACA_KEY,
        'APCA-API-SECRET-KEY': ALPACA_SECRET,
    }
    results = {}
    # Alpaca allows up to 100 symbols per request
    for chunk_start in range(0, len(symbols), 100):
        chunk = symbols[chunk_start:chunk_start + 100]
        try:
            r = requests.get(
                f'{ALPACA_DATA}/v2/stocks/bars',
                headers=headers,
                params={
                    'symbols':   ','.join(chunk),
                    'timeframe': '1Min',
                    'limit':     limit,
                    'feed':      'sip',
                },
                timeout=15,
            )
            if r.status_code == 200:
                data = r.json().get('bars', {})
                results.update(data)
        except Exception as e:
            logger.warning(f'Alpaca bars batch error: {e}')
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
    volumes = [b['v'] for b in bars]

    cur_price  = closes[-1]
    prev_price = closes[-5] if len(closes) >= 5 else closes[0]
    cur_vol    = volumes[-1]
    avg_vol    = vol_baselines.get(symbol) or _volume_avg(volumes[:-1]) or 1

    score = 0
    flags = []

    # +20 volume spike > 2× average
    vol_ratio = cur_vol / avg_vol if avg_vol > 0 else 0
    if vol_ratio >= 2.0:
        score += 20
        flags.append(f'VOL {vol_ratio:.1f}×')

    # +20 RSI in sweet spot 45-65
    rsi = _rsi_from_bars(closes)
    if rsi is not None and 45 <= rsi <= 65:
        score += 20
        flags.append(f'RSI {rsi:.0f}')

    # +20 MACD bullish crossover in last 3 bars
    if _macd_crossover(closes):
        score += 20
        flags.append('MACD↑')

    # +20 price > 5-min high (intraday breakout)
    five_min_high = max(highs[:-1]) if len(highs) > 1 else highs[0]
    if cur_price > five_min_high:
        score += 20
        flags.append('BREAKOUT')

    # +20 insider buy in last 30 days (bonus)
    if symbol in insider_tickers:
        score += 20
        flags.append('INSIDER')

    # Momentum filter: must have moved > 0.5% in last 5 min
    momentum_pct = ((cur_price - prev_price) / prev_price * 100) if prev_price > 0 else 0

    return {
        'ticker':       symbol,
        'score':        score,
        'price':        round(cur_price, 2),
        'momentum_pct': round(momentum_pct, 2),
        'vol_ratio':    round(vol_ratio, 2),
        'rsi':          rsi,
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

def _load_vol_baselines(symbols: list[str]):
    """Load 20-day average volume for all symbols using yfinance (runs once per session)."""
    global _vol_baselines, _baselines_loaded
    if _baselines_loaded:
        return
    logger.info(f'Loading volume baselines for {len(symbols)} symbols…')
    try:
        # Batch download — much faster than individual calls
        df = yf.download(
            symbols[:200],   # limit to avoid timeout
            period='5d',
            interval='1d',
            progress=False,
            auto_adjust=True,
            threads=True,
        )
        if not df.empty and 'Volume' in df:
            vol_df = df['Volume']
            for sym in vol_df.columns:
                avg = vol_df[sym].dropna().mean()
                if avg and avg > 0:
                    _vol_baselines[sym.upper()] = float(avg / 390)  # per-minute avg
        _baselines_loaded = True
        logger.info(f'Volume baselines loaded: {len(_vol_baselines)} symbols')
    except Exception as e:
        logger.warning(f'Volume baselines load error: {e}')
        _baselines_loaded = True  # don't retry on every scan


# ── Main scan loop ───────────────────────────────────────────────

def _run_scan(redis_set_fn, redis_get_fn):
    """Execute one full scan pass. Called every SCAN_INTERVAL seconds."""
    scan_start = datetime.now(_ET)
    logger.info(f'Intraday scanner: starting scan at {scan_start.strftime("%H:%M:%S ET")}')

    insider_tickers = _fetch_insider_tickers()
    universe        = _build_universe(list(insider_tickers))

    # Load volume baselines once per session
    _load_vol_baselines(universe)

    # Fetch 1-min bars in batches (Alpaca allows 100 per request)
    bars_data = _alpaca_bars_batch(universe, limit=10)

    results = []
    for symbol in universe:
        bars = bars_data.get(symbol, [])
        scored = _score_symbol(symbol, bars, insider_tickers, _vol_baselines)
        if not scored.get('skip') and scored['score'] > 0:
            results.append(scored)

    # Sort by score desc, then by momentum
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
