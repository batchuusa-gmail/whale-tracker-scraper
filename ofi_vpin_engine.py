"""
WHAL-104 — Order Flow Imbalance (OFI) & VPIN Engine

Uses Alpaca 1-minute OHLCV bars with Lee-Ready tick classification:
  If Close > Open  → classify bar volume as BUY
  If Close < Open  → classify bar volume as SELL
  If Close == Open → split 50/50

OFI  = (Buy_Vol - Sell_Vol) / Total_Vol  per 5-min rolling window
       > +0.65 sustained 3+ buckets = strong institutional buy pressure
       < -0.65 sustained 3+ buckets = institutional selling / exit signal

VPIN = abs(Buy_Vol - Sell_Vol) / Total_Vol  per volume bucket (50-bar window)
       > 0.70 = adverse selection spike → smart money exiting, reduce exposure

No Polygon.io required — uses existing Alpaca Data API (already configured).
"""

import os
import logging
import threading
from datetime import datetime, timedelta, timezone

import requests

logger = logging.getLogger(__name__)

ALPACA_DATA   = 'https://data.alpaca.markets'
ALPACA_KEY    = os.environ.get('ALPACA_KEY', '')
ALPACA_SECRET = os.environ.get('ALPACA_SECRET', '')

# Scan universe — liquid names with good intraday volume
OFI_UNIVERSE = [
    'SPY', 'QQQ', 'IWM', 'AAPL', 'MSFT', 'NVDA', 'AMZN', 'GOOGL', 'META', 'TSLA',
    'AMD', 'PLTR', 'COIN', 'CRWD', 'PANW', 'NET', 'UBER', 'SNAP', 'SOFI', 'RIVN',
    'JPM', 'GS', 'BAC', 'V', 'MA', 'XLF', 'XLK', 'SOXL', 'TQQQ', 'SQQQ',
    'LLY', 'MRNA', 'NVAX', 'PFE', 'XOM', 'CVX', 'MU', 'INTC', 'QCOM', 'AVGO',
]

OFI_BUY_THRESHOLD  =  0.60   # OFI above this = institutional buy pressure
OFI_SELL_THRESHOLD = -0.60   # OFI below this = institutional sell pressure
VPIN_EXIT_THRESHOLD = 0.70   # VPIN above this = adverse selection / exit signal
MIN_BARS_REQUIRED   = 10     # need at least 10 bars to compute reliable OFI/VPIN

_state_lock = threading.Lock()
_state = {
    'scores':       [],
    'last_updated': None,
    'error':        None,
}
CACHE_TTL = 300   # 5 min — refresh often during market hours


# ── Alpaca bars fetch ─────────────────────────────────────────────────

def _alpaca_headers() -> dict:
    return {
        'APCA-API-KEY-ID':     ALPACA_KEY,
        'APCA-API-SECRET-KEY': ALPACA_SECRET,
    }


_YF_HDR = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'}


def _fetch_bars(ticker: str, limit: int = 60) -> list[dict]:
    """Fetch daily bars via Yahoo Finance chart API (free, works 24/7)."""
    try:
        r = requests.get(
            f'https://query1.finance.yahoo.com/v8/finance/chart/{ticker}',
            params={'interval': '1d', 'range': '30d'},
            headers=_YF_HDR, timeout=10,
        )
        if r.status_code == 200:
            result = r.json().get('chart', {}).get('result', [{}])[0]
            quotes = result.get('indicators', {}).get('quote', [{}])[0]
            opens  = quotes.get('open', [])
            closes = quotes.get('close', [])
            volumes= quotes.get('volume', [])
            bars = []
            for i in range(len(closes)):
                if closes[i] is None: continue
                bars.append({
                    'o': opens[i] or closes[i],
                    'c': closes[i],
                    'v': volumes[i] or 0,
                })
            return bars[-limit:]
    except Exception as e:
        logger.warning(f'_fetch_bars {ticker}: {e}')
    return []


def _fetch_bars_batch(tickers: list[str], limit: int = 60) -> dict[str, list]:
    """Fetch bars for multiple tickers via Yahoo Finance."""
    results = {}
    for ticker in tickers:
        bars = _fetch_bars(ticker, limit)
        if bars:
            results[ticker] = bars
    return results


# ── OFI computation ───────────────────────────────────────────────────

def _classify_bar(bar: dict) -> tuple[float, float]:
    """
    Lee-Ready tick classification for a single bar.
    Returns (buy_volume, sell_volume).
    """
    o = bar.get('o', 0)
    c = bar.get('c', 0)
    v = bar.get('v', 0)
    if c > o:
        return float(v), 0.0
    elif c < o:
        return 0.0, float(v)
    else:
        return float(v) / 2, float(v) / 2


def compute_ofi(bars: list[dict], window: int = 5) -> list[float]:
    """
    Compute rolling OFI over `window`-bar buckets.
    Returns list of OFI values [-1, +1] for each bucket.
    """
    ofi_series = []
    for i in range(0, len(bars) - window + 1, window):
        bucket  = bars[i:i + window]
        buy_vol  = sum(_classify_bar(b)[0] for b in bucket)
        sell_vol = sum(_classify_bar(b)[1] for b in bucket)
        total    = buy_vol + sell_vol
        ofi      = (buy_vol - sell_vol) / total if total > 0 else 0.0
        ofi_series.append(round(ofi, 4))
    return ofi_series


def compute_vpin(bars: list[dict], bucket_size: int = 10) -> float:
    """
    VPIN: Volume-Synchronized Probability of Informed Trading.
    Splits bars into volume buckets, computes |buy_vol - sell_vol| / total per bucket.
    Returns average VPIN across all buckets.
    """
    if len(bars) < bucket_size:
        return 0.0

    bucket_vpins = []
    for i in range(0, len(bars) - bucket_size + 1, bucket_size):
        bucket   = bars[i:i + bucket_size]
        buy_vol  = sum(_classify_bar(b)[0] for b in bucket)
        sell_vol = sum(_classify_bar(b)[1] for b in bucket)
        total    = buy_vol + sell_vol
        vpin     = abs(buy_vol - sell_vol) / total if total > 0 else 0.0
        bucket_vpins.append(vpin)

    return round(sum(bucket_vpins) / len(bucket_vpins), 4) if bucket_vpins else 0.0


# ── Signal classifier ─────────────────────────────────────────────────

def _ofi_signal(ofi_series: list[float]) -> str:
    """
    Classify sustained OFI trend.
    Looking at last 3 buckets for consistency.
    """
    if len(ofi_series) < 1:
        return 'NEUTRAL'
    recent  = ofi_series[-3:]
    avg_ofi = sum(recent) / len(recent)
    sustained_buy  = all(o >= OFI_BUY_THRESHOLD for o in recent)
    sustained_sell = all(o <= OFI_SELL_THRESHOLD for o in recent)

    if sustained_buy:  return 'STRONG BUY PRESSURE'
    if sustained_sell: return 'STRONG SELL PRESSURE'
    if avg_ofi >= 0.4: return 'BUY PRESSURE'
    if avg_ofi <= -0.4: return 'SELL PRESSURE'
    return 'NEUTRAL'


def _vpin_label(vpin: float) -> str:
    if vpin >= VPIN_EXIT_THRESHOLD: return 'EXIT SIGNAL'
    if vpin >= 0.55:                return 'ELEVATED'
    if vpin >= 0.40:                return 'MODERATE'
    return 'NORMAL'


def _conviction(ofi_series: list[float], vpin: float) -> int:
    """0-100 conviction score for the OFI signal."""
    if not ofi_series:
        return 0
    recent  = ofi_series[-3:]
    avg_ofi = abs(sum(recent) / len(recent))
    consistency = sum(1 for o in recent if (o > 0) == (recent[0] > 0)) / len(recent)

    score = int(avg_ofi * 60 + consistency * 30)
    # Penalise if VPIN is elevated (adverse selection)
    if vpin >= VPIN_EXIT_THRESHOLD:
        score = int(score * 0.5)
    return min(score, 100)


# ── Per-ticker scorer ─────────────────────────────────────────────────

def score_ticker(ticker: str, bars: list[dict] = None) -> dict | None:
    """Compute OFI + VPIN for one ticker. Returns None if insufficient data."""
    ticker = ticker.upper()
    if bars is None:
        bars = _fetch_bars(ticker, limit=60)

    if len(bars) < MIN_BARS_REQUIRED:
        return None

    ofi_series = compute_ofi(bars, window=5)
    vpin       = compute_vpin(bars, bucket_size=10)
    signal     = _ofi_signal(ofi_series)
    vpin_lbl   = _vpin_label(vpin)
    conviction = _conviction(ofi_series, vpin)

    # Latest price
    last_bar   = bars[-1]
    price      = round(float(last_bar.get('c', 0)), 2)
    prev_bar   = bars[0]
    prev_price = float(prev_bar.get('c', 0))
    change_pct = round((price - prev_price) / prev_price * 100, 2) if prev_price > 0 else 0

    # Total volume
    total_vol = sum(int(b.get('v', 0)) for b in bars)

    # Buy/sell volume totals (all bars)
    total_buy  = sum(_classify_bar(b)[0] for b in bars)
    total_sell = sum(_classify_bar(b)[1] for b in bars)
    overall_ofi = round((total_buy - total_sell) / (total_buy + total_sell), 4) if (total_buy + total_sell) > 0 else 0

    return {
        'ticker':       ticker,
        'signal':       signal,
        'vpin':         vpin,
        'vpin_label':   vpin_lbl,
        'conviction':   conviction,
        'ofi_latest':   round(ofi_series[-1], 4) if ofi_series else 0,
        'ofi_avg':      round(sum(ofi_series) / len(ofi_series), 4) if ofi_series else 0,
        'ofi_series':   ofi_series[-12:],    # last 12 buckets for chart
        'overall_ofi':  overall_ofi,
        'total_buy_vol':  int(total_buy),
        'total_sell_vol': int(total_sell),
        'total_vol':    total_vol,
        'bars_used':    len(bars),
        'price':        price,
        'change_pct':   change_pct,
        'exit_signal':  vpin >= VPIN_EXIT_THRESHOLD,
        'scored_at':    datetime.now(timezone.utc).isoformat(),
    }


# ── Batch scanner ─────────────────────────────────────────────────────

def scan_ofi_top(universe: list[str] = None) -> list[dict]:
    """
    Scan universe, compute OFI/VPIN for each, return sorted by conviction.
    """
    tickers  = universe or OFI_UNIVERSE
    bars_map = _fetch_bars_batch(tickers, limit=60)

    results = []
    for ticker in tickers:
        bars = bars_map.get(ticker, [])
        try:
            score = score_ticker(ticker, bars)
            if score and score['signal'] != 'NEUTRAL':
                results.append(score)
        except Exception as e:
            logger.warning(f'OFI score error {ticker}: {e}')

    # Sort: exit signals last (dangerous), then by conviction desc
    results.sort(key=lambda x: (x['exit_signal'], -x['conviction']))
    return results


# ── Cache ────────────────────────────────────────────────────────────

def get_ofi_scores(redis_get=None, redis_set=None) -> list[dict]:
    cache_key = 'ofi:scores'
    if redis_get:
        cached = redis_get(cache_key)
        if cached:
            return cached

    with _state_lock:
        scores       = _state['scores']
        last_updated = _state['last_updated']

    stale = True
    if last_updated:
        try:
            age   = (datetime.now(timezone.utc) - datetime.fromisoformat(last_updated)).total_seconds()
            stale = age > CACHE_TTL
        except Exception:
            pass

    # Never block — kick off background refresh if stale
    if stale or not scores:
        import threading as _t
        _t.Thread(target=refresh_ofi, daemon=True, name='ofi-refresh').start()

    if redis_set and scores:
        redis_set(cache_key, scores, ttl_seconds=CACHE_TTL)

    return scores


def refresh_ofi():
    logger.info('OFI/VPIN engine: scanning…')
    try:
        scores = scan_ofi_top()
        with _state_lock:
            _state['scores']       = scores
            _state['last_updated'] = datetime.now(timezone.utc).isoformat()
            _state['error']        = None
        logger.info(f'OFI/VPIN engine: {len(scores)} signals computed')
    except Exception as e:
        logger.error(f'OFI/VPIN refresh error: {e}')
        with _state_lock:
            _state['error'] = str(e)
