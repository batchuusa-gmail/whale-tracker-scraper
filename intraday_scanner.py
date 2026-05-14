"""
Intraday Stock Scanner — Technical Pattern Edition
Replaces whale/insider signals with world-class technical trading patterns.

Strategy:
  - Dynamic universe built each morning from top 25 liquid, volatile stocks
  - Entry: EMA alignment + RSI trending zone + volume surge + pullback to EMA21
  - Stop: 1.5× ATR(14) from entry
  - Targets: entry ± 1× ATR (T1), entry ± 2× ATR (T2)
  - Active 7:00 AM – 3:45 PM ET (extended hours supported)
  - No forced EOD close, no circuit breaker

Kill switch: INTRADAY_SCANNER_ENABLED=true/false (Railway env var)
"""
from __future__ import annotations

import os
import logging
import threading
import time
from datetime import datetime, timedelta, timezone

import pytz
import requests

logger = logging.getLogger(__name__)

# ── Constants ────────────────────────────────────────────────────

SCAN_INTERVAL  = 60          # seconds between scans
TOP_N          = 20          # how many top picks to surface
ALPACA_KEY     = os.environ.get('ALPACA_KEY', '')
ALPACA_SECRET  = os.environ.get('ALPACA_SECRET', '')
ALPACA_DATA    = 'https://data.alpaca.markets'

_ET = pytz.timezone('America/New_York')

# ── In-memory state ──────────────────────────────────────────────

_state: dict = {
    'top_picks':  [],
    'all_scores': [],
    'last_scan':  None,
    'running':    False,
    'scan_count': 0,
    'error':      None,
    'universe':   [],
}
_state_lock = threading.Lock()

# ── Fallback universe (used when dynamic build fails) ────────────

FALLBACK_UNIVERSE = [
    'AAPL', 'MSFT', 'NVDA', 'TSLA', 'AMZN', 'META', 'GOOGL', 'AMD', 'NFLX',
    'SPY', 'QQQ', 'COIN', 'PLTR', 'UBER', 'CRM', 'MU', 'AVGO', 'ARM',
    'MSTR', 'NOW', 'PANW', 'SQ', 'SMCI', 'HOOD', 'SHOP',
]

# Broad candidate pool for dynamic universe selection
_CANDIDATE_POOL = [
    'AAPL', 'MSFT', 'NVDA', 'AMZN', 'META', 'GOOGL', 'TSLA', 'AVGO', 'JPM',
    'V', 'LLY', 'MA', 'COST', 'HD', 'JNJ', 'WMT', 'BAC', 'ABBV', 'MRK',
    'NFLX', 'CRM', 'AMD', 'ORCL', 'ADBE', 'IBM', 'QCOM', 'TXN', 'NOW',
    'INTU', 'CSCO', 'MCD', 'PEP', 'GS', 'MS', 'AMGN', 'ISRG', 'AMAT',
    'PANW', 'LRCX', 'CRWD', 'NET', 'PLTR', 'COIN', 'UBER', 'MU', 'ARM',
    'MSTR', 'SQ', 'SMCI', 'HOOD', 'SHOP', 'SPY', 'QQQ',
]

# Dynamic universe — refreshed each morning at 7 AM ET
_dynamic_universe: list[str] = []
_universe_date: str = ''
_universe_lock = threading.Lock()

# ── US market holidays ───────────────────────────────────────────

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
    return date_str not in _US_MARKET_HOLIDAYS


def is_market_open() -> bool:
    """True during regular market hours 9:30–16:00 ET on trading days."""
    now = datetime.now(_ET)
    if now.weekday() >= 5:
        return False
    if not _is_trading_day(now.strftime('%Y-%m-%d')):
        return False
    mo = now.replace(hour=9,  minute=30, second=0, microsecond=0)
    mc = now.replace(hour=16, minute=0,  second=0, microsecond=0)
    return mo <= now <= mc


def _is_active_window() -> bool:
    """True during our extended trading window 7:00 AM – 3:45 PM ET."""
    now = datetime.now(_ET)
    if now.weekday() >= 5:
        return False
    if not _is_trading_day(now.strftime('%Y-%m-%d')):
        return False
    start = now.replace(hour=7,  minute=0,  second=0, microsecond=0)
    end   = now.replace(hour=15, minute=45, second=0, microsecond=0)
    return start <= now <= end


# ── Alpaca bar fetching ──────────────────────────────────────────

def _alpaca_bars_batch(symbols: list[str], limit: int = 60,
                       timeframe: str = '1Min') -> dict[str, list]:
    """
    Fetch last `limit` bars for symbols via Alpaca IEX feed.
    Returns {symbol: [{'t','o','h','l','c','v'}, ...]}
    """
    results: dict[str, list] = {}
    if not ALPACA_KEY or not ALPACA_SECRET:
        return results

    hdrs = {
        'APCA-API-KEY-ID':     ALPACA_KEY,
        'APCA-API-SECRET-KEY': ALPACA_SECRET,
    }
    chunk_size = 50
    for i in range(0, len(symbols), chunk_size):
        chunk = symbols[i:i + chunk_size]
        try:
            r = requests.get(
                f'{ALPACA_DATA}/v2/stocks/bars',
                headers=hdrs,
                params={
                    'symbols':   ','.join(chunk),
                    'timeframe': timeframe,
                    'limit':     limit,
                    'feed':      'iex',
                    'sort':      'asc',
                },
                timeout=15,
            )
            if r.status_code == 200:
                for sym, bar_list in r.json().get('bars', {}).items():
                    bars = [
                        {'t': b['t'], 'o': b['o'], 'h': b['h'],
                         'l': b['l'], 'c': b['c'], 'v': b['v']}
                        for b in bar_list
                        if all(k in b for k in ('o', 'h', 'l', 'c', 'v'))
                    ]
                    if bars:
                        results[sym.upper()] = bars
            elif r.status_code == 403:
                logger.warning('Alpaca IEX 403 — feed blocked')
            else:
                logger.warning(f'Alpaca bars HTTP {r.status_code}')
        except Exception as e:
            logger.warning(f'Alpaca bars chunk error: {e}')

    return results


def _alpaca_daily_bars(symbols: list[str], limit: int = 5) -> dict[str, list]:
    """Fetch daily bars for universe construction (uses SIP first, falls back to IEX)."""
    results: dict[str, list] = {}
    if not ALPACA_KEY or not ALPACA_SECRET:
        return results

    hdrs = {
        'APCA-API-KEY-ID':     ALPACA_KEY,
        'APCA-API-SECRET-KEY': ALPACA_SECRET,
    }
    chunk_size = 100
    for feed in ('sip', 'iex'):
        missing = [s for s in symbols if s not in results]
        if not missing:
            break
        for i in range(0, len(missing), chunk_size):
            chunk = missing[i:i + chunk_size]
            try:
                r = requests.get(
                    f'{ALPACA_DATA}/v2/stocks/bars',
                    headers=hdrs,
                    params={
                        'symbols':   ','.join(chunk),
                        'timeframe': '1Day',
                        'limit':     limit,
                        'feed':      feed,
                        'sort':      'asc',
                    },
                    timeout=15,
                )
                if r.status_code == 200:
                    for sym, bar_list in r.json().get('bars', {}).items():
                        if sym.upper() not in results:
                            results[sym.upper()] = bar_list
            except Exception as e:
                logger.warning(f'Daily bars {feed} error: {e}')
    return results


# ── Dynamic universe builder ─────────────────────────────────────

def _build_dynamic_universe() -> list[str]:
    """
    Fetch previous-day daily bars for the candidate pool.
    Rank by dollar_volume (close × volume), filter ATR% 1.5–4%, pick top 25.
    Falls back to FALLBACK_UNIVERSE if data is unavailable.
    """
    try:
        daily = _alpaca_daily_bars(_CANDIDATE_POOL, limit=15)
        if not daily:
            logger.warning('Dynamic universe: no daily bars — using fallback')
            return list(FALLBACK_UNIVERSE)

        candidates = []
        for sym, bars in daily.items():
            if len(bars) < 2:
                continue
            closes = [b['c'] for b in bars]
            highs  = [b['h'] for b in bars]
            lows   = [b['l'] for b in bars]
            vols   = [b['v'] for b in bars]

            last_close = closes[-1]
            last_vol   = vols[-1]
            if last_close <= 0 or last_vol <= 0:
                continue

            dollar_vol = last_close * last_vol

            # ATR(14) on daily bars
            atr = _atr(highs, lows, closes, period=min(14, len(bars) - 1))
            if atr is None or last_close <= 0:
                continue
            atr_pct = (atr / last_close) * 100

            # Filter: ATR% must be between 1.5% and 4% (volatile enough, not crazy)
            if not (1.5 <= atr_pct <= 4.0):
                continue

            candidates.append({
                'sym':        sym,
                'dollar_vol': dollar_vol,
                'atr_pct':    atr_pct,
            })

        if not candidates:
            logger.warning('Dynamic universe: no candidates after ATR filter — using fallback')
            return list(FALLBACK_UNIVERSE)

        candidates.sort(key=lambda x: x['dollar_vol'], reverse=True)
        universe = [c['sym'] for c in candidates[:25]]
        logger.info(
            f'Dynamic universe built: {len(universe)} stocks '
            f'(top: {universe[:5]}) from {len(candidates)} candidates'
        )
        return universe

    except Exception as e:
        logger.error(f'Dynamic universe build failed: {e} — using fallback')
        return list(FALLBACK_UNIVERSE)


def _get_universe() -> list[str]:
    """Return today's dynamic universe, building it if needed."""
    global _dynamic_universe, _universe_date
    today = datetime.now(_ET).strftime('%Y-%m-%d')
    with _universe_lock:
        if _universe_date == today and _dynamic_universe:
            return list(_dynamic_universe)
    # Build outside the lock to avoid blocking scans
    new_universe = _build_dynamic_universe()
    with _universe_lock:
        _dynamic_universe = new_universe
        _universe_date    = today
        with _state_lock:
            _state['universe'] = new_universe
    return new_universe


# ── Technical indicators ─────────────────────────────────────────

def _ema(values: list[float], period: int) -> float | None:
    """Exponential moving average of the last `period` values."""
    if len(values) < period:
        return None
    k = 2.0 / (period + 1)
    result = sum(values[:period]) / period  # SMA seed
    for v in values[period:]:
        result = v * k + result * (1 - k)
    return result


def _rsi(closes: list[float], period: int = 14) -> float | None:
    """Wilder RSI using full close list. Needs at least period+1 bars."""
    if len(closes) < period + 1:
        return None
    deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    gains  = [max(d, 0.0) for d in deltas]
    losses = [max(-d, 0.0) for d in deltas]
    # Initial averages (simple)
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    # Wilder smoothing
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    if avg_loss == 0:
        return 100.0
    return round(100 - 100 / (1 + avg_gain / avg_loss), 2)


def _atr(highs: list[float], lows: list[float], closes: list[float],
         period: int = 14) -> float | None:
    """Average True Range (Wilder smoothing)."""
    if len(closes) < 2 or len(highs) < 1 or len(lows) < 1:
        return None
    n = min(len(highs), len(lows), len(closes))
    trs = []
    for i in range(1, n):
        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i]  - closes[i - 1]),
        )
        trs.append(tr)
    if not trs:
        return None
    p = min(period, len(trs))
    atr_val = sum(trs[:p]) / p
    for tr in trs[p:]:
        atr_val = (atr_val * (p - 1) + tr) / p
    return round(atr_val, 4)


def _vol_avg(volumes: list[float], period: int = 20) -> float:
    """Simple average volume over last `period` bars (excluding current bar)."""
    hist = volumes[:-1]  # exclude current bar
    if not hist:
        return 0.0
    window = hist[-period:]
    return sum(window) / len(window)


# ── Earnings proximity check ─────────────────────────────────────

_earnings_cache: dict[str, dict] = {}   # {sym: {'date': str, 'ts': float}}
_EARNINGS_CACHE_TTL = 3600 * 4           # refresh every 4 hours

_SUPA_URL = os.environ.get('SUPABASE_URL', 'https://bedurjtazsfbnkisoeee.supabase.co')
_SUPA_KEY = os.environ.get(
    'SUPABASE_KEY',
    'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJlZHVyanRhenNmYm5raXNvZWVlIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzM2NDcxOTUsImV4cCI6MjA4OTIyMzE5NX0.MK4N9dxAIHlXkGP4rLJPq5tHh9UU8L75EB1b8Q7CVmg'
)


def _has_earnings_soon(sym: str) -> bool:
    """
    Return True if the symbol has earnings within the next 3 days.
    Uses Alpaca's corporate actions endpoint; fails safe (returns False) on error.
    """
    cached = _earnings_cache.get(sym)
    if cached and (time.time() - cached['ts']) < _EARNINGS_CACHE_TTL:
        return cached['has']
    try:
        today  = datetime.now(_ET).date()
        window = today + timedelta(days=3)
        hdrs   = {
            'APCA-API-KEY-ID':     ALPACA_KEY,
            'APCA-API-SECRET-KEY': ALPACA_SECRET,
        }
        r = requests.get(
            f'{ALPACA_DATA}/v1beta1/corporate-actions/announcements',
            headers=hdrs,
            params={
                'ca_types':   'earnings',
                'symbol':     sym,
                'since':      today.isoformat(),
                'until':      window.isoformat(),
            },
            timeout=5,
        )
        has = False
        if r.status_code == 200:
            announcements = r.json().get('announcements', [])
            has = len(announcements) > 0
    except Exception:
        has = False
    _earnings_cache[sym] = {'has': has, 'ts': time.time()}
    return has


# ── Signal scoring ───────────────────────────────────────────────

def _score_symbol(
    symbol: str,
    bars: list[dict],
    _insider_tickers: set = None,   # kept for backward compat with diagnose endpoint
    _vol_baselines: dict = None,    # kept for backward compat with diagnose endpoint
) -> dict:
    """
    Score a symbol using technical pattern criteria.
    Returns a dict with ticker, score (0-100), signal (LONG/SHORT/NONE),
    confidence (0.6-1.0), entry, stop_loss, target_1, target_2, flags.
    """
    # Need at least 55 bars for EMA50 + ATR14 + RSI14
    if not bars or len(bars) < 55:
        return {'ticker': symbol, 'score': 0, 'signal': 'NONE', 'skip': True,
                'skip_reason': f'insufficient bars ({len(bars)})'}

    closes  = [b['c'] for b in bars]
    highs   = [b['h'] for b in bars]
    lows    = [b['l'] for b in bars]
    volumes = [b['v'] for b in bars]

    cur_price = closes[-1]
    if cur_price < 5.0:
        return {'ticker': symbol, 'score': 0, 'signal': 'NONE', 'skip': True,
                'skip_reason': f'price ${cur_price:.2f} too low'}

    # ── Indicators ──────────────────────────────────────────────
    ema8  = _ema(closes, 8)
    ema21 = _ema(closes, 21)
    ema50 = _ema(closes, 50)
    rsi   = _rsi(closes, 14)
    atr14 = _atr(highs, lows, closes, 14)
    cur_vol  = volumes[-1]
    avg_vol  = _vol_avg(volumes, 20)

    if None in (ema8, ema21, ema50, rsi, atr14) or avg_vol == 0:
        return {'ticker': symbol, 'score': 0, 'signal': 'NONE', 'skip': True,
                'skip_reason': 'indicator calc failed'}

    atr_pct = (atr14 / cur_price) * 100

    # ── Determine direction ──────────────────────────────────────
    is_long  = ema8 > ema21 > ema50
    is_short = ema8 < ema21 < ema50

    if not is_long and not is_short:
        return {
            'ticker': symbol, 'score': 0, 'signal': 'NONE', 'skip': False,
            'price': round(cur_price, 2),
            'ema8': round(ema8, 2), 'ema21': round(ema21, 2), 'ema50': round(ema50, 2),
            'rsi': rsi, 'atr_pct': round(atr_pct, 2),
            'flags': ['NO_EMA_ALIGN'],
            'skip_reason': 'EMA not aligned',
        }

    direction = 'LONG' if is_long else 'SHORT'

    # ── Check all entry conditions (each is worth points) ────────
    score  = 0
    flags  = []

    # Condition 1: EMA alignment (40 pts — foundational)
    score += 40
    flags.append(f'EMA_ALIGN_{direction}')

    # Condition 2: RSI in trending zone 45–65 (20 pts)
    rsi_ok = 45 <= rsi <= 65
    if rsi_ok:
        score += 20
        flags.append(f'RSI_{rsi:.0f}')
    else:
        flags.append(f'RSI_{rsi:.0f}_OUT')

    # Condition 3: Volume surge ≥ 1.5× avg (20 pts)
    vol_ratio = cur_vol / avg_vol if avg_vol > 0 else 0
    vol_ok    = vol_ratio >= 1.5
    if vol_ok:
        score += 20
        flags.append(f'VOL_{vol_ratio:.1f}x')
    else:
        flags.append(f'VOL_{vol_ratio:.1f}x_LOW')

    # Condition 4: Pullback — price within 1.5% of EMA21 (20 pts)
    pullback_pct = abs(cur_price - ema21) / ema21 * 100
    pullback_ok  = pullback_pct <= 1.5
    if pullback_ok:
        score += 20
        flags.append(f'PULLBACK_{pullback_pct:.1f}%')
    else:
        flags.append(f'PULLBACK_{pullback_pct:.1f}%_FAR')

    # All 4 conditions met → strong signal
    all_conditions = rsi_ok and vol_ok and pullback_ok

    # ── Earnings filter — kills the signal entirely ──────────────
    if _has_earnings_soon(symbol):
        return {
            'ticker':  symbol, 'score': 0, 'signal': 'NONE', 'skip': False,
            'price':   round(cur_price, 2),
            'flags':   ['EARNINGS_SOON'],
            'skip_reason': 'earnings within 3 days',
        }

    # ── Compute signal and confidence ────────────────────────────
    if all_conditions:
        signal = direction
        # Confidence scales with score: all 4 conditions = 1.0, fewer = lower
        confidence = round(0.6 + (score - 60) / 100 * 0.4, 3)
        confidence = max(0.6, min(1.0, confidence))
    elif score >= 60:
        signal     = direction
        confidence = 0.60
    else:
        signal     = 'NONE'
        confidence = 0.0

    # ── Compute levels ───────────────────────────────────────────
    if direction == 'LONG':
        stop_loss = round(cur_price - 1.5 * atr14, 2)
        target_1  = round(cur_price + 1.0 * atr14, 2)
        target_2  = round(cur_price + 2.0 * atr14, 2)
    else:
        stop_loss = round(cur_price + 1.5 * atr14, 2)
        target_1  = round(cur_price - 1.0 * atr14, 2)
        target_2  = round(cur_price - 2.0 * atr14, 2)

    return {
        'ticker':     symbol,
        'score':      score,
        'signal':     signal,
        'confidence': confidence,
        'price':      round(cur_price, 2),
        'entry':      round(cur_price, 2),
        'stop_loss':  stop_loss,
        'target_1':   target_1,
        'target_2':   target_2,
        'ema8':       round(ema8, 2),
        'ema21':      round(ema21, 2),
        'ema50':      round(ema50, 2),
        'rsi':        rsi,
        'atr14':      round(atr14, 4),
        'atr_pct':    round(atr_pct, 2),
        'vol_ratio':  round(vol_ratio, 2),
        'pullback_pct': round(pullback_pct, 2),
        'flags':      flags,
        'skip':       False,
    }


# ── Insider tickers stub (kept for diagnose endpoint compat) ─────

def _fetch_insider_tickers() -> set[str]:
    """Stub — insider signals replaced by technical patterns. Returns empty set."""
    return set()


# Vol baselines dict — kept for diagnose endpoint compat
_vol_baselines: dict[str, float] = {}


# ── Main scan pass ───────────────────────────────────────────────

def _run_scan(redis_set_fn, redis_get_fn):
    """Execute one full scan pass. Called every SCAN_INTERVAL seconds."""
    scan_start = datetime.now(_ET)
    logger.info(f'Intraday scanner: scan starting at {scan_start.strftime("%H:%M:%S ET")}')

    universe = _get_universe()
    if not universe:
        logger.warning('Scan aborted: empty universe')
        return

    # Fetch 1-min bars — need 60+ bars for EMA50+ATR14+RSI14
    bars_data = _alpaca_bars_batch(universe, limit=70, timeframe='1Min')

    results   = []
    skipped   = 0
    no_bars   = 0

    for sym in universe:
        bars = bars_data.get(sym, [])
        if not bars:
            no_bars += 1
            continue
        scored = _score_symbol(sym, bars)
        if scored.get('skip') or scored.get('score', 0) == 0:
            skipped += 1
        results.append(scored)

    # Sort: actionable signals first, then by score desc
    results.sort(key=lambda x: (
        0 if x.get('signal') not in ('LONG', 'SHORT') else 1,
        x.get('score', 0),
        x.get('confidence', 0),
    ), reverse=True)

    top_picks = results[:TOP_N]

    scan_end  = datetime.now(_ET)
    elapsed   = (scan_end - scan_start).total_seconds()

    actionable = [p for p in top_picks if p.get('signal') in ('LONG', 'SHORT')]
    logger.info(
        f'Scan done in {elapsed:.1f}s | universe={len(universe)} '
        f'bars_received={len(bars_data)} no_bars={no_bars} '
        f'scored={len(results)} actionable={len(actionable)}'
    )

    scan_meta = {
        'last_scan':       scan_end.isoformat(),
        'elapsed_seconds': round(elapsed, 1),
        'stocks_scanned':  len(universe),
        'stocks_scored':   len(results),
        'actionable':      len(actionable),
        'running':         True,
        'scan_count':      _state['scan_count'] + 1,
    }

    redis_set_fn('scanner:top',    top_picks,      ttl_seconds=90)
    redis_set_fn('scanner:all',    results[:200],  ttl_seconds=90)
    redis_set_fn('scanner:status', scan_meta,      ttl_seconds=90)

    with _state_lock:
        _state['top_picks']  = top_picks
        _state['all_scores'] = results[:200]
        _state['last_scan']  = scan_end.isoformat()
        _state['running']    = True
        _state['scan_count'] += 1
        _state['error']      = None

    # Auto-trade bridge — fires orders for high-conviction signals
    _auto_trade_top_picks(top_picks, bars_data, redis_set_fn, redis_get_fn)


# ── Auto-trade bridge ────────────────────────────────────────────

AUTO_TRADE_MIN_SCORE    = 60
AUTO_TRADE_MAX_PER_SCAN = 3


def _auto_trade_top_picks(
    top_picks: list[dict],
    bars_data: dict,
    redis_set_fn,
    redis_get_fn,
):
    """
    For each top pick with signal LONG/SHORT and score >= threshold,
    call intraday_executor.place_intraday_order() directly.
    Gated by INTRADAY_TRADING_ENABLED env var.
    """
    trading_enabled = os.environ.get('INTRADAY_TRADING_ENABLED', 'false').lower() == 'true'
    if not trading_enabled:
        return

    try:
        from intraday_executor import place_intraday_order
    except Exception as e:
        logger.error(f'Auto-trade import error: {e}')
        return

    # Load config from Supabase algo_config
    cfg = {
        'min_score':          AUTO_TRADE_MIN_SCORE,
        'min_confidence':     0.65,
        'max_trades_per_day': AUTO_TRADE_MAX_PER_SCAN,
        'max_positions':      3,
        'position_size_pct':  0.15,
    }
    try:
        r = requests.get(
            f'{_SUPA_URL}/rest/v1/algo_settings?limit=1',
            headers={'apikey': _SUPA_KEY, 'Authorization': f'Bearer {_SUPA_KEY}'},
            timeout=3,
        )
        if r.status_code == 200 and r.json():
            row = r.json()[0]
            for k in ('min_score', 'max_trades_per_day', 'max_positions', 'position_size_pct'):
                if row.get(k) is not None:
                    cfg[k] = row[k]
            if row.get('min_confidence') is not None:
                cfg['min_confidence'] = float(row['min_confidence']) / 100
    except Exception as e:
        logger.warning(f'algo_settings fetch failed: {e}')

    min_score   = int(cfg['min_score'])
    min_conf    = float(cfg['min_confidence'])
    max_per_scan = int(cfg['max_trades_per_day'])

    candidates = [
        p for p in top_picks
        if p.get('signal') in ('LONG', 'SHORT')
        and p.get('score', 0) >= min_score
        and p.get('confidence', 0) >= min_conf
    ]

    logger.info(
        f'Auto-trade: {len(candidates)} candidates '
        f'(min_score={min_score} min_conf={min_conf:.0%})'
    )

    attempted = 0
    for pick in candidates:
        if attempted >= max_per_scan:
            break
        sig_str = 'BUY' if pick['signal'] == 'LONG' else 'SELL'
        order_payload = {
            'ticker':     pick['ticker'],
            'signal':     sig_str,
            'entry':      pick.get('entry', pick.get('price', 0)),
            'stop_loss':  pick.get('stop_loss', 0),
            'target_1':   pick.get('target_1', 0),
            'target_2':   pick.get('target_2', 0),
            'hold_minutes': 0,       # no time stop — hold until stop/target
            'confidence': pick.get('confidence', 0),
            'reasoning':  ' | '.join(pick.get('flags', [])),
            'position_size_pct': float(cfg.get('position_size_pct', 0.10)),
        }
        try:
            result = place_intraday_order(order_payload)
            logger.info(f'Auto-trade {pick["ticker"]}: {result.get("status")}')
        except Exception as e:
            logger.error(f'Auto-trade order error {pick["ticker"]}: {e}')
        attempted += 1


# ── Background thread ────────────────────────────────────────────

def _scanner_loop(redis_set_fn, redis_get_fn):
    logger.info('Intraday scanner thread started')
    _last_status_log = 0
    while True:
        try:
            enabled = os.environ.get('INTRADAY_SCANNER_ENABLED', 'false').lower() == 'true'
            active  = _is_active_window()
            if enabled and active:
                _run_scan(redis_set_fn, redis_get_fn)
            else:
                with _state_lock:
                    _state['running'] = False
                reason = 'outside_window' if not active else 'disabled'
                now_ts = time.time()
                if now_ts - _last_status_log > 1800:
                    now_et = datetime.now(_ET)
                    logger.info(
                        f'Scanner idle — reason={reason} '
                        f'enabled={enabled} active_window={active} '
                        f'ET={now_et.strftime("%H:%M")} '
                        f'scan_count={_state.get("scan_count", 0)}'
                    )
                    _last_status_log = now_ts
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
    """Restart the scanner thread if it dies unexpectedly."""
    while True:
        time.sleep(90)
        alive = any(
            t.name == 'intraday-scanner' and t.is_alive()
            for t in threading.enumerate()
        )
        if not alive:
            logger.error('Intraday scanner thread died — restarting')
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
    threading.Thread(
        target=_scanner_watchdog,
        args=(redis_set_fn, redis_get_fn),
        daemon=True,
        name='scanner-watchdog',
    ).start()
    logger.info('Intraday scanner: thread + watchdog launched')


# ── Public accessors (called by Flask routes in main.py) ─────────

def get_top(redis_get_fn) -> list:
    cached = redis_get_fn('scanner:top')
    if cached is not None:
        return cached
    with _state_lock:
        return list(_state['top_picks'])


def get_top_picks(limit: int = 20) -> list:
    """Alias used by confluence scoring in main.py."""
    with _state_lock:
        return list(_state['top_picks'])[:limit]


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
            'running':     _state['running'],
            'last_scan':   _state['last_scan'],
            'scan_count':  _state['scan_count'],
            'error':       _state['error'],
            'market_open': is_market_open(),
            'active_window': _is_active_window(),
            'enabled':     os.environ.get('INTRADAY_SCANNER_ENABLED', 'false').lower() == 'true',
            'universe':    _state.get('universe', []),
        }


def get_state() -> dict:
    """Full in-memory state snapshot. Used by signal_history route in main.py."""
    with _state_lock:
        return {
            'top_picks':  list(_state['top_picks']),
            'all_scores': list(_state['all_scores']),
            'last_scan':  _state['last_scan'],
            'running':    _state['running'],
            'scan_count': _state['scan_count'],
            'error':      _state['error'],
        }
