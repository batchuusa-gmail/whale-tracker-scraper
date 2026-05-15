"""
FAANG Swing Scanner — 4-Layer Signal Engine
Runs once daily at 9:35 AM ET.

Scoring (100 pts, threshold 60):
  1. EMA Momentum   (35 pts) — EMA21 > EMA50 > EMA200 on daily chart
  2. RSI Zone       (20 pts) — Daily RSI(14) in 40-65 trending zone
  3. Near Support   (25 pts) — Price within 3% of key swing-low support
  4. Options Flow   (20 pts) — Put/call ratio + IV sentiment via yfinance

Additional context (displayed, not scored):
  - Probability range: 1-sigma expected move (5-day and 10-day)
  - Key resistance: nearest swing high above price

Kill switch: SWING_SCANNER_ENABLED=true/false env var
"""
from __future__ import annotations

import os
import logging
import math
import threading
import time
from datetime import datetime, timezone

import pytz
import requests

logger = logging.getLogger(__name__)

FAANG        = ['META', 'AAPL', 'AMZN', 'NFLX', 'GOOGL']
SIGNAL_THRESHOLD = 60   # minimum score to surface a signal
MONITOR_SECS = 300      # check time every 5 min
# Two daily scans: pre-market (7:00 AM ET) and after-hours (7:30 PM ET)
SCAN_WINDOWS_ET = [
    (7,  0),   # pre-market open — uses prior day's completed daily bars
    (19, 30),  # after-hours — uses today's completed daily bars
]

ALPACA_KEY    = os.environ.get('ALPACA_KEY', '')
ALPACA_SECRET = os.environ.get('ALPACA_SECRET', '')
ALPACA_DATA   = 'https://data.alpaca.markets'

_ET = pytz.timezone('America/New_York')

_US_HOLIDAYS = {
    '2025-01-01', '2025-01-20', '2025-02-17', '2025-04-18',
    '2025-05-26', '2025-06-19', '2025-07-04', '2025-09-01',
    '2025-11-27', '2025-12-25',
    '2026-01-01', '2026-01-19', '2026-02-16', '2026-04-03',
    '2026-05-25', '2026-06-19', '2026-07-03', '2026-09-07',
    '2026-11-26', '2026-12-25',
}

_state: dict = {
    'signals':    [],
    'all_scores': [],
    'last_scan':  None,
    'scan_date':  None,
    'running':    False,
    'scan_count': 0,
    'error':      None,
}
_state_lock = threading.Lock()


# ── Alpaca daily bars ────────────────────────────────────────────────────────

def _fetch_daily_bars(symbol: str, limit: int = 210) -> list:
    """Fetch daily OHLCV bars from Alpaca (split-adjusted)."""
    key    = os.environ.get('ALPACA_KEY', '')
    secret = os.environ.get('ALPACA_SECRET', '')
    if not key or not secret:
        return []
    try:
        r = requests.get(
            f'{ALPACA_DATA}/v2/stocks/{symbol}/bars',
            headers={'APCA-API-KEY-ID': key, 'APCA-API-SECRET-KEY': secret},
            params={'timeframe': '1Day', 'limit': limit, 'adjustment': 'split', 'sort': 'asc'},
            timeout=15,
        )
        if r.status_code == 200:
            return r.json().get('bars', [])
    except Exception as e:
        logger.warning(f'daily bars {symbol}: {e}')
    return []


# ── Indicators ───────────────────────────────────────────────────────────────

def _ema(prices: list[float], period: int) -> list[float]:
    if len(prices) < period:
        return []
    k = 2 / (period + 1)
    result = [sum(prices[:period]) / period]
    for p in prices[period:]:
        result.append(p * k + result[-1] * (1 - k))
    return result


def _rsi(closes: list[float], period: int = 14) -> float:
    if len(closes) < period + 2:
        return 50.0
    deltas = [closes[i + 1] - closes[i] for i in range(len(closes) - 1)]
    gains  = [max(d, 0) for d in deltas[-period:]]
    losses = [abs(min(d, 0)) for d in deltas[-period:]]
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return round(100 - (100 / (1 + rs)), 2)


def _atr(bars: list[dict], period: int = 14) -> float:
    if len(bars) < period + 1:
        return 0.0
    trs = []
    for i in range(1, len(bars)):
        h, l, pc = bars[i]['h'], bars[i]['l'], bars[i - 1]['c']
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    return round(sum(trs[-period:]) / period, 4)


# ── Support / Resistance ─────────────────────────────────────────────────────

def _find_sr_levels(bars: list[dict], lookback: int = 60, window: int = 4) -> tuple[list, list]:
    """Return (support_levels, resistance_levels) sorted nearest-first."""
    recent = bars[-lookback:]
    lows   = [b['l'] for b in recent]
    highs  = [b['h'] for b in recent]

    supports    = []
    resistances = []

    for i in range(window, len(recent) - window):
        # Swing low
        if lows[i] == min(lows[i - window:i + window + 1]):
            supports.append(round(lows[i], 2))
        # Swing high
        if highs[i] == max(highs[i - window:i + window + 1]):
            resistances.append(round(highs[i], 2))

    # Deduplicate levels within 0.5% of each other
    def _dedupe(levels):
        levels = sorted(set(levels))
        deduped = []
        for lvl in levels:
            if not deduped or abs(lvl - deduped[-1]) / deduped[-1] > 0.005:
                deduped.append(lvl)
        return deduped

    return _dedupe(supports), _dedupe(resistances)


# ── Options sentiment via yfinance ───────────────────────────────────────────

def _options_sentiment(ticker: str, current_price: float) -> dict:
    """Put/call ratio, ATM IV, and probability range via yfinance."""
    try:
        import yfinance as yf
        stock   = yf.Ticker(ticker)
        expiries = stock.options
        if not expiries:
            return {}

        today = datetime.now(_ET).date()
        best_expiry, best_diff, actual_dte = None, 999, 0
        for exp in expiries:
            try:
                exp_date = datetime.strptime(exp, '%Y-%m-%d').date()
                dte      = (exp_date - today).days
                diff     = abs(dte - 35)
                if 21 <= dte <= 56 and diff < best_diff:
                    best_expiry, best_diff, actual_dte = exp, diff, dte
            except Exception:
                continue

        if not best_expiry:
            return {}

        chain = stock.option_chain(best_expiry)
        calls, puts = chain.calls, chain.puts

        total_call_oi = int(calls['openInterest'].fillna(0).sum())
        total_put_oi  = int(puts['openInterest'].fillna(0).sum())
        pc_ratio      = round(total_put_oi / total_call_oi, 2) if total_call_oi > 0 else 1.0

        # ATM implied volatility
        atm_call = calls.iloc[(calls['strike'] - current_price).abs().argsort()[:1]]
        atm_iv   = float(atm_call['impliedVolatility'].values[0]) if len(atm_call) else 0.25
        atm_iv   = min(max(atm_iv, 0.05), 2.0)

        exp_5d  = round(atm_iv * current_price * math.sqrt(5 / 252), 2)
        exp_10d = round(atm_iv * current_price * math.sqrt(10 / 252), 2)

        if pc_ratio < 0.70:
            sentiment, sentiment_score = 'bullish', 20
        elif pc_ratio < 0.85:
            sentiment, sentiment_score = 'neutral_bullish', 12
        elif pc_ratio < 1.00:
            sentiment, sentiment_score = 'neutral', 5
        else:
            sentiment, sentiment_score = 'bearish', 0

        return {
            'pc_ratio':           pc_ratio,
            'atm_iv_pct':         round(atm_iv * 100, 1),
            'expiry':             best_expiry,
            'dte':                actual_dte,
            'exp_move_5d':        exp_5d,
            'exp_move_10d':       exp_10d,
            'upper_1sigma_5d':    round(current_price + exp_5d, 2),
            'lower_1sigma_5d':    round(current_price - exp_5d, 2),
            'upper_1sigma_10d':   round(current_price + exp_10d, 2),
            'lower_1sigma_10d':   round(current_price - exp_10d, 2),
            'sentiment':          sentiment,
            'sentiment_score':    sentiment_score,
            'call_oi':            total_call_oi,
            'put_oi':             total_put_oi,
        }
    except Exception as e:
        logger.warning(f'options_sentiment {ticker}: {e}')
        return {}


# ── Score a single FAANG stock ───────────────────────────────────────────────

def _score_symbol(symbol: str) -> dict | None:
    bars = _fetch_daily_bars(symbol, 220)
    if len(bars) < 60:
        logger.warning(f'{symbol}: only {len(bars)} bars, skipping')
        return None

    # Exclude today's partial bar for indicator calculation
    hist   = bars[:-1]
    closes = [b['c'] for b in hist]
    cur_price = bars[-1].get('o') or closes[-1]  # use today's open as reference

    # ── EMAs ────────────────────────────────────────────────────
    ema21_series  = _ema(closes, 21)
    ema50_series  = _ema(closes, 50)
    ema200_series = _ema(closes, 200) if len(closes) >= 200 else []

    if not ema21_series or not ema50_series:
        return None

    ema21  = ema21_series[-1]
    ema50  = ema50_series[-1]
    ema200 = ema200_series[-1] if ema200_series else None

    # ── RSI ─────────────────────────────────────────────────────
    rsi_val = _rsi(closes)

    # ── ATR ─────────────────────────────────────────────────────
    atr_val = _atr(hist)

    # ── Support / Resistance ─────────────────────────────────────
    supports, resistances = _find_sr_levels(hist)
    # Nearest support below current price
    supports_below = [s for s in supports if s < cur_price]
    nearest_support = supports_below[-1] if supports_below else None
    # Nearest resistance above current price
    res_above = [r for r in resistances if r > cur_price]
    nearest_resistance = res_above[0] if res_above else None

    # ── Scoring ──────────────────────────────────────────────────
    score    = 0
    notes    = []

    # Layer 1 — EMA Momentum (35 pts)
    ema_score = 0
    if ema21 > ema50:
        ema_score += 20
        notes.append('EMA21>EMA50')
    if ema200 and ema50 > ema200:
        ema_score += 15
        notes.append('EMA50>EMA200')
    elif ema200 is None:
        ema_score += 8  # partial credit — not enough history
    score += ema_score

    # Layer 2 — RSI Zone (20 pts)
    rsi_score = 0
    if 40 <= rsi_val <= 65:
        rsi_score = 20
        notes.append(f'RSI={rsi_val:.0f}✓')
    elif 35 <= rsi_val < 40 or 65 < rsi_val <= 70:
        rsi_score = 10
        notes.append(f'RSI={rsi_val:.0f}~')
    score += rsi_score

    # Layer 3 — Near Key Support (25 pts)
    support_score = 0
    support_pct   = None
    if nearest_support:
        pct_from_support = (cur_price - nearest_support) / nearest_support * 100
        support_pct = round(pct_from_support, 2)
        if pct_from_support <= 1.0:
            support_score = 25
            notes.append(f'@ support ${nearest_support}')
        elif pct_from_support <= 2.0:
            support_score = 15
            notes.append(f'near support ${nearest_support}')
        elif pct_from_support <= 3.0:
            support_score = 5
    score += support_score

    # Layer 4 — Options Sentiment (20 pts)
    options = _options_sentiment(symbol, cur_price)
    sentiment_score = options.get('sentiment_score', 0)
    if options:
        notes.append(f"P/C={options.get('pc_ratio', '?')}")
    score += sentiment_score

    # ── Stops & Targets ──────────────────────────────────────────
    stop     = round(cur_price - 2.0 * atr_val, 2)
    target_1 = round(cur_price + 3.0 * atr_val, 2)
    target_2 = round(cur_price + 6.0 * atr_val, 2)

    # Check if T2 is within 1-sigma 10-day expected move
    t2_within_range = False
    if options and options.get('upper_1sigma_10d'):
        t2_within_range = target_2 <= options['upper_1sigma_10d']

    result = {
        'ticker':             symbol,
        'score':              score,
        'price':              round(cur_price, 2),
        'ema21':              round(ema21, 2),
        'ema50':              round(ema50, 2),
        'ema200':             round(ema200, 2) if ema200 else None,
        'rsi':                rsi_val,
        'atr':                round(atr_val, 2),
        'stop':               stop,
        'target_1':           target_1,
        'target_2':           target_2,
        'nearest_support':    nearest_support,
        'support_pct':        support_pct,
        'nearest_resistance': nearest_resistance,
        'options':            options,
        't2_within_1sigma':   t2_within_range,
        'notes':              notes,
        'ema_score':          ema_score,
        'rsi_score':          rsi_score,
        'support_score':      support_score,
        'sentiment_score':    sentiment_score,
        'scanned_at':         datetime.now(timezone.utc).isoformat(),
        'signal':             score >= SIGNAL_THRESHOLD,
    }
    return result


# ── Scan loop ────────────────────────────────────────────────────────────────

def _run_scan():
    logger.info('Swing scan starting — FAANG universe')
    all_scores = []
    signals    = []

    for symbol in FAANG:
        try:
            result = _score_symbol(symbol)
            if result:
                all_scores.append(result)
                if result['signal']:
                    signals.append(result)
                    logger.info(f'SWING SIGNAL: {symbol} score={result["score"]} '
                                f'price={result["price"]} stop={result["stop"]} '
                                f't2={result["target_2"]}')
                else:
                    logger.info(f'No signal: {symbol} score={result["score"]}')
        except Exception as e:
            logger.error(f'Score {symbol}: {e}')

    signals.sort(key=lambda x: x['score'], reverse=True)
    all_scores.sort(key=lambda x: x['score'], reverse=True)

    now = datetime.now(_ET)
    with _state_lock:
        _state['signals']    = signals
        _state['all_scores'] = all_scores
        _state['last_scan']  = datetime.now(timezone.utc).isoformat()
        _state['scan_date']  = now.strftime('%Y-%m-%d')
        _state['scan_count'] += 1
        _state['error']      = None

    logger.info(f'Swing scan complete — {len(signals)}/{len(FAANG)} signals')


def _scanner_loop():
    last_scan_key = ''  # 'YYYY-MM-DD_HH' prevents double-firing same window
    while True:
        try:
            now = datetime.now(_ET)
            today = now.strftime('%Y-%m-%d')
            is_weekday = now.weekday() < 5
            not_holiday = today not in _US_HOLIDAYS

            if is_weekday and not_holiday:
                for scan_h, scan_m in SCAN_WINDOWS_ET:
                    in_window = (now.hour == scan_h and now.minute >= scan_m) or \
                                (now.hour == scan_h + 1 and now.minute < scan_m)  # 1-hr grace
                    scan_key = f'{today}_{scan_h:02d}'
                    if in_window and last_scan_key != scan_key:
                        with _state_lock:
                            _state['running'] = True
                        _run_scan()
                        last_scan_key = scan_key
                        with _state_lock:
                            _state['running'] = False
                        break
        except Exception as e:
            logger.error(f'Swing scan loop error: {e}')
            with _state_lock:
                _state['error']   = str(e)
                _state['running'] = False

        time.sleep(MONITOR_SECS)


def start(*_):
    """Start the swing scanner background thread."""
    if os.environ.get('SWING_SCANNER_ENABLED', 'false').lower() != 'true':
        logger.info('Swing scanner disabled (SWING_SCANNER_ENABLED != true)')
        return
    t = threading.Thread(target=_scanner_loop, daemon=True, name='swing-scanner')
    t.start()
    logger.info('Swing scanner thread started')


def get_status() -> dict:
    with _state_lock:
        return dict(_state)


def get_signals() -> list:
    with _state_lock:
        return list(_state['signals'])


def get_all_scores() -> list:
    with _state_lock:
        return list(_state['all_scores'])
