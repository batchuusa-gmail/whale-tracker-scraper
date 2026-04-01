"""
WHAL-102 — Market Regime Detector
Classifies the current market into one of 4 regimes:
  BULL     — Low-vol trending up  → momentum strategies favored
  BEAR     — High-vol trending down → defensive, follow put sweeps
  SIDEWAYS — Range-bound          → mean-reversion strategies
  CRISIS   — Volatility spike     → go to cash, pause new entries

7 indicators (all from free APIs):
  1. SPY 200-day MA slope         (Alpaca existing)
  2. VIX level                    (Alpha Vantage free / yfinance)
  3. VIX term structure           (yfinance ^VIX vs ^VIX3M)
  4. HY credit spread             (FRED: BAMLH0A0HYM2)
  5. Put/Call ratio 20-day MA     (CBOE via yfinance)
  6. Market breadth % above 50MA  (computed from S&P 500 prices)
  7. Yield curve 2yr-10yr         (FRED: T10Y2Y)

Kill switch: always-on (regime detection is read-only, no trading side effects here)
"""

import os
import json
import logging
import threading
import time
from datetime import datetime, timedelta, date

import pytz
import requests
import yfinance as yf

logger = logging.getLogger(__name__)

_ET = pytz.timezone('America/New_York')

FRED_BASE  = 'https://fred.stlouisfed.org/graph/fredgraph.csv'
FRED_API   = os.getenv('FRED_API_KEY', '')   # optional — public series work without key

# Regime cache TTL: 4 hours (re-evaluate intraday if VIX spikes)
REGIME_TTL = 14400

# S&P 500 breadth sample — top 50 by market cap (proxy for full index)
BREADTH_SAMPLE = [
    'AAPL', 'MSFT', 'NVDA', 'AMZN', 'GOOGL', 'META', 'TSLA', 'BRK-B', 'JPM', 'V',
    'UNH', 'XOM', 'MA', 'LLY', 'JNJ', 'PG', 'HD', 'MRK', 'AVGO', 'COST',
    'ABBV', 'CVX', 'BAC', 'KO', 'PEP', 'TMO', 'CSCO', 'WMT', 'MCD', 'CRM',
    'ACN', 'ABT', 'ORCL', 'LIN', 'AMD', 'NFLX', 'TXN', 'PM', 'DHR', 'NEE',
    'ADBE', 'QCOM', 'DIS', 'BMY', 'AMGN', 'UNP', 'HON', 'RTX', 'INTC', 'SBUX',
]


# ── FRED helper ──────────────────────────────────────────────────────

def _fred_latest(series_id: str) -> float | None:
    """Fetch the latest value of a FRED series via public CSV endpoint."""
    try:
        url = f'{FRED_BASE}?id={series_id}'
        r = requests.get(url, timeout=15)
        if r.status_code == 200:
            lines = r.text.strip().split('\n')
            # Last non-empty line: DATE,VALUE
            for line in reversed(lines):
                if line and not line.startswith('DATE'):
                    parts = line.split(',')
                    if len(parts) == 2 and parts[1] not in ('.', ''):
                        return float(parts[1])
    except Exception as e:
        logger.warning(f'FRED {series_id}: {e}')
    return None


# ── Individual indicator fetchers ────────────────────────────────────

def _spy_200ma_slope() -> dict:
    """SPY 200-day MA slope — positive = bull, negative = bear."""
    try:
        spy = yf.download('SPY', period='1y', interval='1d', progress=False, auto_adjust=True)
        if spy.empty or len(spy) < 200:
            return {'value': 0.0, 'signal': 'NEUTRAL', 'label': '200MA slope'}
        closes = spy['Close'].values.flatten()
        ma200_now  = closes[-200:].mean()
        ma200_prev = closes[-220:-20].mean()
        slope = (ma200_now - ma200_prev) / ma200_prev * 100
        signal = 'BULL' if slope > 0.1 else 'BEAR' if slope < -0.1 else 'NEUTRAL'
        return {'value': round(slope, 3), 'signal': signal, 'label': '200MA Slope %'}
    except Exception as e:
        logger.warning(f'200MA slope error: {e}')
        return {'value': 0.0, 'signal': 'NEUTRAL', 'label': '200MA Slope %'}


def _vix_level() -> dict:
    """VIX level — <15 bull, >25 bear, >35 crisis."""
    try:
        vix = yf.download('^VIX', period='5d', interval='1d', progress=False, auto_adjust=True)
        if vix.empty:
            return {'value': 20.0, 'signal': 'NEUTRAL', 'label': 'VIX Level'}
        val = float(vix['Close'].iloc[-1])
        if val > 35:   signal = 'CRISIS'
        elif val > 25: signal = 'BEAR'
        elif val < 15: signal = 'BULL'
        else:          signal = 'NEUTRAL'
        return {'value': round(val, 2), 'signal': signal, 'label': 'VIX Level'}
    except Exception as e:
        logger.warning(f'VIX level error: {e}')
        return {'value': 20.0, 'signal': 'NEUTRAL', 'label': 'VIX Level'}


def _vix_term_structure() -> dict:
    """VIX term structure — contango=bull (VIX < VIX3M), backwardation=crisis."""
    try:
        vix  = yf.download('^VIX',  period='5d', interval='1d', progress=False, auto_adjust=True)
        vix3 = yf.download('^VIX3M', period='5d', interval='1d', progress=False, auto_adjust=True)
        if vix.empty or vix3.empty:
            return {'value': 0.0, 'signal': 'NEUTRAL', 'label': 'VIX Term Structure'}
        spot   = float(vix['Close'].iloc[-1])
        front  = float(vix3['Close'].iloc[-1])
        spread = spot - front   # negative = contango (normal/bull), positive = backwardation (crisis)
        if spread < -2:   signal = 'BULL'
        elif spread > 2:  signal = 'CRISIS'
        else:             signal = 'NEUTRAL'
        return {'value': round(spread, 2), 'signal': signal, 'label': 'VIX Term Structure (spot-3m)'}
    except Exception as e:
        logger.warning(f'VIX term structure error: {e}')
        return {'value': 0.0, 'signal': 'NEUTRAL', 'label': 'VIX Term Structure'}


def _credit_spread() -> dict:
    """HY-IG OAS spread from FRED BAMLH0A0HYM2 — <300bps bull, >500bps bear."""
    try:
        val = _fred_latest('BAMLH0A0HYM2')
        if val is None:
            return {'value': 350.0, 'signal': 'NEUTRAL', 'label': 'HY Credit Spread (bps)'}
        if val > 700:   signal = 'CRISIS'
        elif val > 500: signal = 'BEAR'
        elif val < 300: signal = 'BULL'
        else:           signal = 'NEUTRAL'
        return {'value': round(val, 1), 'signal': signal, 'label': 'HY Credit Spread (bps)'}
    except Exception as e:
        logger.warning(f'Credit spread error: {e}')
        return {'value': 350.0, 'signal': 'NEUTRAL', 'label': 'HY Credit Spread (bps)'}


def _put_call_ratio() -> dict:
    """Equity put/call ratio 20-day MA — <0.8 bull (complacency), >1.1 bear (fear)."""
    try:
        # CBOE equity P/C ratio via yfinance
        pc = yf.download('^PCALL', period='30d', interval='1d', progress=False, auto_adjust=True)
        if pc.empty:
            # Fallback: use total P/C ratio
            pc = yf.download('^PCRATIO', period='30d', interval='1d', progress=False, auto_adjust=True)
        if pc.empty:
            return {'value': 0.9, 'signal': 'NEUTRAL', 'label': 'Put/Call Ratio (20d MA)'}
        ma20 = float(pc['Close'].dropna().tail(20).mean())
        if ma20 < 0.7:   signal = 'BULL'    # extreme complacency
        elif ma20 < 0.8: signal = 'BULL'
        elif ma20 > 1.2: signal = 'CRISIS'
        elif ma20 > 1.0: signal = 'BEAR'
        else:            signal = 'NEUTRAL'
        return {'value': round(ma20, 3), 'signal': signal, 'label': 'Put/Call Ratio (20d MA)'}
    except Exception as e:
        logger.warning(f'Put/call ratio error: {e}')
        return {'value': 0.9, 'signal': 'NEUTRAL', 'label': 'Put/Call Ratio (20d MA)'}


def _market_breadth() -> dict:
    """% of BREADTH_SAMPLE stocks above their 50-day MA — >70% bull, <30% bear."""
    try:
        data = yf.download(BREADTH_SAMPLE, period='3mo', interval='1d',
                           progress=False, auto_adjust=True)
        closes = data['Close']
        above = 0
        total = 0
        for ticker in BREADTH_SAMPLE:
            if ticker in closes.columns:
                series = closes[ticker].dropna()
                if len(series) >= 50:
                    total += 1
                    if float(series.iloc[-1]) > float(series.tail(50).mean()):
                        above += 1
        if total == 0:
            return {'value': 50.0, 'signal': 'NEUTRAL', 'label': 'Breadth % above 50MA'}
        pct = above / total * 100
        signal = 'BULL' if pct > 70 else 'BEAR' if pct < 30 else 'NEUTRAL'
        return {'value': round(pct, 1), 'signal': signal, 'label': 'Breadth % above 50MA'}
    except Exception as e:
        logger.warning(f'Market breadth error: {e}')
        return {'value': 50.0, 'signal': 'NEUTRAL', 'label': 'Breadth % above 50MA'}


def _yield_curve() -> dict:
    """10yr-2yr yield curve from FRED T10Y2Y — positive+steepening=bull, inverted=bear."""
    try:
        val = _fred_latest('T10Y2Y')
        if val is None:
            return {'value': 0.0, 'signal': 'NEUTRAL', 'label': 'Yield Curve (10y-2y bps)'}
        # T10Y2Y is in percentage points
        if val > 0.5:    signal = 'BULL'
        elif val < -0.2: signal = 'BEAR'
        else:            signal = 'NEUTRAL'
        return {'value': round(val, 3), 'signal': signal, 'label': 'Yield Curve (10y-2y %)'}
    except Exception as e:
        logger.warning(f'Yield curve error: {e}')
        return {'value': 0.0, 'signal': 'NEUTRAL', 'label': 'Yield Curve (10y-2y %)'}


# ── Regime classification ────────────────────────────────────────────

REGIME_WEIGHTS = {
    'BULL':     {'momentum': 1.4, 'pead': 1.3, 'options_flow': 1.0, 'mean_reversion': 0.5, 'intraday': 1.2},
    'BEAR':     {'momentum': 0.3, 'pead': 0.6, 'options_flow': 1.0, 'mean_reversion': 0.7, 'intraday': 0.5},
    'SIDEWAYS': {'momentum': 0.5, 'pead': 0.8, 'options_flow': 0.8, 'mean_reversion': 1.5, 'intraday': 0.8},
    'CRISIS':   {'momentum': 0.1, 'pead': 0.2, 'options_flow': 0.5, 'mean_reversion': 0.3, 'intraday': 0.0},
}

REGIME_DESCRIPTIONS = {
    'BULL':     'Momentum Favored — Follow insider buys, ride trends',
    'BEAR':     'Defensive Mode — Reduce exposure, follow put sweeps',
    'SIDEWAYS': 'Mean-Reversion Mode — Range trades, options premium selling',
    'CRISIS':   'Cash Mode — Pause new entries, close momentum longs',
}


def _classify_regime(indicators: dict) -> tuple[str, float]:
    """
    Score indicators and return (regime, confidence).
    Each indicator votes: BULL=+1, BEAR=-1, CRISIS=-2, NEUTRAL=0
    """
    signals = [v['signal'] for v in indicators.values()]

    bull_votes   = signals.count('BULL')
    bear_votes   = signals.count('BEAR')
    crisis_votes = signals.count('CRISIS')
    neutral      = signals.count('NEUTRAL')
    total        = len(signals)

    # Crisis overrides: any 2 crisis signals or VIX+credit spread both crisis
    vix_crisis    = indicators.get('vix_level', {}).get('signal') == 'CRISIS'
    credit_crisis = indicators.get('credit_spread', {}).get('signal') == 'CRISIS'
    ts_crisis     = indicators.get('vix_term_structure', {}).get('signal') == 'CRISIS'

    if crisis_votes >= 2 or (vix_crisis and credit_crisis) or (vix_crisis and ts_crisis):
        confidence = min(0.95, 0.5 + crisis_votes * 0.15)
        return 'CRISIS', round(confidence, 2)

    if bear_votes + crisis_votes >= 4:
        confidence = min(0.92, 0.5 + (bear_votes + crisis_votes) * 0.08)
        return 'BEAR', round(confidence, 2)

    if bull_votes >= 4:
        confidence = min(0.92, 0.5 + bull_votes * 0.08)
        return 'BULL', round(confidence, 2)

    # Sideways: mostly neutral, balanced bull/bear
    if neutral >= 3 or (abs(bull_votes - bear_votes) <= 1 and bull_votes + bear_votes <= 3):
        confidence = 0.55 + neutral * 0.05
        return 'SIDEWAYS', round(min(confidence, 0.80), 2)

    # Weak majority
    if bull_votes > bear_votes:
        return 'BULL', round(0.5 + (bull_votes - bear_votes) * 0.06, 2)
    elif bear_votes > bull_votes:
        return 'BEAR', round(0.5 + (bear_votes - bull_votes) * 0.06, 2)

    return 'SIDEWAYS', 0.55


# ── Main entry points ────────────────────────────────────────────────

def compute_regime(redis_set_fn=None, redis_get_fn=None) -> dict:
    """
    Fetch all 7 indicators, classify regime, cache result.
    Takes ~15-30s due to yfinance downloads. Run in background thread.
    """
    logger.info('Regime detector: computing...')
    now_et = datetime.now(_ET)

    indicators = {
        'spy_200ma_slope':    _spy_200ma_slope(),
        'vix_level':          _vix_level(),
        'vix_term_structure': _vix_term_structure(),
        'credit_spread':      _credit_spread(),
        'put_call_ratio':     _put_call_ratio(),
        'market_breadth':     _market_breadth(),
        'yield_curve':        _yield_curve(),
    }

    regime, confidence = _classify_regime(indicators)

    result = {
        'regime':       regime,
        'confidence':   confidence,
        'description':  REGIME_DESCRIPTIONS[regime],
        'weights':      REGIME_WEIGHTS[regime],
        'indicators':   indicators,
        'computed_at':  now_et.isoformat(),
        'next_update':  (now_et + timedelta(hours=4)).isoformat(),
    }

    logger.info(f'Regime: {regime} ({confidence*100:.0f}% confidence)')

    if redis_set_fn:
        try:
            redis_set_fn('regime:current', json.dumps(result), ex=REGIME_TTL)
        except Exception as e:
            logger.warning(f'Regime cache write failed: {e}')

    return result


def get_regime(redis_get_fn=None) -> dict | None:
    """Return cached regime or None."""
    if redis_get_fn:
        try:
            raw = redis_get_fn('regime:current')
            if raw:
                return json.loads(raw)
        except Exception as e:
            logger.warning(f'Regime cache read failed: {e}')
    return None


def get_strategy_weights(redis_get_fn=None) -> dict:
    """Return signal weights for current regime. Defaults to SIDEWAYS if unknown."""
    cached = get_regime(redis_get_fn)
    if cached:
        return cached.get('weights', REGIME_WEIGHTS['SIDEWAYS'])
    return REGIME_WEIGHTS['SIDEWAYS']


# ── Background thread ────────────────────────────────────────────────

_last_computed: str | None = None


def _monitor_loop(redis_set_fn, redis_get_fn):
    global _last_computed
    logger.info('Regime detector: background thread started')

    # Compute immediately on startup
    try:
        compute_regime(redis_set_fn, redis_get_fn)
        _last_computed = datetime.now(_ET).strftime('%Y-%m-%d %H')
    except Exception as e:
        logger.error(f'Regime startup compute failed: {e}')

    while True:
        try:
            now_et   = datetime.now(_ET)
            hour_key = now_et.strftime('%Y-%m-%d %H')

            # Recompute every 4 hours (at market open, midday, mid-afternoon, close)
            compute_hours = {9, 11, 13, 15}
            if now_et.hour in compute_hours and hour_key != _last_computed:
                _last_computed = hour_key
                compute_regime(redis_set_fn, redis_get_fn)

            # Intraday VIX spike check — recompute immediately if VIX jumps >5%
            cached = get_regime(redis_get_fn)
            if cached:
                cached_vix = cached.get('indicators', {}).get('vix_level', {}).get('value', 0)
                try:
                    vix_now = yf.download('^VIX', period='1d', interval='5m',
                                         progress=False, auto_adjust=True)
                    if not vix_now.empty:
                        current_vix = float(vix_now['Close'].iloc[-1])
                        if cached_vix > 0 and abs(current_vix - cached_vix) / cached_vix > 0.05:
                            logger.info(f'VIX spike detected ({cached_vix} → {current_vix}) — recomputing regime')
                            _last_computed = None
                            compute_regime(redis_set_fn, redis_get_fn)
                except Exception:
                    pass

        except Exception as e:
            logger.error(f'Regime monitor error: {e}')

        time.sleep(900)  # check every 15 minutes


def start(redis_set_fn, redis_get_fn):
    t = threading.Thread(
        target=_monitor_loop,
        args=(redis_set_fn, redis_get_fn),
        daemon=True,
        name='regime-detector',
    )
    t.start()
    logger.info('Regime detector started')
