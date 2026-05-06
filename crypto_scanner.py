"""
crypto_scanner.py — Momentum-based algo scanner for crypto coins.

Scans BTC/USD, ETH/USD, SOL/USD, DOGE/USD using Alpaca crypto data API.
Scoring: short-term momentum + volume spike + RSI zone.
Fires BUY signals to crypto_executor when confidence >= threshold.
Runs 24/7 (crypto never closes).
"""
import os
import json
import logging
import threading
import time
from datetime import datetime, timezone
import pytz
import requests

logger = logging.getLogger(__name__)

_ET = pytz.timezone('America/New_York')

ALPACA_KEY    = os.environ.get('ALPACA_KEY', '')
ALPACA_SECRET = os.environ.get('ALPACA_SECRET', '')

_DATA_BASE  = 'https://data.alpaca.markets'
_PAPER_BASE = 'https://paper-api.alpaca.markets'

SCAN_INTERVAL = 300  # 5 minutes

_ENABLED_COINS_DEFAULT = ['BTC/USD', 'ETH/USD', 'SOL/USD', 'DOGE/USD']

_DEFAULT_CONFIG = {
    'enabled':            True,
    'enabled_coins':      _ENABLED_COINS_DEFAULT,
    'min_confidence':     0.55,
    'max_trades_per_day': 4,
    'position_size_pct':  0.25,  # 25% of $5k capital pool = $1,250 per trade (4 trades max = $5k)
    'stop_loss_pct':      0.03,
    'take_profit_pct':    0.06,
    'min_score':          55,
    'scan_interval':      SCAN_INTERVAL,
}

# In-memory state
_state = {
    'running':        False,
    'last_scan':      None,
    'scan_count':     0,
    'signals_today':  0,
    'top_picks':      [],
    'error':          None,
}
_state_lock = threading.Lock()
_thread: threading.Thread | None = None


# ── Helpers ───────────────────────────────────────────────────────────────────

def _headers() -> dict:
    return {
        'APCA-API-KEY-ID':     ALPACA_KEY,
        'APCA-API-SECRET-KEY': ALPACA_SECRET,
    }


def _fetch_bars(symbol: str, timeframe: str = '5Min', limit: int = 30) -> list:
    """Fetch OHLCV bars from Alpaca crypto data API."""
    try:
        r = requests.get(
            f'{_DATA_BASE}/v1beta3/crypto/us/bars',
            headers=_headers(),
            params={'symbols': symbol, 'timeframe': timeframe, 'limit': limit, 'sort': 'asc'},
            timeout=10,
        )
        if r.status_code != 200:
            return []
        data = r.json()
        bars = data.get('bars', {}).get(symbol, [])
        return bars
    except Exception as e:
        logger.warning(f'crypto_scanner _fetch_bars {symbol}: {e}')
        return []


def _calc_rsi(closes: list, period: int = 14) -> float:
    """Simple RSI calculation."""
    if len(closes) < period + 1:
        return 50.0
    gains, losses = [], []
    for i in range(1, len(closes)):
        delta = closes[i] - closes[i - 1]
        gains.append(max(delta, 0))
        losses.append(max(-delta, 0))
    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return round(100 - (100 / (1 + rs)), 1)


def _score_coin(symbol: str, config: dict) -> dict | None:
    """
    Crypto-native scoring 0–100. Momentum is primary signal.
    RSI is used only to avoid entering during sharp reversals — NOT as
    an overbought block. Crypto can stay RSI 80+ for weeks in bull runs.

    Signals:
      - 5-min momentum  (primary — 40 pts max)
      - 1-hr trend      (secondary — 25 pts max)
      - volume spike    (confirmation — 20 pts max)
      - RSI direction   (mild filter — ±10)
    """
    bars = _fetch_bars(symbol, '5Min', 30)
    if len(bars) < 15:
        logger.debug(f'crypto_scanner: not enough bars for {symbol} ({len(bars)})')
        return None

    closes  = [float(b['c']) for b in bars]
    volumes = [float(b['v']) for b in bars]
    price   = closes[-1]

    # Momentum over multiple windows
    mom_5m  = (closes[-1] - closes[-5])  / closes[-5]  * 100 if closes[-5]  else 0
    mom_1h  = (closes[-1] - closes[-13]) / closes[-13] * 100 if closes[-13] else 0
    mom_3m  = (closes[-1] - closes[-3])  / closes[-3]  * 100 if closes[-3]  else 0

    # Volume spike vs prev 5 bars
    avg_vol   = sum(volumes[-6:-1]) / 5 if len(volumes) >= 6 else volumes[-1]
    vol_spike = volumes[-1] / avg_vol if avg_vol > 0 else 1.0

    rsi = _calc_rsi(closes)

    # ── Scoring (crypto-native) ───────────────────────────────────────
    score = 35  # baseline

    # 5-min momentum — primary signal (max +40)
    if mom_5m > 2.0:
        score += 40
    elif mom_5m > 1.0:
        score += 30
    elif mom_5m > 0.4:
        score += 20
    elif mom_5m > 0.1:
        score += 10
    elif mom_5m < -1.5:
        score -= 20  # significant drop — skip
    elif mom_5m < -0.5:
        score -= 10

    # 3-min short burst (bonus for very recent acceleration)
    if mom_3m > 0.5 and mom_5m > 0:
        score += 8

    # 1-hr trend alignment (max +25)
    if mom_1h > 3.0:
        score += 25
    elif mom_1h > 1.5:
        score += 18
    elif mom_1h > 0.5:
        score += 10
    elif mom_1h < -2.0:
        score -= 15
    elif mom_1h < -0.5:
        score -= 5

    # Volume confirmation (max +20)
    if vol_spike > 3.0:
        score += 20
    elif vol_spike > 2.0:
        score += 14
    elif vol_spike > 1.3:
        score += 7

    # RSI — only penalise extreme reversals, not sustained overbought
    # Crypto stays RSI 70-85 during bull trends; that's fine
    if rsi < 25:
        score -= 10  # capitulation / knife-catch risk
    elif rsi > 92:
        score -= 10  # parabolic blow-off, near-term pullback likely

    score = max(0, min(100, score))
    confidence = round(score / 100, 3)

    return {
        'symbol':      symbol,
        'ticker':      symbol.replace('/', ''),
        'price':       round(price, 6),
        'score':       score,
        'confidence':  confidence,
        'mom_3m':      round(mom_3m, 3),
        'mom_5m':      round(mom_5m, 3),
        'mom_1h':      round(mom_1h, 3),
        'vol_spike':   round(vol_spike, 2),
        'rsi':         rsi,
        'signal':      'BUY' if score >= config.get('min_score', 55) else 'WAIT',
    }


# ── Config ────────────────────────────────────────────────────────────────────

_SUPA_URL = os.environ.get('SUPABASE_URL', 'https://bedurjtazsfbnkisoeee.supabase.co')
_SUPA_KEY = os.environ.get('SUPABASE_KEY', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJlZHVyanRhenNmYm5raXNvZWVlIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzM2NDcxOTUsImV4cCI6MjA4OTIyMzE5NX0.MK4N9dxAIHlXkGP4rLJPq5tHh9UU8L75EB1b8Q7CVmg')
_SUPA_HDRS = {
    'apikey':        _SUPA_KEY,
    'Authorization': f'Bearer {_SUPA_KEY}',
    'Content-Type':  'application/json',
    'Prefer':        'resolution=merge-duplicates,return=minimal',
}

_config_lock = threading.Lock()
_live_config: dict = {}


def get_config(redis_get_fn=None) -> dict:
    """Return current algo config (Redis → memory → defaults)."""
    if redis_get_fn:
        cached = redis_get_fn('crypto:config')
        if cached and isinstance(cached, dict):
            return {**_DEFAULT_CONFIG, **cached}
    with _config_lock:
        if _live_config:
            return {**_DEFAULT_CONFIG, **_live_config}
    return dict(_DEFAULT_CONFIG)


def update_config(body: dict, redis_set_fn=None):
    """Persist config changes to memory and Redis."""
    with _config_lock:
        _live_config.update(body)
    if redis_set_fn:
        try:
            redis_set_fn('crypto:config', {**_DEFAULT_CONFIG, **_live_config}, ttl_seconds=86400)
        except Exception:
            pass
    logger.info(f'crypto_scanner: config updated: {body}')


def get_status(redis_get_fn=None) -> dict:
    """Return scanner state for /api/crypto/algo/status."""
    with _state_lock:
        return {
            'running':        _state['running'],
            'last_scan':      _state['last_scan'],
            'scan_count':     _state['scan_count'],
            'signals_today':  _state['signals_today'],
            'top_picks':      _state['top_picks'],
            'error':          _state['error'],
            'market_open':    True,  # crypto 24/7
            'mode':           'paper',
        }


# ── Scan loop ─────────────────────────────────────────────────────────────────

def _run_scan(redis_set_fn=None, redis_get_fn=None):
    """Execute one scan cycle across all enabled coins."""
    config = get_config(redis_get_fn)
    coins  = config.get('enabled_coins', _ENABLED_COINS_DEFAULT)
    min_score = config.get('min_score', 65)

    results = []
    signals = []

    for symbol in coins:
        try:
            result = _score_coin(symbol, config)
            if result is None:
                continue
            results.append(result)
            if result['signal'] == 'BUY' and result['confidence'] >= config.get('min_confidence', 0.68):
                signals.append(result)
                logger.info(
                    f'crypto_scanner: signal {symbol} score={result["score"]} '
                    f'conf={result["confidence"]} mom5m={result["mom_5m"]}%'
                )
        except Exception as e:
            logger.warning(f'crypto_scanner: error scoring {symbol}: {e}')

    results.sort(key=lambda x: x['score'], reverse=True)

    with _state_lock:
        _state['last_scan']     = datetime.now(timezone.utc).isoformat()
        _state['scan_count']   += 1
        _state['top_picks']     = results[:5]
        _state['signals_today'] += len(signals)

    if redis_set_fn:
        try:
            redis_set_fn('crypto:scan:results', results[:10], ttl_seconds=360)
        except Exception:
            pass

    # Fire signals to executor
    if signals:
        _fire_signals(signals, config)

    return results


def _fire_signals(signals: list, config: dict):
    """Hand top signals to crypto_executor for order placement."""
    try:
        from crypto_executor import place_crypto_order, get_status as executor_status
        status = executor_status()
        trades_today = status.get('trades_today', 0)
        max_trades   = config.get('max_trades_per_day', 4)

        if trades_today >= max_trades:
            logger.info(f'crypto_scanner: max trades/day reached ({trades_today}/{max_trades}), skipping')
            return

        # Only fire the top signal per scan cycle
        top = signals[0]
        place_crypto_order({
            'ticker':           top['ticker'],
            'symbol':           top['symbol'],
            'signal':           'BUY',
            'confidence':       top['confidence'],
            'score':            top['score'],
            'entry':            top['price'],
            'stop_loss_pct':    config.get('stop_loss_pct', 0.03),
            'take_profit_pct':  config.get('take_profit_pct', 0.06),
            'position_size_pct': config.get('position_size_pct', 0.05),
            'reasoning':        f'Score {top["score"]}/100 | mom5m={top["mom_5m"]}% | RSI={top["rsi"]}',
        })
    except Exception as e:
        logger.error(f'crypto_scanner: _fire_signals error: {e}')


def _scan_loop(redis_set_fn=None, redis_get_fn=None):
    with _state_lock:
        _state['running'] = True

    logger.info('crypto_scanner: scan loop started (24/7)')

    while True:
        try:
            _run_scan(redis_set_fn, redis_get_fn)
        except Exception as e:
            logger.error(f'crypto_scanner: scan cycle error: {e}')
            with _state_lock:
                _state['error'] = str(e)

        config = get_config(redis_get_fn)
        interval = config.get('scan_interval', SCAN_INTERVAL)
        time.sleep(interval)


def start(redis_set_fn=None, redis_get_fn=None):
    """Launch background scan thread. Called once at app startup."""
    global _thread
    if _thread and _thread.is_alive():
        logger.info('crypto_scanner: already running')
        return
    _thread = threading.Thread(
        target=_scan_loop,
        args=(redis_set_fn, redis_get_fn),
        daemon=True,
        name='crypto-scanner',
    )
    _thread.start()
    logger.info('crypto_scanner: background thread launched')
