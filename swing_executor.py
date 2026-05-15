"""
FAANG Swing Trade Executor
Places paper trades on signals from swing_scanner.py.

Rules:
  - Stop:     2× daily ATR below entry
  - Target 1: 3× daily ATR → sell 50%
  - Target 2: 6× daily ATR → sell remainder
  - Max $20,000 per trade (20% of buying power)
  - Max 5 positions (one per FAANG stock)
  - Position monitor every 5 minutes
  - No forced close — hold until stop or target

Kill switch: SWING_TRADING_ENABLED=true/false env var
"""
from __future__ import annotations

import os
import logging
import threading
import time
from datetime import datetime, timezone

import requests

logger = logging.getLogger(__name__)

PAPER_BASE    = 'https://paper-api.alpaca.markets'
ALPACA_DATA   = 'https://data.alpaca.markets'
MAX_TRADE_USD = 20_000
MAX_POSITIONS = 5
MONITOR_SECS  = 300   # 5 minutes

_SUPA_URL = os.environ.get('SUPABASE_URL', 'https://bedurjtazsfbnkisoeee.supabase.co')
_SUPA_KEY = os.environ.get('SUPABASE_KEY', '')
_SUPA_HDRS = {
    'apikey':        _SUPA_KEY,
    'Authorization': f'Bearer {_SUPA_KEY}',
    'Content-Type':  'application/json',
    'Prefer':        'resolution=merge-duplicates,return=minimal',
}

_partial_sold: dict[str, bool] = {}
_state_lock = threading.Lock()

FAANG = {'META', 'AAPL', 'AMZN', 'NFLX', 'GOOGL'}


# ── Helpers ──────────────────────────────────────────────────────────────────

def _enabled() -> bool:
    return os.environ.get('SWING_TRADING_ENABLED', 'false').lower() == 'true'


def _headers() -> dict:
    return {
        'APCA-API-KEY-ID':     os.environ.get('ALPACA_KEY', ''),
        'APCA-API-SECRET-KEY': os.environ.get('ALPACA_SECRET', ''),
        'Content-Type': 'application/json',
    }


def _alpaca(method: str, path: str, **kwargs):
    url = f'{PAPER_BASE}{path}'
    try:
        r = getattr(requests, method)(url, headers=_headers(), timeout=15, **kwargs)
        try:
            data = r.json()
        except Exception:
            data = {}
        return r.status_code, data
    except Exception as e:
        logger.error(f'Alpaca {method.upper()} {path}: {e}')
        return 0, {}


def _supa_get(table: str, query: str = '') -> list:
    try:
        r = requests.get(
            f'{_SUPA_URL}/rest/v1/{table}?{query}',
            headers=_SUPA_HDRS, timeout=15,
        )
        if r.status_code == 200:
            return r.json()
    except Exception as e:
        logger.error(f'Supabase {table} get: {e}')
    return []


def _supa_upsert(table: str, rows: list):
    if not rows:
        return
    try:
        r = requests.post(
            f'{_SUPA_URL}/rest/v1/{table}',
            headers=_SUPA_HDRS, json=rows, timeout=15,
        )
        if r.status_code not in (200, 201, 204):
            logger.warning(f'Supabase {table}: {r.status_code} — table may not exist yet')
    except Exception as e:
        logger.error(f'Supabase {table} upsert: {e}')


def _send_fcm(title: str, body: str):
    try:
        from firebase_push import send_to_tokens
        r = requests.get(
            f'{_SUPA_URL}/rest/v1/user_devices?select=fcm_token',
            headers={'apikey': _SUPA_KEY, 'Authorization': f'Bearer {_SUPA_KEY}'},
            timeout=10,
        )
        tokens = [row.get('fcm_token') for row in (r.json() if r.status_code == 200 else []) if row.get('fcm_token')]
        if tokens:
            send_to_tokens(tokens, title, body, {})
    except Exception as e:
        logger.warning(f'FCM: {e}')


# ── Position sizing ──────────────────────────────────────────────────────────

def _calc_qty(entry: float) -> int:
    _, account = _alpaca('get', '/v2/account')
    bp = float(account.get('non_marginable_buying_power') or account.get('buying_power') or 0)
    alloc = min(bp * 0.20, MAX_TRADE_USD)
    return max(1, int(alloc / entry))


# ── Open a swing trade ───────────────────────────────────────────────────────

def execute_swing_signal(signal: dict) -> dict:
    """Place a buy order for a qualifying FAANG swing signal."""
    if not _enabled():
        return {'status': 'disabled'}

    ticker = signal.get('ticker', '')
    if ticker not in FAANG:
        return {'status': 'skipped', 'reason': 'not FAANG'}

    # Check max positions
    _, positions = _alpaca('get', '/v2/positions')
    if not isinstance(positions, list):
        return {'status': 'error', 'reason': 'alpaca error'}

    swing_open = _supa_get('swing_trades', 'status=neq.closed&select=ticker')
    swing_tickers = {r.get('ticker') for r in swing_open}

    if ticker in swing_tickers:
        return {'status': 'skipped', 'reason': f'{ticker} already open'}
    if len(swing_tickers) >= MAX_POSITIONS:
        return {'status': 'skipped', 'reason': f'max {MAX_POSITIONS} swing positions'}

    entry = float(signal.get('price', 0))
    if entry <= 0:
        return {'status': 'error', 'reason': 'invalid price'}

    qty = _calc_qty(entry)
    if qty < 1:
        return {'status': 'error', 'reason': 'insufficient buying power'}

    sc, order = _alpaca('post', '/v2/orders', json={
        'symbol':         ticker,
        'qty':            str(qty),
        'side':           'buy',
        'type':           'market',
        'time_in_force':  'day',
        'extended_hours': False,
    })

    if sc not in (200, 201):
        logger.error(f'Swing order failed {ticker}: {sc} {order}')
        return {'status': 'error', 'reason': str(order)}

    order_id = order.get('id', '')
    now = datetime.now(timezone.utc).isoformat()

    _supa_upsert('swing_trades', [{
        'order_id':    order_id,
        'ticker':      ticker,
        'side':        'buy',
        'qty':         qty,
        'entry_price': entry,
        'stop_loss':   signal.get('stop'),
        'target_1':    signal.get('target_1'),
        'target_2':    signal.get('target_2'),
        'atr14_daily': signal.get('atr'),
        'score':       signal.get('score'),
        'ema21':       signal.get('ema21'),
        'ema50':       signal.get('ema50'),
        'ema200':      signal.get('ema200'),
        'rsi':         signal.get('rsi'),
        'status':      'open',
        'entry_time':  now,
    }])

    _send_fcm(
        f'SWING BUY {ticker}',
        f'{qty} shares @ ~${entry:.2f} | Stop ${signal.get("stop")} | T2 ${signal.get("target_2")}',
    )
    logger.info(f'Swing BUY {ticker} x{qty} @ ~${entry:.2f}')
    return {'status': 'ok', 'order_id': order_id, 'qty': qty}


# ── Monitor open positions ───────────────────────────────────────────────────

def _latest_price(ticker: str) -> float | None:
    key    = os.environ.get('ALPACA_KEY', '')
    secret = os.environ.get('ALPACA_SECRET', '')
    try:
        r = requests.get(
            f'{ALPACA_DATA}/v2/stocks/{ticker}/trades/latest',
            headers={'APCA-API-KEY-ID': key, 'APCA-API-SECRET-KEY': secret},
            params={'feed': 'iex'}, timeout=8,
        )
        if r.status_code == 200:
            return float(r.json().get('trade', {}).get('p', 0) or 0) or None
    except Exception:
        pass
    return None


def _check_positions():
    open_trades = _supa_get('swing_trades', 'status=neq.closed&select=*')
    if not open_trades:
        return

    _, alpaca_positions = _alpaca('get', '/v2/positions')
    if not isinstance(alpaca_positions, list):
        return

    alpaca_map = {p.get('symbol'): p for p in alpaca_positions}

    for trade in open_trades:
        ticker = trade.get('ticker', '')
        if not ticker or ticker not in FAANG:
            continue
        if ticker not in alpaca_map:
            continue  # already closed in Alpaca

        pos         = alpaca_map[ticker]
        cur_price   = float(pos.get('current_price') or 0) or _latest_price(ticker) or 0
        entry_price = float(trade.get('entry_price') or pos.get('avg_entry_price') or 0)
        qty         = float(pos.get('qty') or 0)
        stop        = float(trade.get('stop_loss') or entry_price * 0.93)
        t1          = float(trade.get('target_1') or entry_price * 1.05)
        t2          = float(trade.get('target_2') or entry_price * 1.10)

        if cur_price <= 0 or entry_price <= 0:
            continue

        with _state_lock:
            already_partial = _partial_sold.get(ticker, False)

        exit_reason = None
        if cur_price <= stop:
            exit_reason = 'stop_loss'
        elif cur_price >= t2:
            exit_reason = 'target_2'
        elif cur_price >= t1 and not already_partial:
            exit_reason = 'target_1_partial'

        if exit_reason == 'target_1_partial':
            _sell_partial(ticker, trade, cur_price, qty)
        elif exit_reason:
            _close_position(ticker, trade, exit_reason, cur_price, qty, entry_price)


def _sell_partial(ticker: str, trade: dict, cur_price: float, qty: float):
    partial_qty = max(1, int(qty / 2))
    sc, order   = _alpaca('post', '/v2/orders', json={
        'symbol': ticker, 'qty': str(partial_qty),
        'side': 'sell', 'type': 'market', 'time_in_force': 'day',
    })
    if sc in (200, 201):
        logger.info(f'Swing partial sell {partial_qty} {ticker} @ ~${cur_price:.2f}')
        with _state_lock:
            _partial_sold[ticker] = True
        _supa_upsert('swing_trades', [{'order_id': trade.get('order_id', ''), 'ticker': ticker, 'status': 'partial', 'partial_sold': True}])
        _send_fcm(f'SWING PARTIAL {ticker}', f'Sold 50% @ ${cur_price:.2f} (Target 1 hit)')


def _close_position(ticker: str, trade: dict, reason: str, cur_price: float, qty: float, entry_price: float):
    sc, _ = _alpaca('delete', f'/v2/positions/{ticker}')
    if sc in (200, 201, 204):
        pnl     = round((cur_price - entry_price) * qty, 2)
        pnl_pct = round((cur_price - entry_price) / entry_price, 4) if entry_price else 0
        now     = datetime.now(timezone.utc).isoformat()
        logger.info(f'Swing CLOSE {ticker} reason={reason} pnl=${pnl:.2f}')
        with _state_lock:
            _partial_sold.pop(ticker, None)
        _supa_upsert('swing_trades', [{
            'order_id':   trade.get('order_id', ''),
            'ticker':     ticker,
            'status':     'closed',
            'exit_price': cur_price,
            'exit_time':  now,
            'exit_reason': reason,
            'pnl':        pnl,
            'pnl_pct':    pnl_pct,
        }])
        emoji = '✅' if pnl >= 0 else '🔴'
        _send_fcm(
            f'{emoji} SWING CLOSE {ticker}',
            f'{reason.replace("_", " ").title()} | P&L ${pnl:+.2f} ({pnl_pct:.1%})',
        )


def _monitor_loop():
    while True:
        try:
            if _enabled():
                _check_positions()
        except Exception as e:
            logger.error(f'Swing monitor error: {e}')
        time.sleep(MONITOR_SECS)


def start():
    """Start the swing position monitor thread."""
    t = threading.Thread(target=_monitor_loop, daemon=True, name='swing-monitor')
    t.start()
    logger.info('Swing executor monitor started')


def get_open_positions() -> list:
    """Return open swing positions enriched with current Alpaca data."""
    open_trades = _supa_get('swing_trades', 'status=neq.closed&select=*&order=entry_time.desc')
    if not open_trades:
        return []

    _, alpaca_positions = _alpaca('get', '/v2/positions')
    alpaca_map = {p.get('symbol'): p for p in (alpaca_positions if isinstance(alpaca_positions, list) else [])}

    result = []
    for trade in open_trades:
        ticker    = trade.get('ticker', '')
        pos       = alpaca_map.get(ticker, {})
        cur_price = float(pos.get('current_price') or trade.get('entry_price') or 0)
        entry     = float(trade.get('entry_price') or 0)
        qty       = float(pos.get('qty') or trade.get('qty') or 0)
        unrealized = round((cur_price - entry) * qty, 2) if cur_price and entry else 0

        result.append({
            **trade,
            'current_price':   cur_price,
            'unrealized_pnl':  unrealized,
            'unrealized_pct':  round((cur_price - entry) / entry * 100, 2) if entry else 0,
            'qty':             qty,
        })
    return result
