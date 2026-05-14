"""
crypto_executor.py — Crypto order execution and position monitor.

Uses Alpaca paper API (same keys as stocks).
Crypto trades 24/7 — no market-hours gate.
Writes to Supabase `crypto_trades` table.

Position management:
  Stop loss:    -3%  → full close
  Target 1:    +6%  → sell 50% (partial)
  Target 2:    +12% → close rest
  Time stop:   4 hours max hold
"""
import os
import json
import logging
import threading
import time
from datetime import datetime, timezone, timedelta
import pytz
import requests

logger = logging.getLogger(__name__)

_ET = pytz.timezone('America/New_York')

ALPACA_KEY    = os.environ.get('ALPACA_KEY', '')
ALPACA_SECRET = os.environ.get('ALPACA_SECRET', '')
_PAPER_BASE   = 'https://paper-api.alpaca.markets'
_DATA_BASE    = 'https://data.alpaca.markets'

CRYPTO_TRADING_ENABLED = os.environ.get('CRYPTO_TRADING_ENABLED', 'true').lower() == 'true'
CRYPTO_CAPITAL_USD     = float(os.environ.get('CRYPTO_CAPITAL_USD', '5000'))  # fixed capital pool for crypto

_SUPA_URL  = os.environ.get('SUPABASE_URL', 'https://bedurjtazsfbnkisoeee.supabase.co')
_SUPA_KEY  = os.environ.get('SUPABASE_KEY', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJlZHVyanRhenNmYm5raXNvZWVlIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzM2NDcxOTUsImV4cCI6MjA4OTIyMzE5NX0.MK4N9dxAIHlXkGP4rLJPq5tHh9UU8L75EB1b8Q7CVmg')
_SUPA_HDRS = {
    'apikey':        _SUPA_KEY,
    'Authorization': f'Bearer {_SUPA_KEY}',
    'Content-Type':  'application/json',
    'Prefer':        'resolution=merge-duplicates,return=minimal',
}

# In-memory position tracking: {order_id: {...}}
_open_positions: dict = {}
_positions_lock = threading.Lock()

_state = {
    'running':      False,
    'trades_today': 0,
    'pnl_today':    0.0,
    'circuit_open': False,
    'circuit_reason': '',
}
_state_lock = threading.Lock()

_monitor_thread: threading.Thread | None = None


# ── Supabase helpers ──────────────────────────────────────────────────────────

def _supa_get(table: str, query: str = '') -> list:
    try:
        r = requests.get(
            f'{_SUPA_URL}/rest/v1/{table}?{query}',
            headers=_SUPA_HDRS,
            timeout=10,
        )
        return r.json() if r.status_code == 200 else []
    except Exception as e:
        logger.warning(f'crypto_executor _supa_get {table}: {e}')
        return []


def _supa_insert(table: str, row: dict):
    try:
        r = requests.post(
            f'{_SUPA_URL}/rest/v1/{table}',
            headers=_SUPA_HDRS,
            json=row,
            timeout=10,
        )
        if r.status_code not in (200, 201, 204):
            logger.warning(f'crypto_executor _supa_insert {table}: {r.status_code} {r.text[:200]}')
    except Exception as e:
        logger.warning(f'crypto_executor _supa_insert {table}: {e}')


def _supa_update(table: str, match_query: str, updates: dict):
    try:
        r = requests.patch(
            f'{_SUPA_URL}/rest/v1/{table}?{match_query}',
            headers=_SUPA_HDRS,
            json=updates,
            timeout=10,
        )
        if r.status_code not in (200, 201, 204):
            logger.warning(f'crypto_executor _supa_update {table}: {r.status_code} {r.text[:200]}')
    except Exception as e:
        logger.warning(f'crypto_executor _supa_update {table}: {e}')


# ── Alpaca helpers ────────────────────────────────────────────────────────────

def _alpaca_headers() -> dict:
    return {
        'APCA-API-KEY-ID':     ALPACA_KEY,
        'APCA-API-SECRET-KEY': ALPACA_SECRET,
        'Content-Type':        'application/json',
    }


def _alpaca(method: str, path: str, base: str = _PAPER_BASE, **kwargs):
    url = f'{base}{path}'
    try:
        r = getattr(requests, method)(url, headers=_alpaca_headers(), timeout=15, **kwargs)
        try:
            data = r.json()
        except Exception:
            data = {}
        return r.status_code, data
    except Exception as e:
        logger.error(f'crypto_executor _alpaca {method} {path}: {e}')
        return 0, {}


def _get_portfolio_value() -> float:
    """Return current portfolio equity from Alpaca account."""
    status, data = _alpaca('get', '/v2/account')
    if status == 200:
        return float(data.get('equity', 10000))
    return 10000.0


def _get_latest_price(ticker: str) -> float | None:
    """Fetch latest crypto price from Alpaca data API."""
    # Alpaca crypto data uses symbol with slash: BTCUSD → BTC/USD
    symbol = ticker[:3] + '/' + ticker[3:] if len(ticker) == 6 else ticker
    try:
        status, data = _alpaca(
            'get',
            f'/v1beta3/crypto/us/latest/trades?symbols={symbol}',
            base=_DATA_BASE,
        )
        if status == 200:
            trades = data.get('trades', {})
            trade  = trades.get(symbol) or trades.get(ticker)
            if trade:
                return float(trade.get('p', 0))
    except Exception as e:
        logger.warning(f'crypto_executor _get_latest_price {ticker}: {e}')
    return None


def _get_open_positions() -> list:
    """Return all open Alpaca positions."""
    status, data = _alpaca('get', '/v2/positions')
    if status == 200 and isinstance(data, list):
        return data
    return []


# ── FCM push notification ─────────────────────────────────────────────────────

def _push(title: str, body: str):
    try:
        requests.post(
            'https://whale-tracker-scraper-production.up.railway.app/api/notify/send',
            json={'title': title, 'body': body},
            timeout=8,
        )
    except Exception:
        pass


# ── Order placement ───────────────────────────────────────────────────────────

def place_crypto_order(signal: dict) -> dict:
    """
    Place a market BUY order for a crypto signal.

    signal keys: ticker, symbol, entry, stop_loss_pct, take_profit_pct,
                 position_size_pct, confidence, score, reasoning
    Returns: {status, order_id, ticker, qty, entry, stop_loss, target_1, target_2}
    """
    if not CRYPTO_TRADING_ENABLED:
        logger.info(f'crypto_executor: CRYPTO_TRADING_ENABLED=false — paper-dry-run for {signal.get("ticker")}')
        # Still record the signal but don't submit to Alpaca
        return {'status': 'dry_run', 'ticker': signal.get('ticker'), 'reason': 'trading disabled'}

    with _state_lock:
        if _state['circuit_open']:
            return {'status': 'blocked', 'reason': _state['circuit_reason']}

    ticker          = signal['ticker']          # e.g. BTCUSD
    entry_price     = float(signal['entry'])
    position_pct    = float(signal.get('position_size_pct', 0.05))
    stop_loss_pct   = float(signal.get('stop_loss_pct', 0.03))
    take_profit_pct = float(signal.get('take_profit_pct', 0.06))

    # Use actual available buying power from Alpaca, capped at CRYPTO_CAPITAL_USD
    acct_status, acct = _alpaca('get', '/v2/account')
    available_cash = float(acct.get('non_marginable_buying_power') or acct.get('cash', 0)) if acct_status == 200 else 0
    capital = min(available_cash, CRYPTO_CAPITAL_USD)
    if capital < 10:
        return {'status': 'error', 'reason': f'insufficient buying power (${available_cash:.2f} available)'}

    trade_value = capital * position_pct
    qty         = round(trade_value / entry_price, 6) if entry_price > 0 else 0

    if qty <= 0 or trade_value < 1.0:
        return {'status': 'error', 'reason': 'qty too small'}

    stop_loss  = round(entry_price * (1 - stop_loss_pct), 6)
    target_1   = round(entry_price * (1 + take_profit_pct), 6)
    target_2   = round(entry_price * (1 + take_profit_pct * 2), 6)

    status, order = _alpaca('post', '/v2/orders', json={
        'symbol':          ticker,
        'qty':             str(qty),
        'side':            'buy',
        'type':            'market',
        'time_in_force':   'gtc',
        'client_order_id': f'crypto-{ticker}-{int(time.time())}',
    })

    if status not in (200, 201):
        logger.error(f'crypto_executor: order failed {ticker} {status}: {order}')
        return {'status': 'error', 'http_status': status, 'detail': order}

    order_id = order.get('id', '')
    now_utc  = datetime.now(timezone.utc)
    today_et = datetime.now(_ET).strftime('%Y-%m-%d')

    trade_row = {
        'order_id':     order_id,
        'ticker':       ticker,
        'symbol':       signal.get('symbol', ticker),
        'side':         'BUY',
        'qty':          qty,
        'entry_price':  entry_price,
        'stop_loss':    stop_loss,
        'target_1':     target_1,
        'target_2':     target_2,
        'confidence':   signal.get('confidence', 0),
        'score':        signal.get('score', 0),
        'ai_reasoning': signal.get('reasoning', ''),
        'status':       'open',
        'partial_sold': False,
        'pnl':          None,
        'pnl_pct':      None,
        'exit_price':   None,
        'exit_reason':  None,
        'entry_time':   now_utc.isoformat(),
        'exit_time':    None,
        'date':         today_et,
        'asset_class':  'crypto',
    }

    _supa_insert('crypto_trades', trade_row)

    with _positions_lock:
        _open_positions[order_id] = trade_row.copy()

    with _state_lock:
        _state['trades_today'] += 1

    logger.info(f'crypto_executor: BUY {ticker} qty={qty} entry={entry_price} order={order_id}')
    _push(f'🟢 Crypto BUY: {ticker}', f'Entry ${entry_price:,.4f} | SL ${stop_loss:,.4f} | TP ${target_1:,.4f}')

    return {
        'status':   'submitted',
        'order_id': order_id,
        'ticker':   ticker,
        'qty':      qty,
        'entry':    entry_price,
        'stop_loss': stop_loss,
        'target_1': target_1,
        'target_2': target_2,
    }


# ── Position monitoring ───────────────────────────────────────────────────────

def _close_position(pos: dict, current_price: float, reason: str):
    """Market sell entire remaining position and record to Supabase."""
    ticker   = pos['ticker']
    order_id = pos['order_id']
    qty      = pos['qty']

    partial_sold = pos.get('partial_sold', False)
    sell_qty     = round(qty * 0.5, 6) if not partial_sold else round(qty * 0.5, 6)
    # After partial, we only hold 50%; sell the remainder
    if partial_sold:
        sell_qty = round(qty * 0.5, 6)
    else:
        sell_qty = qty

    status, order = _alpaca('post', '/v2/orders', json={
        'symbol':        ticker,
        'qty':           str(sell_qty),
        'side':          'sell',
        'type':          'market',
        'time_in_force': 'gtc',
    })

    if status not in (200, 201):
        logger.warning(f'crypto_executor: close failed {ticker} {status}: {order}')

    entry_price = float(pos['entry_price'])
    pnl         = round((current_price - entry_price) * qty, 4)
    pnl_pct     = round((current_price - entry_price) / entry_price * 100, 3)
    now_utc     = datetime.now(timezone.utc).isoformat()

    _supa_update(
        'crypto_trades',
        f'order_id=eq.{order_id}',
        {
            'status':      'closed',
            'exit_price':  current_price,
            'exit_reason': reason,
            'exit_time':   now_utc,
            'pnl':         pnl,
            'pnl_pct':     pnl_pct,
        },
    )

    with _positions_lock:
        _open_positions.pop(order_id, None)

    with _state_lock:
        _state['pnl_today'] = round(_state['pnl_today'] + pnl, 4)

    sign = '+' if pnl >= 0 else ''
    logger.info(f'crypto_executor: CLOSE {ticker} price={current_price} pnl={sign}{pnl} reason={reason}')
    emoji = '🟢' if pnl >= 0 else '🔴'
    _push(f'{emoji} Crypto SELL: {ticker}', f'P&L {sign}${pnl:,.4f} | {reason}')


def _sell_partial(pos: dict, current_price: float):
    """Sell 50% at Target 1 and mark partial_sold."""
    ticker   = pos['ticker']
    order_id = pos['order_id']
    qty_half = round(pos['qty'] * 0.5, 6)

    status, order = _alpaca('post', '/v2/orders', json={
        'symbol':        ticker,
        'qty':           str(qty_half),
        'side':          'sell',
        'type':          'market',
        'time_in_force': 'gtc',
    })

    if status in (200, 201):
        with _positions_lock:
            if order_id in _open_positions:
                _open_positions[order_id]['partial_sold'] = True

        _supa_update('crypto_trades', f'order_id=eq.{order_id}', {'partial_sold': True})
        logger.info(f'crypto_executor: PARTIAL SELL {ticker} qty={qty_half} at {current_price}')
        _push(f'⚡ Crypto Partial: {ticker}', f'Sold 50% at ${current_price:,.4f} (Target 1 hit)')
    else:
        logger.warning(f'crypto_executor: partial sell failed {ticker} {status}')


def _check_positions():
    """Check all tracked open positions against stop/target/time limits."""
    with _positions_lock:
        positions_snapshot = dict(_open_positions)

    for order_id, pos in positions_snapshot.items():
        ticker = pos['ticker']
        try:
            current_price = _get_latest_price(ticker)
            if current_price is None or current_price == 0:
                continue

            entry_price  = float(pos['entry_price'])
            stop_loss    = float(pos['stop_loss'])
            target_1     = float(pos['target_1'])
            target_2     = float(pos['target_2'])
            partial_sold = pos.get('partial_sold', False)
            entry_time   = pos.get('entry_time')

            # Time stop: max 4 hours
            if entry_time:
                try:
                    entry_dt = datetime.fromisoformat(entry_time.replace('Z', '+00:00'))
                    held_hrs = (datetime.now(timezone.utc) - entry_dt).total_seconds() / 3600
                    if held_hrs >= 4.0:
                        _close_position(pos, current_price, 'time_stop')
                        continue
                except Exception:
                    pass

            # Stop loss
            if current_price <= stop_loss:
                _close_position(pos, current_price, 'stop_loss')
                continue

            # Target 2 — full close
            if current_price >= target_2:
                _close_position(pos, current_price, 'target_2')
                continue

            # Target 1 — partial sell (only once)
            if current_price >= target_1 and not partial_sold:
                _sell_partial(pos, current_price)

        except Exception as e:
            logger.warning(f'crypto_executor: _check_positions error for {ticker}: {e}')


def _reload_open_positions():
    """On startup, reload any open positions from Supabase into memory."""
    rows = _supa_get('crypto_trades', 'status=eq.open&order=entry_time.desc&limit=50')
    with _positions_lock:
        for row in rows:
            oid = row.get('order_id')
            if oid:
                _open_positions[oid] = row
    if rows:
        logger.info(f'crypto_executor: reloaded {len(rows)} open positions from Supabase')


def _update_daily_state():
    """Refresh today's trade count and P&L from Supabase."""
    today = datetime.now(_ET).strftime('%Y-%m-%d')
    rows  = _supa_get('crypto_trades', f'date=eq.{today}&status=eq.closed&select=pnl&limit=100')
    pnls  = [float(r.get('pnl') or 0) for r in rows]
    open_today = _supa_get('crypto_trades', f'date=eq.{today}&status=eq.open&select=order_id&limit=100')

    with _state_lock:
        _state['trades_today'] = len(rows) + len(open_today)
        _state['pnl_today']    = round(sum(pnls), 4)


def _monitor_loop():
    """Background loop: check positions every 60s, refresh daily stats every 5 min."""
    with _state_lock:
        _state['running'] = True

    _reload_open_positions()
    logger.info('crypto_executor: monitor loop started (24/7)')

    tick = 0
    while True:
        try:
            _check_positions()
            if tick % 5 == 0:
                _update_daily_state()
            tick += 1
        except Exception as e:
            logger.error(f'crypto_executor: monitor loop error: {e}')
        time.sleep(60)


# ── Circuit breaker ───────────────────────────────────────────────────────────

def trip_circuit(reason: str):
    with _state_lock:
        _state['circuit_open']   = True
        _state['circuit_reason'] = reason
    logger.warning(f'crypto_executor: circuit breaker tripped: {reason}')
    _push('⚠️ Crypto Circuit Breaker', reason)


def reset_circuit():
    with _state_lock:
        _state['circuit_open']   = False
        _state['circuit_reason'] = ''
    logger.info('crypto_executor: circuit breaker reset')


def force_close_all() -> dict:
    """Emergency close all open crypto positions."""
    with _positions_lock:
        positions_snapshot = dict(_open_positions)

    closed = []
    errors = []

    for order_id, pos in positions_snapshot.items():
        ticker = pos['ticker']
        try:
            price = _get_latest_price(ticker) or float(pos['entry_price'])
            _close_position(pos, price, 'force_close')
            closed.append(ticker)
        except Exception as e:
            errors.append({'ticker': ticker, 'error': str(e)})

    logger.info(f'crypto_executor: force_close_all: closed={closed} errors={errors}')
    return {'closed': closed, 'errors': errors, 'total': len(closed)}


# ── Status / public API ───────────────────────────────────────────────────────

def get_status() -> dict:
    with _state_lock:
        return dict(_state)


def get_trades_today() -> list:
    today = datetime.now(_ET).strftime('%Y-%m-%d')
    return _supa_get(
        'crypto_trades',
        f'date=eq.{today}&order=entry_time.desc&limit=100',
    )


def start():
    """Launch background monitor thread at app startup."""
    global _monitor_thread
    if _monitor_thread and _monitor_thread.is_alive():
        logger.info('crypto_executor: monitor already running')
        return
    _monitor_thread = threading.Thread(
        target=_monitor_loop,
        daemon=True,
        name='crypto-monitor',
    )
    _monitor_thread.start()
    logger.info('crypto_executor: monitor thread launched')
