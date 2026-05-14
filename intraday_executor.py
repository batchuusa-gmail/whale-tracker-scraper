"""
Intraday Trade Executor — Technical Pattern Edition
Places and monitors paper trades on Alpaca.

Rules:
  - Stop loss:  1.5× ATR(14) from entry
  - Target 1:   1× ATR → sell 50% of position
  - Target 2:   2× ATR → sell remainder
  - Max 3 simultaneous open positions
  - Extended hours orders (extended_hours=True, time_in_force='day')
  - No forced EOD close — hold overnight if stop/target not hit
  - No circuit breaker

Kill switch: INTRADAY_TRADING_ENABLED=true/false (Railway env var)
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

PAPER_BASE    = 'https://paper-api.alpaca.markets'
ALPACA_DATA   = 'https://data.alpaca.markets'
MAX_POSITIONS = 25
MAX_TRADE_USD = 15_000    # hard cap per trade
MIN_HOLD_MINS = 3         # don't trigger stop/target in first 3 min
MONITOR_SECS  = 30

_ET = pytz.timezone('America/New_York')

# Per-ticker partial-sell state: {ticker: bool}
_partial_sold: dict[str, bool] = {}
_state_lock = threading.Lock()


# ── Helpers ──────────────────────────────────────────────────────

def _enabled() -> bool:
    return os.environ.get('INTRADAY_TRADING_ENABLED', 'false').lower() == 'true'


def _headers() -> dict:
    return {
        'APCA-API-KEY-ID':     os.environ.get('ALPACA_KEY', ''),
        'APCA-API-SECRET-KEY': os.environ.get('ALPACA_SECRET', ''),
        'Content-Type': 'application/json',
    }


def _alpaca(method: str, path: str, **kwargs):
    """Make an Alpaca paper API call. Returns (status_code, data)."""
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


def _latest_price(ticker: str) -> float | None:
    """Fetch latest trade price via Alpaca IEX feed."""
    key    = os.environ.get('ALPACA_KEY', '')
    secret = os.environ.get('ALPACA_SECRET', '')
    if not key or not secret:
        return None
    try:
        r = requests.get(
            f'{ALPACA_DATA}/v2/stocks/{ticker}/trades/latest',
            headers={'APCA-API-KEY-ID': key, 'APCA-API-SECRET-KEY': secret},
            params={'feed': 'iex'},
            timeout=8,
        )
        if r.status_code == 200:
            return float(r.json().get('trade', {}).get('p', 0) or 0) or None
    except Exception as e:
        logger.warning(f'_latest_price({ticker}): {e}')
    return None


# ── FCM push ─────────────────────────────────────────────────────

_SUPA_URL = os.environ.get('SUPABASE_URL', 'https://bedurjtazsfbnkisoeee.supabase.co')
_SUPA_KEY = os.environ.get(
    'SUPABASE_KEY',
    'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJlZHVyanRhenNmYm5raXNvZWVlIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzM2NDcxOTUsImV4cCI6MjA4OTIyMzE5NX0.MK4N9dxAIHlXkGP4rLJPq5tHh9UU8L75EB1b8Q7CVmg'
)
_SUPA_HDRS = {
    'apikey':        _SUPA_KEY,
    'Authorization': f'Bearer {_SUPA_KEY}',
    'Content-Type':  'application/json',
    'Prefer':        'resolution=merge-duplicates,return=minimal',
}


def _send_fcm_to_all(title: str, body: str, data: dict | None = None):
    """Send FCM push notification to all registered device tokens."""
    try:
        from firebase_push import send_to_tokens
        r = requests.get(
            f'{_SUPA_URL}/rest/v1/user_devices?select=fcm_token',
            headers={
                'apikey':        _SUPA_KEY,
                'Authorization': f'Bearer {_SUPA_KEY}',
            },
            timeout=10,
        )
        tokens = [
            row.get('fcm_token') for row in (r.json() if r.status_code == 200 else [])
            if row.get('fcm_token')
        ]
        if not tokens:
            return
        sent = send_to_tokens(tokens, title, body, data or {})
        logger.info(f'FCM sent to {sent}/{len(tokens)} devices: {title}')
    except Exception as e:
        logger.warning(f'FCM send error: {e}')


# ── Supabase helpers ─────────────────────────────────────────────

def _supa_upsert(table: str, rows: list):
    if not rows:
        return
    try:
        r = requests.post(
            f'{_SUPA_URL}/rest/v1/{table}',
            headers=_SUPA_HDRS, json=rows, timeout=15,
        )
        if r.status_code not in (200, 201, 204):
            logger.error(f'Supabase {table}: {r.status_code} {r.text[:200]}')
    except Exception as e:
        logger.error(f'Supabase {table} upsert: {e}')


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


# ── Market hours ─────────────────────────────────────────────────

def _is_market_open() -> bool:
    now = datetime.now(_ET)
    if now.weekday() >= 5:
        return False
    mo = now.replace(hour=9,  minute=30, second=0, microsecond=0)
    mc = now.replace(hour=16, minute=0,  second=0, microsecond=0)
    return mo <= now <= mc


def _is_active_window() -> bool:
    """Trading window: 7:00 AM – 3:45 PM ET."""
    now = datetime.now(_ET)
    if now.weekday() >= 5:
        return False
    start = now.replace(hour=7,  minute=0,  second=0, microsecond=0)
    end   = now.replace(hour=15, minute=45, second=0, microsecond=0)
    return start <= now <= end


# ── Position sizing ──────────────────────────────────────────────

def _calc_qty(entry: float, position_size_pct: float) -> tuple[int, str]:
    """
    Calculate share quantity using non_marginable_buying_power × position_size_pct.
    Caps at MAX_TRADE_USD per trade.
    Returns (qty, error_message_or_empty).
    """
    if entry <= 0:
        return 0, 'entry price is 0'

    _, account = _alpaca('get', '/v2/account')
    if not account:
        return 0, 'could not fetch account'

    # PDT guard
    is_pdt = account.get('pattern_day_trader', False)
    dt_bp  = float(account.get('daytrading_buying_power') or 0)
    if is_pdt and dt_bp == 0:
        return 0, 'PDT daytrading_buying_power=0 — reset paper account'

    buying_power = float(account.get('non_marginable_buying_power') or
                         account.get('buying_power') or 0)
    if buying_power < entry:
        return 0, f'insufficient buying_power ${buying_power:.2f}'

    alloc = min(buying_power * position_size_pct, MAX_TRADE_USD)
    qty   = max(1, int(alloc / entry))
    return qty, ''


# ── Order placement ──────────────────────────────────────────────

def place_intraday_order(signal: dict) -> dict:
    """
    Place a market order on Alpaca paper account.

    Required signal fields: ticker, signal ('BUY'/'SELL'), entry,
    stop_loss, target_1, target_2, confidence, reasoning

    Optional: position_size_pct (default 0.10), qty_override, manual
    """
    if not _enabled():
        return {'status': 'disabled', 'message': 'INTRADAY_TRADING_ENABLED is not true'}

    ticker     = (signal.get('ticker') or '').upper()
    sig_str    = (signal.get('signal') or '').upper()
    confidence = float(signal.get('confidence') or 0)
    entry      = float(signal.get('entry') or 0)
    stop_loss  = float(signal.get('stop_loss') or 0)
    target_1   = float(signal.get('target_1') or signal.get('target') or 0)
    target_2   = float(signal.get('target_2') or 0)
    reasoning  = str(signal.get('reasoning') or '')
    is_manual  = bool(signal.get('manual'))
    pos_pct    = float(signal.get('position_size_pct') or 0.10)

    if sig_str not in ('BUY', 'SELL'):
        return {'status': 'skipped', 'message': f'Signal {sig_str!r} not actionable'}

    # Check open positions
    _, positions = _alpaca('get', '/v2/positions')
    open_count   = len(positions) if isinstance(positions, list) else 0

    # Read max_positions from Supabase (or use default)
    effective_max = MAX_POSITIONS
    try:
        rows = _supa_get('algo_settings', 'limit=1')
        if rows and rows[0].get('max_positions'):
            effective_max = int(rows[0]['max_positions'])
    except Exception:
        pass

    if not is_manual and open_count >= effective_max:
        return {'status': 'skipped', 'message': f'Max {effective_max} positions open'}

    # Prevent duplicate position in same ticker
    if isinstance(positions, list) and not is_manual:
        if any(p.get('symbol') == ticker for p in positions):
            return {'status': 'skipped', 'message': f'Already holding {ticker}'}

    # Quantity
    qty_override = signal.get('qty_override')
    if qty_override and int(qty_override) > 0:
        qty = int(qty_override)
        qty_err = ''
    else:
        qty, qty_err = _calc_qty(entry, pos_pct)

    if qty_err:
        logger.warning(f'place_order qty error for {ticker}: {qty_err}')
        return {'status': 'skipped', 'message': qty_err}
    if qty <= 0:
        return {'status': 'skipped', 'message': 'qty calculated as 0'}

    # Determine side
    side = 'buy' if sig_str == 'BUY' else 'sell'

    # Place order with extended hours support
    status_code, order = _alpaca('post', '/v2/orders', json={
        'symbol':          ticker,
        'qty':             str(qty),
        'side':            side,
        'type':            'market',
        'time_in_force':   'day',
        'extended_hours':  True,
    })

    order_id   = order.get('id', '')
    order_stat = order.get('status', 'unknown')
    now_iso    = datetime.now(timezone.utc).isoformat()

    if status_code in (200, 201):
        logger.info(f'Order placed: {side.upper()} {qty} {ticker} @ ~${entry:.2f}')
        with _state_lock:
            _partial_sold[ticker] = False
        _send_fcm_to_all(
            title=f'{side.upper()} {ticker}',
            body=f'Entry ${entry:.2f} | Stop ${stop_loss:.2f} | Conf {confidence:.0%}',
            data={'ticker': ticker, 'signal': side.upper(), 'confidence': str(confidence)},
        )
    else:
        logger.error(f'Order failed: {status_code} {order}')

    # Derive fallback targets if not provided
    if target_1 <= 0:
        target_1 = round(entry * 1.01, 2)
    if target_2 <= 0:
        target_2 = round(entry * 1.02, 2)

    row = {
        'order_id':     order_id,
        'ticker':       ticker,
        'side':         side,
        'qty':          qty,
        'entry_price':  round(entry, 2),
        'exit_price':   None,
        'stop_loss':    round(stop_loss, 2),
        'target_1':     round(target_1, 2),
        'target_2':     round(target_2, 2),
        'hold_minutes': 0,
        'confidence':   round(confidence, 3),
        'ai_reasoning': reasoning[:500],
        'status':       order_stat,
        'pnl':          None,
        'pnl_pct':      None,
        'exit_reason':  None,
        'entry_time':   now_iso,
        'exit_time':    None,
        'partial_sold': False,
        'date':         datetime.now(_ET).strftime('%Y-%m-%d'),
    }
    _supa_upsert('intraday_trades', [row])

    return {
        'status':      'placed' if status_code in (200, 201) else 'error',
        'order_id':    order_id,
        'ticker':      ticker,
        'side':        side,
        'qty':         qty,
        'entry':       round(entry, 2),
        'stop_loss':   round(stop_loss, 2),
        'target_1':    round(target_1, 2),
        'target_2':    round(target_2, 2),
        'http_status': status_code,
    }


# ── Position monitoring ──────────────────────────────────────────

def _check_positions():
    """Check each open position against stop/target rules every MONITOR_SECS."""
    status_code, positions = _alpaca('get', '/v2/positions')
    if not isinstance(positions, list):
        return

    trades = _supa_get('intraday_trades', 'status=neq.closed&order=entry_time.desc&limit=100')
    trade_map = {r.get('ticker', ''): r for r in trades}

    _CRYPTO_SUFFIXES = ('USD', 'USDT', 'USDC', 'BTC', 'ETH')

    for pos in positions:
        ticker = pos.get('symbol', '')
        if not ticker:
            continue
        # Skip crypto — handled by crypto_executor
        if any(ticker.endswith(s) for s in _CRYPTO_SUFFIXES) and len(ticker) > 4:
            continue

        cur_price   = float(pos.get('current_price') or 0)
        entry_price = float(pos.get('avg_entry_price') or 0)
        qty         = float(pos.get('qty') or 0)
        side        = pos.get('side', 'long')

        if cur_price <= 0 or entry_price <= 0:
            continue

        trade = trade_map.get(ticker, {})
        stop  = float(trade.get('stop_loss') or (
            entry_price * 0.985 if side == 'long' else entry_price * 1.015))
        t1    = float(trade.get('target_1') or entry_price * 1.01)
        t2    = float(trade.get('target_2') or entry_price * 1.02)

        # Minutes held
        mins_held = 0
        entry_time_str = trade.get('entry_time')
        if entry_time_str:
            try:
                et = datetime.fromisoformat(entry_time_str.replace('Z', '+00:00'))
                mins_held = (datetime.now(timezone.utc) - et).total_seconds() / 60
            except Exception:
                pass

        # Don't check exits in first MIN_HOLD_MINS
        if mins_held < MIN_HOLD_MINS:
            continue

        with _state_lock:
            already_partial = _partial_sold.get(ticker, False)

        exit_reason = None
        if side == 'long':
            if cur_price <= stop:
                exit_reason = 'stop_loss'
            elif cur_price >= t2:
                exit_reason = 'target_2'
            elif cur_price >= t1 and not already_partial:
                exit_reason = 'target_1_partial'
        elif side == 'short':
            if cur_price >= stop:
                exit_reason = 'stop_loss'
            elif cur_price <= t2:
                exit_reason = 'target_2'
            elif cur_price <= t1 and not already_partial:
                exit_reason = 'target_1_partial'

        if exit_reason == 'target_1_partial':
            _sell_partial(ticker, pos, trade, cur_price, qty)
        elif exit_reason:
            _close_position(ticker, pos, trade, exit_reason, cur_price)


def _sell_partial(ticker: str, pos: dict, trade: dict, cur_price: float, qty: float):
    """Sell 50% of position at Target 1."""
    partial_qty = max(1, int(qty / 2))
    side        = 'sell' if pos.get('side', 'long') == 'long' else 'buy'
    sc, order   = _alpaca('post', '/v2/orders', json={
        'symbol':         ticker,
        'qty':            str(partial_qty),
        'side':           side,
        'type':           'market',
        'time_in_force':  'day',
        'extended_hours': True,
    })
    if sc in (200, 201):
        logger.info(f'Partial sell: {partial_qty} {ticker} @ ~${cur_price:.2f} (target_1)')
        with _state_lock:
            _partial_sold[ticker] = True
        _supa_upsert('intraday_trades', [{
            'order_id':     trade.get('order_id', ''),
            'ticker':       ticker,
            'partial_sold': True,
            'status':       'partial',
        }])
    else:
        logger.error(f'Partial sell failed: {sc} {order}')


def _close_position(ticker: str, pos: dict, trade: dict, reason: str, exit_price: float):
    """Fully close a position via Alpaca."""
    sc, order = _alpaca('delete', f'/v2/positions/{ticker}')
    if sc in (200, 201, 204):
        entry    = float(pos.get('avg_entry_price') or 0)
        qty      = float(pos.get('qty') or 0)
        side     = pos.get('side', 'long')
        pnl      = (exit_price - entry) * qty if side == 'long' else (entry - exit_price) * qty
        pnl_pct  = (exit_price - entry) / entry if entry > 0 else 0

        hold_mins = 0
        entry_time_str = trade.get('entry_time')
        if entry_time_str:
            try:
                et = datetime.fromisoformat(entry_time_str.replace('Z', '+00:00'))
                hold_mins = int((datetime.now(timezone.utc) - et).total_seconds() / 60)
            except Exception:
                pass

        now = datetime.now(timezone.utc).isoformat()
        logger.info(
            f'Close: {ticker} | reason={reason} | '
            f'hold={hold_mins}m | pnl=${pnl:.2f} ({pnl_pct:.1%})'
        )
        with _state_lock:
            _partial_sold.pop(ticker, None)

        pnl_sign = '+' if pnl >= 0 else ''
        _send_fcm_to_all(
            title=f'SELL {ticker} — {pnl_sign}${pnl:.2f}',
            body=f'P&L: {pnl_sign}{pnl_pct:.1%} | {reason} | Held {hold_mins}m',
            data={'ticker': ticker, 'signal': 'SELL', 'pnl': str(round(pnl, 2))},
        )
        _supa_upsert('intraday_trades', [{
            'order_id':     trade.get('order_id', ''),
            'ticker':       ticker,
            'status':       'closed',
            'exit_price':   round(exit_price, 2),
            'exit_reason':  reason,
            'pnl':          round(pnl, 2),
            'pnl_pct':      round(pnl_pct, 4),
            'exit_time':    now,
            'hold_minutes': hold_mins,
        }])
    else:
        logger.error(f'Close failed: {ticker} {sc}')


def force_close_all() -> dict:
    """Emergency: close ALL open intraday positions immediately."""
    logger.warning('Intraday executor: FORCE CLOSING ALL POSITIONS')
    _, positions = _alpaca('get', '/v2/positions')
    closed = []
    errors = []

    trades    = _supa_get('intraday_trades', 'status=neq.closed&order=entry_time.desc&limit=100')
    trade_map = {r.get('ticker', ''): r for r in trades}

    if isinstance(positions, list):
        for pos in positions:
            ticker = pos.get('symbol', '')
            if not ticker:
                continue
            sc, _ = _alpaca('delete', f'/v2/positions/{ticker}')
            if sc in (200, 201, 204):
                closed.append(ticker)
                with _state_lock:
                    _partial_sold.pop(ticker, None)
                trade   = trade_map.get(ticker, {})
                entry   = float(pos.get('avg_entry_price') or 0)
                cur     = float(pos.get('current_price') or 0)
                qty     = float(pos.get('qty') or 0)
                side    = pos.get('side', 'long')
                pnl     = (cur - entry) * qty if side == 'long' else (entry - cur) * qty
                pnl_pct = (cur - entry) / entry if entry > 0 else 0
                hold_mins = 0
                entry_time_str = trade.get('entry_time')
                if entry_time_str:
                    try:
                        et = datetime.fromisoformat(entry_time_str.replace('Z', '+00:00'))
                        hold_mins = int((datetime.now(timezone.utc) - et).total_seconds() / 60)
                    except Exception:
                        pass
                _supa_upsert('intraday_trades', [{
                    'order_id':     trade.get('order_id', ''),
                    'ticker':       ticker,
                    'status':       'closed',
                    'exit_price':   round(cur, 2),
                    'exit_reason':  'force_close',
                    'pnl':          round(pnl, 2),
                    'pnl_pct':      round(pnl_pct, 4),
                    'exit_time':    datetime.now(timezone.utc).isoformat(),
                    'hold_minutes': hold_mins,
                }])
            else:
                errors.append(ticker)

    # Alpaca's bulk close as safety net
    _alpaca('delete', '/v2/positions')
    logger.info(f'Force close: closed={closed} errors={errors}')
    return {'closed': closed, 'errors': errors, 'total': len(closed)}


# ── Monitor loop ─────────────────────────────────────────────────

def _monitor_loop():
    """Background thread: checks positions every MONITOR_SECS seconds."""
    logger.info('Intraday executor: monitor thread started')
    while True:
        try:
            if _enabled():
                _check_positions()
        except Exception as e:
            logger.error(f'Intraday monitor error: {e}')
        time.sleep(MONITOR_SECS)


def start():
    """Start the background position monitor thread. Call once at app startup."""
    t = threading.Thread(target=_monitor_loop, daemon=True, name='intraday-monitor')
    t.start()
    logger.info('Intraday executor: monitor thread launched')


# ── Public read APIs (called by Flask routes in main.py) ─────────

def get_open_positions() -> list:
    """Return current open intraday positions from Alpaca + Supabase metadata."""
    _, positions = _alpaca('get', '/v2/positions')
    if not isinstance(positions, list):
        return []

    today     = datetime.now(_ET).strftime('%Y-%m-%d')
    trades    = _supa_get('intraday_trades', f'date=eq.{today}&status=neq.closed')
    trade_map = {r.get('ticker', ''): r for r in trades}

    _CRYPTO_SUFFIXES = ('USD', 'USDT', 'USDC', 'BTC', 'ETH')
    result = []
    for p in positions:
        ticker = p.get('symbol', '')
        if any(ticker.endswith(s) for s in _CRYPTO_SUFFIXES) and len(ticker) > 4:
            continue

        entry   = float(p.get('avg_entry_price') or 0)
        cur     = float(p.get('current_price') or 0)
        qty     = float(p.get('qty') or 0)
        pnl     = float(p.get('unrealized_pl') or 0)
        pnl_pct = (cur - entry) / entry if entry > 0 else 0
        trade   = trade_map.get(ticker, {})

        mins_held = 0
        entry_time_str = trade.get('entry_time')
        if entry_time_str:
            try:
                et = datetime.fromisoformat(entry_time_str.replace('Z', '+00:00'))
                mins_held = int((datetime.now(timezone.utc) - et).total_seconds() / 60)
            except Exception:
                pass

        result.append({
            'ticker':         ticker,
            'qty':            qty,
            'side':           p.get('side', 'long'),
            'entry_price':    round(entry, 2),
            'current_price':  round(cur, 2),
            'market_value':   round(float(p.get('market_value') or 0), 2),
            'unrealized_pnl': round(pnl, 2),
            'pnl_pct':        round(pnl_pct * 100, 2),
            'stop_loss':      trade.get('stop_loss'),
            'target_1':       trade.get('target_1'),
            'target_2':       trade.get('target_2'),
            'mins_held':      mins_held,
            'partial_sold':   trade.get('partial_sold', False),
            'ai_reasoning':   trade.get('ai_reasoning', ''),
            'confidence':     trade.get('confidence'),
        })
    return result


def get_trades_today() -> list:
    """Return all intraday trades placed today from Supabase."""
    today = datetime.now(_ET).strftime('%Y-%m-%d')
    rows  = _supa_get('intraday_trades', f'date=eq.{today}&order=entry_time.desc')
    return rows if isinstance(rows, list) else []
