"""
WHAL-96 — Intraday Trade Executor
Places and monitors paper trades on Alpaca with tight intraday stop/target rules.
ALL positions force-closed at 3:45 PM ET — never hold overnight.

Kill switch: INTRADAY_TRADING_ENABLED=true/false (Railway env var)

Trade Rules:
  - Max 5 simultaneous open positions
  - Max 10% of paper portfolio per trade
  - Stop loss:  -1.5% from entry
  - Target 1:  +2.5% → sell 50% (partial, or AI-computed)
  - Target 2:  +4.0% → sell remainder (or AI-computed)
  - Time stop: close if held > 120 minutes
  - Hard close: ALL positions closed at 3:45 PM ET
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

PAPER_BASE      = 'https://paper-api.alpaca.markets'
ALPACA_DATA     = 'https://data.alpaca.markets'
STOP_LOSS_PCT   = 0.015   # -1.5%
TARGET_1_PCT    = 0.025   # +2.5% → sell 50%  (fallback if AI omits)
TARGET_2_PCT    = 0.040   # +4.0% → sell remainder (fallback if AI omits)
MAX_HOLD_MINS   = 120
MIN_HOLD_MINS   = 3       # don't check stop/target for first 3 minutes
MAX_POSITIONS   = 5
MAX_ALLOC_PCT   = 0.10    # 10% of portfolio per trade
MONITOR_SECS    = 30
FORCE_CLOSE_H   = 15      # 3 PM ET
FORCE_CLOSE_M   = 45      # :45

_ET = pytz.timezone('America/New_York')

# ── State ────────────────────────────────────────────────────────

# Tracks partial-sell status per ticker: {ticker: bool}
_partial_sold: dict[str, bool] = {}
_state_lock = threading.Lock()


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
    """Fetch latest trade price from Alpaca data API."""
    key    = os.environ.get('ALPACA_KEY', '')
    secret = os.environ.get('ALPACA_SECRET', '')
    if not key or not secret:
        return None
    try:
        r = requests.get(
            f'{ALPACA_DATA}/v2/stocks/{ticker}/trades/latest',
            headers={'APCA-API-KEY-ID': key, 'APCA-API-SECRET-KEY': secret},
            timeout=8,
        )
        if r.status_code == 200:
            return float(r.json().get('trade', {}).get('p', 0) or 0) or None
    except Exception as e:
        logger.warning(f'_latest_price({ticker}): {e}')
    return None


# ── Supabase helpers ─────────────────────────────────────────────

_SUPA_URL = os.environ.get('SUPABASE_URL', 'https://bedurjtazsfbnkisoeee.supabase.co')
_SUPA_KEY = os.environ.get(
    'SUPABASE_KEY',
    'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJlZHVyanRhenNmYm5raXNvZWVlIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzM2NDcxOTUsImV4cCI6MjA4OTIyMzE5NX0.MK4N9dxAIHlXkGP4rLJPq5tHh9UU8L75EB1b8Q7CVmg'
)
_SUPA_HDRS = {
    'apikey': _SUPA_KEY,
    'Authorization': f'Bearer {_SUPA_KEY}',
    'Content-Type': 'application/json',
    'Prefer': 'resolution=merge-duplicates,return=minimal',
}


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


# ── Market hours helpers ─────────────────────────────────────────

def _is_market_open() -> bool:
    now = datetime.now(_ET)
    if now.weekday() >= 5:
        return False
    mo = now.replace(hour=9,  minute=30, second=0, microsecond=0)
    mc = now.replace(hour=16, minute=0,  second=0, microsecond=0)
    return mo <= now <= mc


def _is_force_close_time() -> bool:
    now = datetime.now(_ET)
    fc  = now.replace(hour=FORCE_CLOSE_H, minute=FORCE_CLOSE_M, second=0, microsecond=0)
    return now >= fc and now.weekday() < 5


# ── Core trade execution ─────────────────────────────────────────

def place_intraday_order(signal: dict) -> dict:
    """
    Place an intraday market order on Alpaca paper account.

    signal must have: ticker, signal ('BUY'/'SELL'), entry, stop_loss,
                      target, hold_minutes, confidence, reasoning
    Returns dict with status, order details.
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
    hold_mins  = int(signal.get('hold_minutes') or 60)
    reasoning  = str(signal.get('reasoning') or '')

    if sig_str not in ('BUY', 'SELL'):
        return {'status': 'skipped', 'message': f'Signal {sig_str} is not actionable'}

    if confidence < 0.70:
        return {'status': 'skipped', 'message': f'Confidence {confidence:.0%} below 70% threshold'}

    # Check position limits
    _, positions = _alpaca('get', '/v2/positions')
    open_count   = len(positions) if isinstance(positions, list) else 0
    if open_count >= MAX_POSITIONS:
        return {'status': 'skipped', 'message': f'Max {MAX_POSITIONS} open positions reached'}

    # Check if already in this ticker
    if isinstance(positions, list):
        existing = [p for p in positions if p.get('symbol') == ticker]
        if existing:
            return {'status': 'skipped', 'message': f'Already holding {ticker}'}

    # Calculate quantity: 10% of portfolio
    _, account = _alpaca('get', '/v2/account')
    equity     = float(account.get('equity') or 100_000)
    alloc      = equity * MAX_ALLOC_PCT
    qty        = max(1, int(alloc / entry)) if entry > 0 else 1

    # Place market order
    side = 'buy' if sig_str == 'BUY' else 'sell'
    status_code, order = _alpaca('post', '/v2/orders', json={
        'symbol':        ticker,
        'qty':           str(qty),
        'side':          side,
        'type':          'market',
        'time_in_force': 'day',
    })

    order_id    = order.get('id', '')
    order_stat  = order.get('status', 'unknown')
    now_iso     = datetime.now(timezone.utc).isoformat()

    if status_code in (200, 201):
        logger.info(f'Intraday order placed: {side.upper()} {qty} {ticker} @ ~${entry:.2f}')
        with _state_lock:
            _partial_sold[ticker] = False
    else:
        logger.error(f'Intraday order failed: {status_code} {order}')

    row = {
        'order_id':      order_id,
        'ticker':        ticker,
        'side':          side,
        'qty':           qty,
        'entry_price':   round(entry, 2),
        'exit_price':    None,
        'stop_loss':     round(stop_loss, 2),
        'target_1':      round(target_1 if target_1 > 0 else entry * (1 + TARGET_1_PCT), 2),
        'target_2':      round(target_2 if target_2 > 0 else entry * (1 + TARGET_2_PCT), 2),
        'hold_minutes':  hold_mins,
        'confidence':    round(confidence, 3),
        'ai_reasoning':  reasoning[:500],
        'status':        order_stat,
        'pnl':           None,
        'pnl_pct':       None,
        'exit_reason':   None,
        'entry_time':    now_iso,
        'exit_time':     None,
        'partial_sold':  False,
        'date':          datetime.now(_ET).strftime('%Y-%m-%d'),
    }
    _supa_upsert('intraday_trades', [row])

    return {
        'status':       'placed' if status_code in (200, 201) else 'error',
        'order_id':     order_id,
        'ticker':       ticker,
        'side':         side,
        'qty':          qty,
        'entry':        round(entry, 2),
        'stop_loss':    round(stop_loss, 2),
        'target_1':     round(target_1 if target_1 > 0 else entry * (1 + TARGET_1_PCT), 2),
        'target_2':     round(target_2 if target_2 > 0 else entry * (1 + TARGET_2_PCT), 2),
        'http_status':  status_code,
    }


# ── Position monitor ─────────────────────────────────────────────

def _check_positions():
    """Check each open position against stop/target/time rules."""
    status_code, positions = _alpaca('get', '/v2/positions')
    if not isinstance(positions, list):
        return

    # Load trade records for stop/target/entry_time
    today = datetime.now(_ET).strftime('%Y-%m-%d')
    trades = _supa_get('intraday_trades', f'date=eq.{today}&status=neq.closed')
    trade_map = {r.get('ticker', ''): r for r in trades}

    for pos in positions:
        ticker      = pos.get('symbol', '')
        cur_price   = float(pos.get('current_price') or 0)
        entry_price = float(pos.get('avg_entry_price') or 0)
        qty         = float(pos.get('qty') or 0)
        side        = pos.get('side', 'long')

        if cur_price <= 0 or entry_price <= 0:
            continue

        trade = trade_map.get(ticker, {})
        stop  = float(trade.get('stop_loss') or (entry_price * (1 - STOP_LOSS_PCT)))
        t2    = float(trade.get('target_2')  or (entry_price * (1 + TARGET_2_PCT)))
        t1    = float(trade.get('target_1')  or (entry_price * (1 + TARGET_1_PCT)))

        # Minutes held
        entry_time_str = trade.get('entry_time')
        mins_held      = 0
        if entry_time_str:
            try:
                et  = datetime.fromisoformat(entry_time_str.replace('Z', '+00:00'))
                mins_held = (datetime.now(timezone.utc) - et).total_seconds() / 60
            except Exception:
                pass

        pnl_pct = (cur_price - entry_price) / entry_price if entry_price > 0 else 0

        # Skip stop/target checks for the first MIN_HOLD_MINS minutes
        if mins_held < MIN_HOLD_MINS:
            continue

        with _state_lock:
            already_partial = _partial_sold.get(ticker, False)

        # Determine exit action
        exit_reason = None
        if side == 'long':
            if cur_price <= stop:
                exit_reason = 'stop_loss'
            elif cur_price >= t2:
                exit_reason = 'target_2'
            elif cur_price >= t1 and not already_partial:
                exit_reason = 'target_1_partial'
            elif mins_held >= MAX_HOLD_MINS:
                exit_reason = 'time_stop'
        elif side == 'short':
            if cur_price >= stop:
                exit_reason = 'stop_loss'
            elif cur_price <= t2:
                exit_reason = 'target_2'
            elif mins_held >= MAX_HOLD_MINS:
                exit_reason = 'time_stop'

        if exit_reason == 'target_1_partial':
            _sell_partial(ticker, pos, trade, cur_price, qty)
        elif exit_reason:
            _close_position(ticker, pos, trade, exit_reason, cur_price)


def _sell_partial(ticker: str, pos: dict, trade: dict, cur_price: float, qty: float):
    """Sell 50% of position at Target 1."""
    partial_qty = max(1, int(qty / 2))
    side        = 'sell' if pos.get('side', 'long') == 'long' else 'buy'
    status_code, order = _alpaca('post', '/v2/orders', json={
        'symbol':        ticker,
        'qty':           str(partial_qty),
        'side':          side,
        'type':          'market',
        'time_in_force': 'day',
    })
    if status_code in (200, 201):
        logger.info(f'Intraday partial sell: {partial_qty} {ticker} @ ~${cur_price:.2f} (target_1)')
        with _state_lock:
            _partial_sold[ticker] = True
        # Update Supabase
        order_id = trade.get('order_id', '')
        _supa_upsert('intraday_trades', [{
            'order_id':     order_id,
            'ticker':       ticker,
            'partial_sold': True,
            'status':       'partial',
        }])
    else:
        logger.error(f'Intraday partial sell failed: {status_code}')


def _close_position(ticker: str, pos: dict, trade: dict, reason: str, exit_price: float):
    """Fully close a position."""
    status_code, order = _alpaca('delete', f'/v2/positions/{ticker}')
    if status_code in (200, 201, 204):
        entry  = float(pos.get('avg_entry_price') or 0)
        qty    = float(pos.get('qty') or 0)
        side   = pos.get('side', 'long')
        pnl    = (exit_price - entry) * qty if side == 'long' else (entry - exit_price) * qty
        pnl_pct = (exit_price - entry) / entry if entry > 0 else 0
        now    = datetime.now(timezone.utc).isoformat()
        # Compute hold_minutes
        hold_mins = 0
        entry_time_str = trade.get('entry_time')
        if entry_time_str:
            try:
                et = datetime.fromisoformat(entry_time_str.replace('Z', '+00:00'))
                hold_mins = int((datetime.now(timezone.utc) - et).total_seconds() / 60)
            except Exception:
                pass
        logger.info(f'Intraday close: {ticker} | reason={reason} | hold={hold_mins}m | pnl=${pnl:.2f} ({pnl_pct:.1%})')
        with _state_lock:
            _partial_sold.pop(ticker, None)
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
        logger.error(f'Intraday close failed: {ticker} {status_code}')


def force_close_all() -> dict:
    """Emergency: close ALL open intraday positions immediately."""
    logger.warning('Intraday executor: FORCE CLOSING ALL POSITIONS')
    status_code, positions = _alpaca('get', '/v2/positions')
    closed = []
    errors = []

    today = datetime.now(_ET).strftime('%Y-%m-%d')
    trades = _supa_get('intraday_trades', f'date=eq.{today}&status=neq.closed')
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
                # Update Supabase with closed status + hold_minutes
                trade = trade_map.get(ticker, {})
                entry  = float(pos.get('avg_entry_price') or 0)
                cur    = float(pos.get('current_price') or 0)
                qty    = float(pos.get('qty') or 0)
                side   = pos.get('side', 'long')
                pnl    = (cur - entry) * qty if side == 'long' else (entry - cur) * qty
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

    # Also try Alpaca's close-all endpoint as a safety net
    _alpaca('delete', '/v2/positions')

    _log_event('force_close_all', {'closed': closed, 'errors': errors})
    logger.info(f'Force close complete: {closed}')
    return {'closed': closed, 'errors': errors, 'total': len(closed)}


def _log_event(event: str, data: dict = None):
    """Log a system event to Supabase."""
    _supa_upsert('intraday_events', [{
        'event':      event,
        'data':       json.dumps(data or {}),
        'created_at': datetime.now(timezone.utc).isoformat(),
    }])


# ── Monitor loop ─────────────────────────────────────────────────

_force_closed_today = False
_force_close_date   = ''


def _monitor_loop():
    global _force_closed_today, _force_close_date
    logger.info('Intraday executor: monitor thread started')

    while True:
        try:
            today = datetime.now(_ET).strftime('%Y-%m-%d')

            # Reset daily force-close flag on new day
            if today != _force_close_date:
                _force_close_date   = today
                _force_closed_today = False

            if _enabled() and _is_market_open():
                # Force close at 3:45 PM ET
                if _is_force_close_time() and not _force_closed_today:
                    force_close_all()
                    _log_event('force_close_3_45pm', {'date': today})
                    _force_closed_today = True
                elif not _is_force_close_time():
                    _check_positions()
            else:
                pass  # market closed or disabled
        except Exception as e:
            logger.error(f'Intraday monitor error: {e}')

        time.sleep(MONITOR_SECS)


def start():
    """Start the background position monitor thread. Call once at app startup."""
    t = threading.Thread(target=_monitor_loop, daemon=True, name='intraday-monitor')
    t.start()
    logger.info('Intraday executor: monitor thread launched')


# ── Public read APIs (used by Flask endpoints) ───────────────────

def get_open_positions() -> list:
    """Return current open intraday positions from Alpaca."""
    _, positions = _alpaca('get', '/v2/positions')
    if not isinstance(positions, list):
        return []

    today = datetime.now(_ET).strftime('%Y-%m-%d')
    trades = _supa_get('intraday_trades', f'date=eq.{today}&status=neq.closed')
    trade_map = {r.get('ticker', ''): r for r in trades}

    result = []
    for p in positions:
        ticker      = p.get('symbol', '')
        entry       = float(p.get('avg_entry_price') or 0)
        cur         = float(p.get('current_price') or 0)
        qty         = float(p.get('qty') or 0)
        pnl         = float(p.get('unrealized_pl') or 0)
        pnl_pct     = (cur - entry) / entry if entry > 0 else 0
        trade       = trade_map.get(ticker, {})

        entry_time_str = trade.get('entry_time')
        mins_held      = 0
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
