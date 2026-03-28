"""
WHAL-78 — Paper Trading Bot
Auto-executes trades on Alpaca paper account when AI signals pass risk rules.
Polls open positions every 5 minutes for stop loss / target exits.
"""
import os
import json
import logging
import threading
import time
from datetime import datetime, timezone

import requests

logger = logging.getLogger(__name__)

PAPER_BASE   = 'https://paper-api.alpaca.markets'
STOP_LOSS_PCT = 0.10   # -10%
TARGET_PCT    = 0.20   # +20%
POLL_INTERVAL = 300    # 5 minutes


def _enabled():
    return os.environ.get('PAPER_TRADING_ENABLED', 'false').lower() == 'true'


def _headers():
    return {
        'APCA-API-KEY-ID':     os.environ.get('ALPACA_KEY', ''),
        'APCA-API-SECRET-KEY': os.environ.get('ALPACA_SECRET', ''),
        'Content-Type': 'application/json',
    }


def _alpaca(method, path, **kwargs):
    """Make an Alpaca paper trading API call. Returns (status_code, data)."""
    url = f'{PAPER_BASE}{path}'
    try:
        r = getattr(requests, method)(url, headers=_headers(), timeout=15, **kwargs)
        try:
            data = r.json()
        except Exception:
            data = {}
        return r.status_code, data
    except Exception as e:
        logger.error(f'Alpaca API error {method.upper()} {path}: {e}')
        return 0, {}


# ── Supabase helpers (self-contained to avoid circular import) ────

_SUPA_URL = os.environ.get('SUPABASE_URL', 'https://bedurjtazsfbnkisoeee.supabase.co')
_SUPA_KEY = os.environ.get(
    'SUPABASE_KEY',
    'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJlZHVyanRhenNmYm5raXNvZWVlIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzM2NDcxOTUsImV4cCI6MjA4OTIyMzE5NX0.MK4N9dxAIHlXkGP4rLJPq5tHh9UU8L75EB1b8Q7CVmg'
)
_SUPA_HEADERS = {
    'apikey': _SUPA_KEY,
    'Authorization': f'Bearer {_SUPA_KEY}',
    'Content-Type': 'application/json',
    'Prefer': 'resolution=merge-duplicates,return=minimal',
}


def _supa_upsert(table, rows):
    if not rows:
        return
    try:
        r = requests.post(
            f'{_SUPA_URL}/rest/v1/{table}',
            headers=_SUPA_HEADERS,
            json=rows,
            timeout=15,
        )
        if r.status_code not in (200, 201, 204):
            logger.error(f'Supabase {table} upsert error: {r.status_code} {r.text[:200]}')
    except Exception as e:
        logger.error(f'Supabase {table} upsert exception: {e}')


def _supa_get(table, query=''):
    try:
        r = requests.get(
            f'{_SUPA_URL}/rest/v1/{table}?{query}',
            headers=_SUPA_HEADERS,
            timeout=15,
        )
        if r.status_code == 200:
            return r.json()
    except Exception as e:
        logger.error(f'Supabase {table} get exception: {e}')
    return []


# ── Core trading functions ────────────────────────────────────────

def place_order(signal: dict) -> dict:
    """
    Place a market order on Alpaca paper account based on an AI signal.

    signal must contain:
      ticker, signal (BUY/STRONG BUY/SELL/STRONG SELL),
      current_price, position_size, max_shares,
      stop_loss, target_price, risk_passed
    """
    if not _enabled():
        return {'status': 'disabled', 'message': 'PAPER_TRADING_ENABLED is not true'}

    if not signal.get('risk_passed'):
        return {'status': 'skipped', 'message': 'Signal did not pass risk rules'}

    ticker     = (signal.get('ticker') or '').upper()
    sig_str    = (signal.get('signal') or '').upper()
    max_shares = int(signal.get('max_shares') or 0)
    entry      = float(signal.get('current_price') or 0)
    stop_loss  = float(signal.get('stop_loss') or 0)
    target     = float(signal.get('target_price') or 0)

    if not ticker or max_shares <= 0 or entry <= 0:
        return {'status': 'skipped', 'message': f'Invalid order params: ticker={ticker} shares={max_shares} price={entry}'}

    side = 'buy' if 'BUY' in sig_str else 'sell' if 'SELL' in sig_str else None
    if not side:
        return {'status': 'skipped', 'message': f'Signal {sig_str} is not actionable (NEUTRAL)'}

    # Place market order
    status_code, order = _alpaca('post', '/v2/orders', json={
        'symbol':        ticker,
        'qty':           str(max_shares),
        'side':          side,
        'type':          'market',
        'time_in_force': 'day',
    })

    order_id     = order.get('id', '')
    order_status = order.get('status', 'unknown')
    now          = datetime.now(timezone.utc).isoformat()

    if status_code in (200, 201):
        logger.info(f'Paper order placed: {side.upper()} {max_shares} {ticker} | order_id={order_id}')
    else:
        logger.error(f'Paper order failed: {status_code} {order}')

    row = {
        'order_id':       order_id,
        'ticker':         ticker,
        'side':           side,
        'qty':            max_shares,
        'entry_price':    entry,
        'stop_loss':      stop_loss,
        'target_price':   target,
        'signal':         sig_str,
        'confidence':     signal.get('confidence'),
        'owner_name':     signal.get('owner_name', ''),
        'owner_type':     signal.get('owner_type', ''),
        'status':         order_status,
        'alpaca_status':  order_status,
        'placed_at':      now,
        'closed_at':      None,
        'exit_price':     None,
        'pnl':            None,
        'exit_reason':    None,
    }
    _supa_upsert('paper_trades', [row])

    return {
        'status':   'placed' if status_code in (200, 201) else 'error',
        'order_id': order_id,
        'side':     side,
        'qty':      max_shares,
        'ticker':   ticker,
        'http_status': status_code,
        'alpaca_response': order,
    }


def get_portfolio() -> dict:
    """Return Alpaca paper account info + open positions."""
    _, account   = _alpaca('get', '/v2/account')
    _, positions = _alpaca('get', '/v2/positions')

    pos_list = []
    if isinstance(positions, list):
        for p in positions:
            entry = float(p.get('avg_entry_price') or 0)
            cur   = float(p.get('current_price') or 0)
            qty   = float(p.get('qty') or 0)
            pnl   = float(p.get('unrealized_pl') or 0)
            pnl_pct = ((cur - entry) / entry * 100) if entry > 0 else 0
            pos_list.append({
                'ticker':      p.get('symbol'),
                'qty':         qty,
                'side':        p.get('side'),
                'entry_price': entry,
                'current_price': cur,
                'market_value': float(p.get('market_value') or 0),
                'unrealized_pnl': pnl,
                'unrealized_pnl_pct': round(pnl_pct, 2),
            })

    return {
        'equity':          float(account.get('equity') or 0),
        'cash':            float(account.get('cash') or 0),
        'buying_power':    float(account.get('buying_power') or 0),
        'portfolio_value': float(account.get('portfolio_value') or 0),
        'today_pnl':       float(account.get('equity', 0) or 0) - float(account.get('last_equity', 0) or 0),
        'positions':       pos_list,
        'position_count':  len(pos_list),
        'paper_trading_enabled': _enabled(),
    }


def get_trades(limit: int = 50) -> list:
    """Return recent paper trades from Supabase."""
    rows = _supa_get('paper_trades', f'order=placed_at.desc&limit={limit}')
    return rows if isinstance(rows, list) else []


def get_performance() -> dict:
    """Compute performance metrics from closed paper trades."""
    rows = _supa_get('paper_trades', 'status=eq.closed&order=closed_at.desc&limit=200')
    if not isinstance(rows, list):
        rows = []

    closed = [r for r in rows if r.get('pnl') is not None]
    if not closed:
        return {
            'total_trades': 0,
            'win_rate': 0,
            'total_pnl': 0,
            'avg_pnl_per_trade': 0,
            'best_trade': None,
            'worst_trade': None,
            'avg_hold_days': 0,
        }

    pnls      = [float(r['pnl']) for r in closed]
    wins      = [p for p in pnls if p > 0]
    win_rate  = round(len(wins) / len(pnls) * 100, 1) if pnls else 0
    total_pnl = round(sum(pnls), 2)

    # Hold time in days
    hold_days_list = []
    for r in closed:
        try:
            placed = datetime.fromisoformat(r['placed_at'].replace('Z', '+00:00'))
            closed_at = datetime.fromisoformat(r['closed_at'].replace('Z', '+00:00'))
            hold_days_list.append((closed_at - placed).total_seconds() / 86400)
        except Exception:
            pass

    best  = closed[pnls.index(max(pnls))] if pnls else None
    worst = closed[pnls.index(min(pnls))] if pnls else None

    return {
        'total_trades':       len(closed),
        'win_rate':           win_rate,
        'total_pnl':          total_pnl,
        'avg_pnl_per_trade':  round(total_pnl / len(pnls), 2) if pnls else 0,
        'best_trade':         {'ticker': best.get('ticker'), 'pnl': best.get('pnl')} if best else None,
        'worst_trade':        {'ticker': worst.get('ticker'), 'pnl': worst.get('pnl')} if worst else None,
        'avg_hold_days':      round(sum(hold_days_list) / len(hold_days_list), 1) if hold_days_list else 0,
    }


# ── Position monitor — runs in background thread ─────────────────

def _monitor_positions():
    """
    Poll open positions every 5 minutes.
    Auto-close when stop loss (-10%) or target (+20%) is hit.
    """
    logger.info('Paper trader: position monitor started')
    while True:
        try:
            if _enabled():
                _check_exits()
        except Exception as e:
            logger.error(f'Paper trader monitor error: {e}')
        time.sleep(POLL_INTERVAL)


def _check_exits():
    """Check each open position against stop loss / target and close if triggered."""
    _, positions = _alpaca('get', '/v2/positions')
    if not isinstance(positions, list):
        return

    # Load stored trades to get entry/stop/target
    open_trades = _supa_get('paper_trades', 'status=neq.closed&status=neq.cancelled')
    trade_map = {r.get('ticker'): r for r in (open_trades or [])}

    for pos in positions:
        ticker     = pos.get('symbol', '')
        cur_price  = float(pos.get('current_price') or 0)
        entry_price = float(pos.get('avg_entry_price') or 0)
        side       = pos.get('side', 'long')

        if cur_price <= 0 or entry_price <= 0:
            continue

        trade = trade_map.get(ticker, {})
        stop  = float(trade.get('stop_loss') or (entry_price * (1 - STOP_LOSS_PCT)))
        target = float(trade.get('target_price') or (entry_price * (1 + TARGET_PCT)))

        exit_reason = None
        if side == 'long':
            if cur_price <= stop:
                exit_reason = 'stop_loss'
            elif cur_price >= target:
                exit_reason = 'target_hit'
        elif side == 'short':
            if cur_price >= stop:
                exit_reason = 'stop_loss'
            elif cur_price <= target:
                exit_reason = 'target_hit'

        if exit_reason:
            _close_position(ticker, pos, trade, exit_reason, cur_price)


def _close_position(ticker, pos, trade, exit_reason, exit_price):
    """Liquidate a position on Alpaca and update Supabase."""
    logger.info(f'Paper trader: closing {ticker} — reason={exit_reason} exit_price={exit_price}')

    status_code, order = _alpaca('delete', f'/v2/positions/{ticker}')

    if status_code in (200, 201, 204):
        qty        = float(pos.get('qty') or 0)
        entry      = float(pos.get('avg_entry_price') or 0)
        side       = pos.get('side', 'long')
        pnl        = (exit_price - entry) * qty if side == 'long' else (entry - exit_price) * qty
        now        = datetime.now(timezone.utc).isoformat()
        order_id   = trade.get('order_id', '')

        _supa_upsert('paper_trades', [{
            'order_id':    order_id,
            'ticker':      ticker,
            'status':      'closed',
            'exit_price':  round(exit_price, 2),
            'exit_reason': exit_reason,
            'pnl':         round(pnl, 2),
            'closed_at':   now,
        }])
        logger.info(f'Paper trader: {ticker} closed | pnl=${pnl:.2f} | reason={exit_reason}')
    else:
        logger.error(f'Paper trader: failed to close {ticker}: {status_code} {order}')


# ── Start monitor thread on module import ────────────────────────
_monitor_thread = threading.Thread(target=_monitor_positions, daemon=True)
_monitor_thread.start()
