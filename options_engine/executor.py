"""
Options Executor — places Alpaca paper options orders for Income and Growth picks.
Activated only when OPTIONS_TRADING_ENABLED=true.

Income picks  → sell-to-open put  (credit received)
Growth picks  → buy-to-open call/put (debit paid)
"""
import logging
import os
from datetime import date, timedelta

import requests

logger = logging.getLogger(__name__)

_PAPER_BASE = 'https://paper-api.alpaca.markets'

_SUPA_URL = os.environ.get('SUPABASE_URL', 'https://bedurjtazsfbnkisoeee.supabase.co')
_PT_URL   = f'{_SUPA_URL}/rest/v1/options_paper_trades'


def _enabled() -> bool:
    if os.environ.get('OPTIONS_TRADING_ENABLED', 'false').lower() != 'true':
        return False
    # Respect Supabase config toggle set via app Config screen
    try:
        key = os.environ.get('SUPABASE_KEY', '')
        hdrs = {'apikey': key, 'Authorization': f'Bearer {key}'}
        r = requests.get(f'{_SUPA_URL}/rest/v1/options_config?id=eq.1', headers=hdrs, timeout=3)
        if r.ok and r.json():
            return bool(r.json()[0].get('trading_enabled', True))
    except Exception:
        pass
    return True


def _alpaca_hdrs() -> dict:
    return {
        'APCA-API-KEY-ID':     os.environ.get('ALPACA_KEY', ''),
        'APCA-API-SECRET-KEY': os.environ.get('ALPACA_SECRET', ''),
        'Content-Type':        'application/json',
    }


def _supa_hdrs() -> dict:
    key = os.environ.get('SUPABASE_KEY', '')
    return {
        'apikey':        key,
        'Authorization': f'Bearer {key}',
        'Content-Type':  'application/json',
        'Prefer':        'return=representation',
    }


def _alpaca_get(path: str, params: dict = None):
    try:
        r = requests.get(f'{_PAPER_BASE}{path}', headers=_alpaca_hdrs(), params=params, timeout=10)
        return r.status_code, r.json()
    except Exception as e:
        logger.error(f'options_executor alpaca GET {path}: {e}')
        return 0, {}


def _alpaca_post(path: str, body: dict):
    try:
        r = requests.post(f'{_PAPER_BASE}{path}', headers=_alpaca_hdrs(), json=body, timeout=10)
        return r.status_code, r.json()
    except Exception as e:
        logger.error(f'options_executor alpaca POST {path}: {e}')
        return 0, {}


def _find_contract_symbol(ticker: str, option_type: str, strike: float, expiry: str) -> str | None:
    """Look up the Alpaca OCC symbol for a contract."""
    expiry_date = date.fromisoformat(expiry)
    params = {
        'underlying_symbols':  ticker,
        'type':                option_type.lower(),
        'expiration_date_gte': (expiry_date - timedelta(days=1)).isoformat(),
        'expiration_date_lte': (expiry_date + timedelta(days=1)).isoformat(),
        'strike_price_gte':    str(round(strike * 0.99, 2)),
        'strike_price_lte':    str(round(strike * 1.01, 2)),
        'limit':               10,
    }
    status, data = _alpaca_get('/v2/options/contracts', params)
    if status != 200:
        logger.warning(f'options_executor: contract lookup {ticker} {option_type} {strike} {expiry} → {status}: {data}')
        return None

    contracts = [c for c in data.get('option_contracts', []) if c.get('tradable')]
    # Prefer exact expiry + strike match
    exact = [c for c in contracts
             if c.get('expiration_date') == expiry
             and abs(float(c.get('strike_price', 0)) - strike) < 0.5]
    pool = exact or contracts
    if not pool:
        return None
    pool.sort(key=lambda c: abs(float(c.get('strike_price', 0)) - strike))
    symbol = pool[0]['symbol']
    logger.info(f'options_executor: resolved {ticker} {option_type} {strike} {expiry} → {symbol}')
    return symbol


def _already_open(ticker: str, option_type: str, strike: float, expiry: str) -> bool:
    """Return True if there's already an open paper trade for this exact contract."""
    try:
        r = requests.get(_PT_URL, headers=_supa_hdrs(), params={
            'ticker':      f'eq.{ticker}',
            'option_type': f'eq.{option_type.upper()}',
            'strike':      f'eq.{strike}',
            'expiry':      f'eq.{expiry}',
            'status':      'eq.open',
            'limit':       '1',
        }, timeout=5)
        return bool(r.ok and r.json())
    except Exception:
        return False


def _log_trade(pick: dict, contracts: int = 1):
    row = {
        'ticker':            pick['ticker'],
        'strategy':          pick.get('strategy', 'INCOME'),
        'direction':         pick.get('direction', 'short'),
        'option_type':       pick.get('option_type', 'PUT').upper(),
        'strike':            float(pick['strike']),
        'expiry':            pick['expiry'],
        'dte_at_entry':      int(pick.get('dte', 0)),
        'premium_entry':     float(pick.get('premium', 0)),
        'delta':             float(pick.get('delta', 0)),
        'pop':               float(pick.get('pop', 0)),
        'iv_rank':           int(pick.get('iv_rank_proxy', 0)),
        'stock_price_entry': float(pick.get('stock_price', 0)),
        'max_risk':          float(pick.get('max_risk', pick.get('max_loss', 0))),
        'max_loss':          float(pick.get('max_loss', 0)),
        'contracts':         contracts,
        'status':            'open',
        'earnings_risk':     bool(pick.get('earnings_risk', False)),
    }
    try:
        r = requests.post(_PT_URL, headers=_supa_hdrs(), json=row, timeout=10)
        if not r.ok:
            logger.error(f'options_executor: Supabase log failed: {r.status_code} {r.text[:200]}')
    except Exception as e:
        logger.error(f'options_executor: Supabase log error: {e}')


def _push_notification(pick: dict, side: str):
    try:
        key = os.environ.get('SUPABASE_KEY', '')
        hdrs = {'apikey': key, 'Authorization': f'Bearer {key}'}
        r = requests.get(f'{_SUPA_URL}/rest/v1/user_devices?select=fcm_token', headers=hdrs, timeout=8)
        tokens = [row['fcm_token'] for row in (r.json() if r.ok else []) if row.get('fcm_token')]
        if not tokens:
            return
        from firebase_push import send_to_tokens
        otype  = pick.get('option_type', 'PUT').upper()
        action = f"SELL {otype}" if side == 'sell' else f"BUY {otype}"
        title  = f"Options Trade: {pick['ticker']} {action}"
        body   = (f"${pick['strike']:.0f} exp {pick['expiry']} · "
                  f"${pick.get('premium', 0):.2f}/share")
        if pick.get('pop'):
            body += f" · POP {pick['pop']:.0%}"
        sent = send_to_tokens(tokens, title, body, {'type': 'options_trade', 'ticker': pick['ticker']})
        logger.info(f'options_executor: FCM push → {sent} devices')
    except Exception as e:
        logger.warning(f'options_executor: push error: {e}')


def execute_income_pick(pick: dict) -> dict:
    """Sell-to-open a cash-secured put for an Income pick."""
    if not _enabled():
        return {'status': 'disabled'}

    ticker = pick['ticker']
    strike = float(pick['strike'])
    expiry = pick['expiry']
    premium = float(pick.get('premium', 0))

    if _already_open(ticker, 'put', strike, expiry):
        logger.info(f'options_executor: skipping {ticker} PUT {strike} {expiry} — already open')
        return {'status': 'skipped', 'reason': 'already open'}

    symbol = _find_contract_symbol(ticker, 'put', strike, expiry)
    if not symbol:
        return {'status': 'error', 'reason': f'contract not found: {ticker} put {strike} {expiry}'}

    order_body = {
        'symbol':        symbol,
        'qty':           '1',
        'side':          'sell',
        'type':          'limit',
        'time_in_force': 'day',
        'limit_price':   str(round(premium, 2)),
    }
    status, resp = _alpaca_post('/v2/orders', order_body)
    if status not in (200, 201):
        logger.error(f'options_executor: SELL PUT {symbol} failed {status}: {resp}')
        return {'status': 'error', 'reason': str(resp.get('message', resp)), 'symbol': symbol}

    order_id = resp.get('id', '')
    logger.info(f'options_executor: SELL PUT {symbol} prem={premium:.2f} order={order_id}')
    _log_trade(pick)
    _push_notification(pick, 'sell')
    return {'status': 'ok', 'order_id': order_id, 'symbol': symbol}


def execute_growth_pick(pick: dict) -> dict:
    """Buy-to-open a directional call/put for a Growth pick."""
    if not _enabled():
        return {'status': 'disabled'}

    ticker      = pick['ticker']
    strike      = float(pick['strike'])
    expiry      = pick['expiry']
    premium     = float(pick.get('premium', 0))
    option_type = pick.get('option_type', 'call').lower()

    if _already_open(ticker, option_type, strike, expiry):
        logger.info(f'options_executor: skipping {ticker} {option_type} {strike} {expiry} — already open')
        return {'status': 'skipped', 'reason': 'already open'}

    symbol = _find_contract_symbol(ticker, option_type, strike, expiry)
    if not symbol:
        return {'status': 'error', 'reason': f'contract not found: {ticker} {option_type} {strike} {expiry}'}

    # Use ask-side price (slightly above mid) to improve fill probability
    limit_price = round(premium * 1.03, 2)
    order_body = {
        'symbol':        symbol,
        'qty':           '1',
        'side':          'buy',
        'type':          'limit',
        'time_in_force': 'day',
        'limit_price':   str(limit_price),
    }
    status, resp = _alpaca_post('/v2/orders', order_body)
    if status not in (200, 201):
        logger.error(f'options_executor: BUY {option_type.upper()} {symbol} failed {status}: {resp}')
        return {'status': 'error', 'reason': str(resp.get('message', resp)), 'symbol': symbol}

    order_id = resp.get('id', '')
    logger.info(f'options_executor: BUY {option_type.upper()} {symbol} prem={premium:.2f} order={order_id}')
    _log_trade(pick)
    _push_notification(pick, 'buy')
    return {'status': 'ok', 'order_id': order_id, 'symbol': symbol}
