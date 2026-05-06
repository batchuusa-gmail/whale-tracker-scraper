"""
WHAL-79: Live Trading Engine — real money auto-trading via user's Alpaca account (Elite tier).

Each user provides their own Alpaca API keys, stored encrypted in Supabase.
The bot trades on their behalf using their keys — never the platform's keys.
"""
import os
import base64
import logging
import requests
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

SUPABASE_URL = os.environ.get('SUPABASE_URL', 'https://bedurjtazsfbnkisoeee.supabase.co')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY', '')
_SUPA_HDRS = {
    'apikey': SUPABASE_KEY,
    'Authorization': f'Bearer {SUPABASE_KEY}',
    'Content-Type': 'application/json',
    'Prefer': 'resolution=merge-duplicates,return=minimal',
}

# ── Encryption helpers (XOR with env secret — lightweight, Railway env is secure) ─────────

_ENC_SECRET = os.environ.get('LIVE_TRADING_ENC_SECRET', 'whale-tracker-xor-secret-2026')


def _encrypt(value: str) -> str:
    """Simple XOR+base64 encryption for API keys at rest."""
    key = _ENC_SECRET.encode()
    enc = bytes(v ^ key[i % len(key)] for i, v in enumerate(value.encode()))
    return base64.b64encode(enc).decode()


def _decrypt(value: str) -> str:
    """Decrypt XOR+base64 encoded string."""
    try:
        enc = base64.b64decode(value.encode())
        key = _ENC_SECRET.encode()
        return bytes(v ^ key[i % len(key)] for i, v in enumerate(enc)).decode()
    except Exception:
        return value  # return as-is if decryption fails (may already be plaintext)


# ── Supabase helpers ────────────────────────────────────────────────────────────

def _sb_get(table: str, query: str) -> list:
    try:
        r = requests.get(
            f'{SUPABASE_URL}/rest/v1/{table}?{query}',
            headers=_SUPA_HDRS, timeout=10,
        )
        return r.json() if r.status_code == 200 and isinstance(r.json(), list) else []
    except Exception as e:
        logger.warning(f'_sb_get {table}: {e}')
        return []


def _sb_upsert(table: str, row: dict, conflict: str = 'user_id') -> bool:
    try:
        r = requests.post(
            f'{SUPABASE_URL}/rest/v1/{table}?on_conflict={conflict}',
            headers=_SUPA_HDRS, json=row, timeout=10,
        )
        return r.status_code in (200, 201, 204)
    except Exception as e:
        logger.warning(f'_sb_upsert {table}: {e}')
        return False


def _sb_delete(table: str, query: str) -> bool:
    try:
        r = requests.delete(
            f'{SUPABASE_URL}/rest/v1/{table}?{query}',
            headers=_SUPA_HDRS, timeout=10,
        )
        return r.status_code in (200, 204)
    except Exception as e:
        logger.warning(f'_sb_delete {table}: {e}')
        return False


# ── Alpaca client per user ───────────────────────────────────────────────────────

def _alpaca_call(api_key: str, api_secret: str, method: str, path: str,
                 json_body: dict = None, live: bool = False) -> tuple[int, dict]:
    """Make an Alpaca API call using user-provided credentials."""
    base = 'https://api.alpaca.markets' if live else 'https://paper-api.alpaca.markets'
    hdrs = {'APCA-API-KEY-ID': api_key, 'APCA-API-SECRET-KEY': api_secret}
    try:
        fn = getattr(requests, method.lower())
        kwargs = {'headers': hdrs, 'timeout': 10}
        if json_body:
            kwargs['json'] = json_body
        r = fn(f'{base}{path}', **kwargs)
        try:
            return r.status_code, r.json()
        except Exception:
            return r.status_code, {}
    except Exception as e:
        logger.warning(f'Alpaca {method} {path}: {e}')
        return 0, {'error': str(e)}


# ── Public API ───────────────────────────────────────────────────────────────────

def connect_live_account(user_id: str, api_key: str, api_secret: str,
                          max_allocation: float = 5000.0, live_mode: bool = False) -> dict:
    """
    Validate and store user's Alpaca credentials.
    Returns {ok, error?, account_info?}
    """
    if not api_key or not api_secret:
        return {'ok': False, 'error': 'api_key and api_secret are required'}

    # Validate credentials against Alpaca
    sc, account = _alpaca_call(api_key, api_secret, 'get', '/v2/account', live=live_mode)
    if sc != 200:
        return {'ok': False, 'error': f'Invalid Alpaca credentials (HTTP {sc})'}

    # Store encrypted
    row = {
        'user_id':         user_id,
        'alpaca_key_enc':  _encrypt(api_key),
        'alpaca_sec_enc':  _encrypt(api_secret),
        'max_allocation':  max_allocation,
        'live_mode':       live_mode,
        'enabled':         True,
        'connected_at':    datetime.now(timezone.utc).isoformat(),
        'equity':          float(account.get('equity') or 0),
        'buying_power':    float(account.get('buying_power') or 0),
    }
    _sb_upsert('live_trading_accounts', row, conflict='user_id')

    return {
        'ok': True,
        'account_info': {
            'equity':       row['equity'],
            'buying_power': row['buying_power'],
            'account_number': account.get('account_number', ''),
            'live_mode':    live_mode,
        }
    }


def disconnect_live_account(user_id: str) -> dict:
    """Remove user's live trading credentials from Supabase."""
    ok = _sb_delete('live_trading_accounts', f'user_id=eq.{user_id}')
    return {'ok': ok}


def get_live_portfolio(user_id: str) -> dict:
    """Get current portfolio for user from their Alpaca account."""
    rows = _sb_get('live_trading_accounts', f'user_id=eq.{user_id}&enabled=eq.true')
    if not rows:
        return {'error': 'No connected account', 'positions': [], 'equity': 0}

    row = rows[0]
    api_key = _decrypt(row.get('alpaca_key_enc', ''))
    api_sec = _decrypt(row.get('alpaca_sec_enc', ''))
    live    = row.get('live_mode', False)

    sc_acc, account   = _alpaca_call(api_key, api_sec, 'get', '/v2/account', live=live)
    sc_pos, positions = _alpaca_call(api_key, api_sec, 'get', '/v2/positions', live=live)

    if sc_acc != 200:
        return {'error': 'Could not reach Alpaca', 'positions': [], 'equity': 0}

    pos_list = []
    if isinstance(positions, list):
        for p in positions:
            pos_list.append({
                'ticker':        p.get('symbol'),
                'qty':           float(p.get('qty', 0)),
                'avg_entry':     float(p.get('avg_entry_price', 0)),
                'current_price': float(p.get('current_price', 0)),
                'market_value':  float(p.get('market_value', 0)),
                'unrealized_pl': float(p.get('unrealized_pl', 0)),
                'unrealized_plpc': float(p.get('unrealized_plpc', 0)),
                'side':          p.get('side', 'long'),
            })

    return {
        'equity':       float(account.get('equity', 0)),
        'buying_power': float(account.get('buying_power', 0)),
        'cash':         float(account.get('cash', 0)),
        'pnl_today':    float(account.get('equity', 0)) - float(account.get('last_equity', 0)),
        'positions':    pos_list,
        'live_mode':    live,
        'max_allocation': float(row.get('max_allocation', 5000)),
    }


def get_live_trades(user_id: str, limit: int = 50) -> list:
    """Get trade history for user from Supabase live_trades table."""
    rows = _sb_get('live_trades', f'user_id=eq.{user_id}&order=created_at.desc&limit={limit}')
    return rows


def pause_live_trading(user_id: str) -> dict:
    """Pause bot for this user."""
    rows = _sb_get('live_trading_accounts', f'user_id=eq.{user_id}')
    if not rows:
        return {'ok': False, 'error': 'No account found'}
    _sb_upsert('live_trading_accounts', {'user_id': user_id, 'enabled': False}, conflict='user_id')
    return {'ok': True, 'status': 'paused'}


def resume_live_trading(user_id: str) -> dict:
    """Resume bot for this user."""
    rows = _sb_get('live_trading_accounts', f'user_id=eq.{user_id}')
    if not rows:
        return {'ok': False, 'error': 'No account found'}
    _sb_upsert('live_trading_accounts', {'user_id': user_id, 'enabled': True}, conflict='user_id')
    return {'ok': True, 'status': 'active'}
