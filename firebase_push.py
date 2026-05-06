"""
WHAL-111 — Firebase Cloud Messaging sender.

Uses the FCM HTTP v1 API with a Service Account JSON stored as env var:
  FIREBASE_SERVICE_ACCOUNT_JSON={"type":"service_account", ...}

If the env var is not set, all send calls silently no-op (safe for dev).
"""

import os
import json
import logging
import time
import threading
from typing import Optional

logger = logging.getLogger(__name__)

_FIREBASE_PROJECT_ID = 'whaletracker-79d6a'
_FCM_ENDPOINT = f'https://fcm.googleapis.com/v1/projects/{_FIREBASE_PROJECT_ID}/messages:send'

# ── Service account credentials (lazy-loaded) ─────────────────────────────────
_creds = None
_creds_lock = threading.Lock()
_token: Optional[str] = None
_token_expiry: float = 0


def _load_credentials():
    global _creds
    sa_json = os.environ.get('FIREBASE_SERVICE_ACCOUNT_JSON', '')
    if not sa_json:
        return None
    try:
        import google.oauth2.service_account as sa
        info = json.loads(sa_json)
        _creds = sa.Credentials.from_service_account_info(
            info,
            scopes=['https://www.googleapis.com/auth/firebase.messaging'],
        )
        return _creds
    except Exception as e:
        logger.warning(f'Firebase credentials load error: {e}')
        return None


def _get_access_token() -> Optional[str]:
    """Return a fresh OAuth2 access token for the FCM v1 API."""
    global _token, _token_expiry
    with _creds_lock:
        if _token and time.time() < _token_expiry - 60:
            return _token
        creds = _creds or _load_credentials()
        if creds is None:
            return None
        try:
            import google.auth.transport.requests as google_requests
            creds.refresh(google_requests.Request())
            _token = creds.token
            # google-auth sets expiry as a datetime
            if creds.expiry:
                _token_expiry = creds.expiry.timestamp()
            else:
                _token_expiry = time.time() + 3600
            return _token
        except Exception as e:
            logger.warning(f'Firebase token refresh error: {e}')
            return None


def send_to_token(token: str, title: str, body: str, data: dict = None) -> bool:
    """Send a push notification to a single FCM device token."""
    access_token = _get_access_token()
    if not access_token:
        logger.debug('FCM: no credentials configured — skipping push')
        return False
    try:
        import requests
        payload = {
            'message': {
                'token': token,
                'notification': {'title': title, 'body': body},
                'data': {k: str(v) for k, v in (data or {}).items()},
                'android': {'priority': 'high'},
                'apns': {'payload': {'aps': {'sound': 'default'}}},
            }
        }
        r = requests.post(
            _FCM_ENDPOINT,
            headers={
                'Authorization': f'Bearer {access_token}',
                'Content-Type': 'application/json',
            },
            json=payload,
            timeout=10,
        )
        if r.status_code == 200:
            return True
        logger.warning(f'FCM send failed {r.status_code}: {r.text[:200]}')
        return False
    except Exception as e:
        logger.warning(f'FCM send error: {e}')
        return False


def send_to_tokens(tokens: list, title: str, body: str, data: dict = None) -> int:
    """Send to multiple tokens; returns count of successful sends."""
    return sum(1 for t in tokens if send_to_token(t, title, body, data))


def send_to_all_devices(title: str, body: str, data: dict = None,
                         supabase=None) -> int:
    """Send to every FCM token in the user_devices Supabase table."""
    if supabase is None:
        return 0
    try:
        rows = supabase.table('user_devices').select('fcm_token').execute()
        tokens = [r['fcm_token'] for r in (rows.data or []) if r.get('fcm_token')]
        if not tokens:
            return 0
        return send_to_tokens(tokens, title, body, data)
    except Exception as e:
        logger.warning(f'send_to_all_devices error: {e}')
        return 0


def notify_watchlist_activity(ticker: str, insider: str, value: float,
                               supabase_client=None) -> int:
    """
    Notify all users who have `ticker` in their watchlist.
    Reads FCM tokens from the Supabase `user_devices` + `user_watchlist` tables.
    Returns number of notifications sent.
    """
    if supabase_client is None:
        return 0
    try:
        # Find users who watch this ticker
        wl_rows = supabase_client.table('user_watchlist') \
            .select('user_id') \
            .eq('ticker', ticker) \
            .execute()
        user_ids = list({r['user_id'] for r in (wl_rows.data or []) if r.get('user_id')})
        if not user_ids:
            return 0

        # Get their FCM tokens
        dev_rows = supabase_client.table('user_devices') \
            .select('fcm_token') \
            .in_('user_id', user_ids) \
            .execute()
        tokens = [r['fcm_token'] for r in (dev_rows.data or []) if r.get('fcm_token')]
        if not tokens:
            return 0

        val_str = f'${value/1_000_000:.1f}M' if value >= 1_000_000 else f'${value:,.0f}'
        title = f'🐋 Whale Alert: {ticker}'
        body  = f'{insider} bought {val_str} — insider activity detected'
        return send_to_tokens(tokens, title, body, {'ticker': ticker})
    except Exception as e:
        logger.warning(f'notify_watchlist_activity error: {e}')
        return 0
