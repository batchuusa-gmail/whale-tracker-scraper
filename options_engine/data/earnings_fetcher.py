"""
Earnings calendar fetcher using Yahoo Finance quoteSummary calendarEvents module.
Reuses the existing crumb session from options_fetcher to avoid duplicate auth.
"""
import logging
import time
from datetime import date

logger = logging.getLogger(__name__)

_cache: dict = {}
_CACHE_TTL = 3600  # earnings dates only change weekly — 1hr TTL is fine


def _cached(key: str, fn):
    entry = _cache.get(key)
    if entry and time.time() - entry['ts'] < _CACHE_TTL:
        return entry['val']
    val = fn()
    if val is not None:
        _cache[key] = {'val': val, 'ts': time.time()}
    return val


def get_next_earnings_date(ticker: str) -> date | None:
    """
    Return the next upcoming earnings date for ticker, or None if unknown.
    Uses Yahoo Finance quoteSummary calendarEvents — same crumb session as options_fetcher.
    """
    def _fetch():
        try:
            from options_engine.data.options_fetcher import _get_session_crumb
            sess, crumb = _get_session_crumb()
            if not crumb:
                return None

            r = sess.get(
                f'https://query2.finance.yahoo.com/v10/finance/quoteSummary/{ticker}',
                params={'crumb': crumb, 'modules': 'calendarEvents'},
                timeout=10,
            )
            if r.status_code != 200:
                return None

            data   = r.json()
            result = ((data.get('quoteSummary') or {}).get('result') or [])
            if not result:
                return None

            events   = result[0].get('calendarEvents') or {}
            earnings = events.get('earnings') or {}
            dates    = earnings.get('earningsDate') or []

            today = date.today()
            for entry in dates:
                raw_ts = entry.get('raw')
                if raw_ts is None:
                    continue
                try:
                    d = date.fromtimestamp(raw_ts)
                    if d >= today:
                        return d
                except Exception:
                    continue
            return None

        except Exception as e:
            logger.debug(f'earnings_fetcher {ticker}: {e}')
            return None

    return _cached(f'earn:{ticker}', _fetch)


def has_earnings_within(ticker: str, expiry: date) -> bool:
    """
    True if ticker has a confirmed upcoming earnings date on or before expiry.
    Safe to call — returns False on any failure so it never blocks valid picks.
    """
    try:
        earn_date = get_next_earnings_date(ticker)
        if earn_date is None:
            return False
        return earn_date <= expiry
    except Exception:
        return False
