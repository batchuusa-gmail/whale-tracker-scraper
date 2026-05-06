"""
Options chain fetcher using Yahoo Finance v7 API with crumb authentication.
yfinance is blocked on Railway — this uses raw requests with cookie/crumb auth.
"""
import logging
import time
from datetime import date, datetime
import pandas as pd

logger = logging.getLogger(__name__)

_cache: dict = {}
_CACHE_TTL = 300  # 5 minutes

# Shared session + crumb (refreshed when stale)
_session_state: dict = {'session': None, 'crumb': None, 'ts': 0}
_SESSION_TTL = 1800  # refresh session every 30 min

_REQUIRED_COLS = [
    'contractSymbol', 'strike', 'lastPrice', 'bid', 'ask',
    'volume', 'openInterest', 'impliedVolatility', 'inTheMoney',
]


def _cached(key: str, fn):
    entry = _cache.get(key)
    if entry and time.time() - entry['ts'] < _CACHE_TTL:
        return entry['val']
    val = fn()
    if val is not None:
        _cache[key] = {'val': val, 'ts': time.time()}
    return val


def _get_session_crumb() -> tuple:
    """Return (requests.Session, crumb_str). Refreshes if stale."""
    import requests
    state = _session_state
    if state['session'] and state['crumb'] and (time.time() - state['ts']) < _SESSION_TTL:
        return state['session'], state['crumb']

    sess = requests.Session()
    sess.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
    })
    try:
        sess.get('https://fc.yahoo.com', timeout=8)
    except Exception:
        pass
    try:
        r = sess.get('https://query1.finance.yahoo.com/v1/test/getcrumb', timeout=8)
        crumb = r.text.strip() if r.status_code == 200 else ''
    except Exception:
        crumb = ''

    state['session'] = sess
    state['crumb']   = crumb
    state['ts']      = time.time()
    return sess, crumb


def _dte(unix_ts: int) -> int:
    """Days to expiration from today given a Unix timestamp."""
    try:
        exp = date.fromtimestamp(unix_ts)
        return (exp - date.today()).days
    except Exception:
        return -1


def _parse_contracts(raw_list: list, option_type: str, expiry_str: str, dte: int) -> pd.DataFrame:
    """Convert list of Yahoo Finance contract dicts to a standard DataFrame."""
    if not raw_list:
        return pd.DataFrame()

    rows = []
    for c in raw_list:
        bid = float(c.get('bid') or 0)
        ask = float(c.get('ask') or 0)
        mid = round((bid + ask) / 2, 4)
        row = {
            'contractSymbol':    c.get('contractSymbol', ''),
            'strike':            float(c.get('strike') or 0),
            'lastPrice':         float(c.get('lastPrice') or 0),
            'bid':               bid,
            'ask':               ask,
            'mid':               mid,
            'volume':            int(c.get('volume') or 0),
            'openInterest':      int(c.get('openInterest') or 0),
            'impliedVolatility': float(c.get('impliedVolatility') or 0),
            'inTheMoney':        bool(c.get('inTheMoney', False)),
            'option_type':       option_type,
            'expiry':            expiry_str,
            'dte':               dte,
        }
        rows.append(row)

    df = pd.DataFrame(rows)
    # Drop zero IV or zero strike
    df = df[(df['impliedVolatility'] > 0) & (df['strike'] > 0)]
    # Drop contracts with no bid and no last price
    df = df[~((df['bid'] == 0) & (df['lastPrice'] == 0))]
    return df.reset_index(drop=True)


def fetch_options_chain(
    ticker: str,
    min_dte: int = 21,
    max_dte: int = 60,
) -> dict | None:
    """
    Fetch options chains for all expiries with DTE in [min_dte, max_dte].
    Uses Yahoo Finance v7 API with crumb auth.

    Returns {'calls': DataFrame, 'puts': DataFrame} or None.
    """
    def _fetch():
        try:
            sess, crumb = _get_session_crumb()
            if not crumb:
                logger.warning(f'options_fetcher {ticker}: no crumb available')
                return None

            # First request — get list of expiration dates
            r = sess.get(
                f'https://query2.finance.yahoo.com/v7/finance/options/{ticker}',
                params={'crumb': crumb},
                timeout=12,
            )
            if r.status_code != 200:
                logger.warning(f'options_fetcher {ticker}: HTTP {r.status_code}')
                return None

            data  = r.json()
            chain = (data.get('optionChain') or {}).get('result') or []
            if not chain:
                return None
            chain = chain[0]

            exp_dates = chain.get('expirationDates', [])  # list of unix timestamps

            calls_frames, puts_frames = [], []

            for unix_ts in exp_dates:
                dte = _dte(unix_ts)
                if dte < min_dte or dte > max_dte:
                    continue

                expiry_str = date.fromtimestamp(unix_ts).isoformat()

                # Fetch options for this specific expiration
                r2 = sess.get(
                    f'https://query2.finance.yahoo.com/v7/finance/options/{ticker}',
                    params={'crumb': crumb, 'date': unix_ts},
                    timeout=12,
                )
                if r2.status_code != 200:
                    continue

                d2    = r2.json()
                chain2 = (d2.get('optionChain') or {}).get('result') or []
                if not chain2:
                    continue
                opts = (chain2[0].get('options') or [{}])[0]

                calls_raw = opts.get('calls', [])
                puts_raw  = opts.get('puts', [])

                if calls_raw:
                    calls_frames.append(_parse_contracts(calls_raw, 'call', expiry_str, dte))
                if puts_raw:
                    puts_frames.append(_parse_contracts(puts_raw,  'put',  expiry_str, dte))

            if not calls_frames and not puts_frames:
                logger.warning(f'options_fetcher {ticker}: no valid expirations in DTE [{min_dte},{max_dte}]')
                return None

            calls = pd.concat(calls_frames, ignore_index=True) if calls_frames else pd.DataFrame()
            puts  = pd.concat(puts_frames,  ignore_index=True) if puts_frames  else pd.DataFrame()

            logger.info(f'options_fetcher {ticker}: {len(calls)} calls, {len(puts)} puts (DTE {min_dte}-{max_dte})')
            return {'calls': calls, 'puts': puts}

        except Exception as e:
            logger.warning(f'options_fetcher {ticker}: {e}')
            return None

    return _cached(f'chain:{ticker}:{min_dte}:{max_dte}', _fetch)


def fetch_options_for_tickers(
    tickers: list,
    min_dte: int = 21,
    max_dte: int = 60,
) -> dict:
    """Batch fetch options chains. Returns {ticker: {'calls': df, 'puts': df}}."""
    results = {}
    for ticker in tickers:
        chain = fetch_options_chain(ticker, min_dte, max_dte)
        if chain:
            results[ticker] = chain
        else:
            logger.debug(f'options_fetcher: no chain for {ticker}')
    return results
