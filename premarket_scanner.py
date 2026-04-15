"""
WHAL-121 — Pre-Market Gap Scanner
Runs every 5 minutes from 4:00 AM to 9:30 AM ET on trading days.
Uses Alpaca extended-hours bars — reliable on Railway server IPs.

Detects:
  - Gap % vs previous close (up or down)
  - Pre-market volume vs 10-day average
  - Direction bias (bullish/bearish)

Stores top 20 gappers in Redis as `premarket:watchlist`.
Intraday scanner reads this at 9:30 AM open and prioritizes these stocks.
"""

import os
import logging
import time
from datetime import datetime, timedelta, date

import pytz
import requests

logger = logging.getLogger(__name__)

ALPACA_KEY    = os.environ.get('ALPACA_KEY', '')
ALPACA_SECRET = os.environ.get('ALPACA_SECRET', '')
_ET = pytz.timezone('America/New_York')

# ── Scan universe — high-liquidity stocks most active pre-market ──
PREMARKET_UNIVERSE = [
    # Mega-cap tech (most pre-market activity)
    'AAPL', 'MSFT', 'NVDA', 'GOOGL', 'AMZN', 'META', 'TSLA', 'AVGO',
    'AMD', 'INTC', 'QCOM', 'MU', 'CRM', 'ADBE', 'NFLX', 'ORCL',
    # Finance
    'JPM', 'BAC', 'GS', 'MS', 'V', 'MA', 'C', 'WFC',
    # High-beta / retail favorites
    'PLTR', 'COIN', 'HOOD', 'SOFI', 'RIVN', 'LCID', 'NIO', 'SNOW',
    'CRWD', 'PANW', 'NET', 'DDOG', 'ZS', 'UBER', 'DASH', 'ABNB',
    'SHOP', 'SQ', 'PYPL', 'RBLX',
    # ETFs (market direction)
    'SPY', 'QQQ', 'IWM', 'TQQQ', 'SQQQ', 'SOXL', 'SOXS',
]

MIN_GAP_PCT    = 1.0   # minimum % gap to include in watchlist
MIN_PREMARKET_VOL = 0   # IEX latestTrade.s is per-trade size, not total volume — skip vol filter

# ── In-memory state ──────────────────────────────────────────────
_state = {
    'watchlist':    [],
    'last_scan':    None,
    'scan_count':   0,
    'market_date':  None,
}


def _alpaca_headers():
    return {
        'APCA-API-KEY-ID':     ALPACA_KEY,
        'APCA-API-SECRET-KEY': ALPACA_SECRET,
    }


def _is_trading_day(date_str: str) -> bool:
    """Check Alpaca calendar for holidays."""
    if not ALPACA_KEY:
        return True
    try:
        r = requests.get(
            'https://paper-api.alpaca.markets/v2/calendar',
            headers=_alpaca_headers(),
            params={'start': date_str, 'end': date_str},
            timeout=8,
        )
        if r.status_code == 200:
            return len(r.json()) > 0
    except Exception:
        pass
    return True


def _is_premarket() -> bool:
    """True between 4:00 AM and 9:30 AM ET on a trading day."""
    now = datetime.now(_ET)
    if now.weekday() >= 5:
        return False
    premarket_start = now.replace(hour=4,  minute=0,  second=0, microsecond=0)
    premarket_end   = now.replace(hour=9,  minute=30, second=0, microsecond=0)
    if not (premarket_start <= now < premarket_end):
        return False
    return _is_trading_day(now.strftime('%Y-%m-%d'))


def _fetch_prev_close(symbols: list) -> dict:
    """
    Fetch previous day's closing prices via Alpaca Snapshots API.
    Snapshots include prevDailyBar (yesterday's OHLCV) for all symbols — free with IEX feed.
    """
    prev_closes = {}
    if not ALPACA_KEY:
        return prev_closes
    try:
        chunk_size = 100
        for i in range(0, len(symbols), chunk_size):
            chunk = symbols[i:i + chunk_size]
            r = requests.get(
                'https://data.alpaca.markets/v2/stocks/snapshots',
                headers=_alpaca_headers(),
                params={'symbols': ','.join(chunk), 'feed': 'iex'},
                timeout=15,
            )
            logger.info('Snapshot HTTP %d body_len=%d', r.status_code, len(r.text))
            if r.status_code == 200:
                snap_data = r.json()
                logger.info('Snapshot keys: %s', list(snap_data.keys())[:5])
                for sym, snap in snap_data.items():
                    prev_bar = snap.get('prevDailyBar') or {}
                    daily_bar = snap.get('dailyBar') or {}
                    if prev_bar.get('c'):
                        prev_closes[sym] = float(prev_bar['c'])
                    elif daily_bar.get('c'):
                        prev_closes[sym] = float(daily_bar['c'])
            else:
                logger.warning('Snapshot HTTP %d: %s', r.status_code, r.text[:300])
    except Exception as e:
        logger.warning(f'Prev close fetch error: {e}')
    return prev_closes


def _fetch_premarket_bars(symbols: list) -> dict:
    """
    Fetch current pre-market price via Alpaca Snapshots latestTrade.
    IEX covers real-time trades for most major stocks including pre-market.
    Returns synthetic single-bar dicts keyed by symbol.
    """
    results = {}
    if not ALPACA_KEY:
        return results
    try:
        chunk_size = 100
        for i in range(0, len(symbols), chunk_size):
            chunk = symbols[i:i + chunk_size]
            r = requests.get(
                'https://data.alpaca.markets/v2/stocks/snapshots',
                headers=_alpaca_headers(),
                params={'symbols': ','.join(chunk), 'feed': 'iex'},
                timeout=15,
            )
            if r.status_code == 200:
                for sym, snap in r.json().items():
                    # Use latestTrade price as current pre-market price
                    trade = snap.get('latestTrade') or {}
                    minute_bar = snap.get('minuteBar') or {}
                    daily_bar = snap.get('dailyBar') or {}
                    price = trade.get('p') or minute_bar.get('c')
                    # Pre-market volume = today's accumulated daily bar volume (before open)
                    vol = int(daily_bar.get('v', 0)) or int(trade.get('s', 0))
                    if price:
                        results[sym] = [{'c': float(price), 'v': vol}]
            else:
                logger.warning('PM snapshot HTTP %d: %s', r.status_code, r.text[:300])
    except Exception as e:
        logger.warning(f'Pre-market bars fetch error: {e}')
    logger.info('PM snapshot: %d symbols with latest trade', len(results))
    return results


def run_scan(redis_set_fn=None, redis_get_fn=None) -> list:
    """
    Run one pre-market gap scan.
    Returns list of top gapper dicts sorted by abs(gap_pct) descending.
    Stores result in Redis as premarket:watchlist.
    """
    logger.info('Pre-market scan: starting… (key_set=%s)', bool(ALPACA_KEY))

    prev_closes   = _fetch_prev_close(PREMARKET_UNIVERSE)
    premarket_bars = _fetch_premarket_bars(PREMARKET_UNIVERSE)

    logger.info('Pre-market scan: prev_closes=%d premarket_bars=%d',
                len(prev_closes), len(premarket_bars))

    if not prev_closes or not premarket_bars:
        logger.warning('Pre-market scan: no data returned from Alpaca '
                       '(prev_closes=%d, premarket_bars=%d)',
                       len(prev_closes), len(premarket_bars))
        return []

    gappers = []
    for sym in PREMARKET_UNIVERSE:
        bars      = premarket_bars.get(sym, [])
        prev_close = prev_closes.get(sym)
        if not bars or not prev_close or prev_close <= 0:
            continue

        # Current pre-market price = last bar's close
        pm_price = float(bars[-1]['c'])
        gap_pct  = round((pm_price - prev_close) / prev_close * 100, 2)

        # Pre-market volume = sum of all bar volumes
        pm_volume = sum(int(b.get('v', 0)) for b in bars)

        if abs(gap_pct) < MIN_GAP_PCT or (MIN_PREMARKET_VOL > 0 and pm_volume < MIN_PREMARKET_VOL):
            continue

        direction = 'up' if gap_pct > 0 else 'down'
        gappers.append({
            'ticker':      sym,
            'prev_close':  round(prev_close, 2),
            'pm_price':    round(pm_price, 2),
            'gap_pct':     gap_pct,
            'pm_volume':   pm_volume,
            'direction':   direction,
            'bar_count':   len(bars),
            'scanned_at':  datetime.now(_ET).isoformat(),
        })

    # Sort by abs gap % descending
    gappers.sort(key=lambda x: abs(x['gap_pct']), reverse=True)
    top = gappers[:20]

    _state['watchlist']  = top
    _state['last_scan']  = datetime.now(_ET).isoformat()
    _state['scan_count'] += 1
    _state['market_date'] = datetime.now(_ET).strftime('%Y-%m-%d')

    if redis_set_fn:
        redis_set_fn('premarket:watchlist', {
            'watchlist':  top,
            'last_scan':  _state['last_scan'],
            'scan_count': _state['scan_count'],
        }, ttl_seconds=600)

    logger.info(f'Pre-market scan: {len(top)} gappers found '
                f'(up: {sum(1 for g in top if g["direction"]=="up")}, '
                f'down: {sum(1 for g in top if g["direction"]=="down")})')
    return top


def get_watchlist(redis_get_fn=None) -> dict:
    """Return current pre-market watchlist from Redis or in-memory state."""
    if redis_get_fn:
        cached = redis_get_fn('premarket:watchlist')
        if cached:
            return cached
    return {
        'watchlist':  _state['watchlist'],
        'last_scan':  _state['last_scan'],
        'scan_count': _state['scan_count'],
    }


def get_priority_tickers(redis_get_fn=None) -> set:
    """Return set of pre-market gapper tickers for intraday scanner priority."""
    data = get_watchlist(redis_get_fn)
    return {g['ticker'] for g in data.get('watchlist', [])}


# ── Background loop ──────────────────────────────────────────────

def _scan_loop(redis_set_fn=None, redis_get_fn=None):
    """Runs every 5 minutes. Active only during pre-market hours (4–9:30 AM ET)."""
    logger.info('Pre-market scanner thread started')
    while True:
        try:
            if _is_premarket():
                run_scan(redis_set_fn, redis_get_fn)
            else:
                now = datetime.now(_ET)
                # Clear stale watchlist at end of trading day
                if now.hour >= 16 and _state['watchlist']:
                    _state['watchlist'] = []
                    logger.info('Pre-market watchlist cleared (end of day)')
        except Exception as e:
            logger.error(f'Pre-market scanner error: {e}')
        time.sleep(300)  # every 5 minutes


def start(redis_set_fn=None, redis_get_fn=None):
    """Start the pre-market scanner background thread."""
    import threading
    t = threading.Thread(
        target=_scan_loop,
        args=(redis_set_fn, redis_get_fn),
        daemon=True,
        name='premarket-scanner',
    )
    t.start()
    logger.info('Pre-market scanner: background thread launched')
