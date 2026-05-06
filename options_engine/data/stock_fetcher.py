"""
Stock price, volume, and historical volatility fetcher.
Uses Alpaca historical bars API (primary) — already authenticated on Railway.
yfinance kept as fallback for any ticker Alpaca doesn't carry.
"""
import logging
import os
import time
import numpy as np

logger = logging.getLogger(__name__)

_cache: dict = {}
_CACHE_TTL = 300  # 5 minutes

_ALPACA_KEY    = os.environ.get('ALPACA_KEY', '')
_ALPACA_SECRET = os.environ.get('ALPACA_SECRET', '')
_ALPACA_DATA   = 'https://data.alpaca.markets'


def _cached(key: str, fn):
    entry = _cache.get(key)
    if entry and time.time() - entry['ts'] < _CACHE_TTL:
        return entry['val']
    val = fn()
    # Don't cache None — allow retry next time
    if val is not None:
        _cache[key] = {'val': val, 'ts': time.time()}
    return val


def _alpaca_bars(ticker: str, days: int = 400) -> list[dict] | None:
    """Fetch daily bars from Alpaca data API. Returns list of bar dicts or None."""
    if not _ALPACA_KEY or not _ALPACA_SECRET:
        return None
    try:
        import requests
        from datetime import date, timedelta
        start = (date.today() - timedelta(days=days)).isoformat()
        end   = date.today().isoformat()
        # Try without feed restriction first — SIP has fuller history
        for feed in [None, 'iex']:
            params = {
                'timeframe': '1Day',
                'start':     start,
                'end':       end,
                'limit':     1000,
                'sort':      'asc',
            }
            if feed:
                params['feed'] = feed
            headers = {
                'APCA-API-KEY-ID':     _ALPACA_KEY,
                'APCA-API-SECRET-KEY': _ALPACA_SECRET,
            }
            r = requests.get(
                f'{_ALPACA_DATA}/v2/stocks/{ticker}/bars',
                params=params, headers=headers, timeout=15,
            )
            if r.status_code == 200:
                bars = r.json().get('bars', [])
                if bars and len(bars) >= 21:
                    logger.debug(f'stock_fetcher alpaca {ticker}: {len(bars)} bars (feed={feed})')
                    return bars
            else:
                logger.warning(f'stock_fetcher alpaca {ticker} feed={feed}: HTTP {r.status_code} {r.text[:100]}')
        return None
    except Exception as e:
        logger.warning(f'stock_fetcher alpaca {ticker}: {e}')
        return None


def _yf_bars(ticker: str) -> tuple[list, list] | None:
    """Fallback: fetch closes + volumes via yfinance."""
    try:
        import yfinance as yf
        hist = yf.download(ticker, period='1y', auto_adjust=True, progress=False, show_errors=False)
        if hist is None or hist.empty or len(hist) < 21:
            t = yf.Ticker(ticker)
            hist = t.history(period='1y', auto_adjust=True)
        if hist is None or hist.empty or len(hist) < 21:
            return None
        if hasattr(hist.columns, 'get_level_values'):
            hist.columns = hist.columns.get_level_values(0)
        closes  = list(hist['Close'].values.astype(float))
        volumes = list(hist['Volume'].values.astype(float))
        return closes, volumes
    except Exception as e:
        logger.warning(f'stock_fetcher yfinance {ticker}: {e}')
        return None


def fetch_stock_snapshot(ticker: str) -> dict | None:
    """
    Return snapshot dict: price, prev_close, change_pct,
    avg_volume_20d, volume_today, ma_50, ma_200,
    hv_20, iv_rank_proxy, above_ma50, above_ma200.
    Returns None on failure.
    """
    def _fetch():
        closes, volumes = [], []

        # Primary: Alpaca daily bars
        bars = _alpaca_bars(ticker)
        if bars and len(bars) >= 21:
            closes  = [float(b['c']) for b in bars]
            volumes = [float(b['v']) for b in bars]
            logger.debug(f'stock_fetcher {ticker}: alpaca OK {len(bars)} bars')
        else:
            # Fallback: yfinance
            result = _yf_bars(ticker)
            if result:
                closes, volumes = result
                logger.debug(f'stock_fetcher {ticker}: yfinance fallback OK {len(closes)} bars')

        if len(closes) < 21:
            logger.warning(f'stock_fetcher {ticker}: insufficient data ({len(closes)} bars)')
            return None

        closes  = np.array(closes, dtype=float)
        volumes = np.array(volumes, dtype=float)

        price      = float(closes[-1])
        prev_close = float(closes[-2])
        ma_50      = float(np.mean(closes[-50:]))  if len(closes) >= 50  else None
        ma_200     = float(np.mean(closes[-200:])) if len(closes) >= 200 else None

        # 20-day historical volatility (annualised %)
        log_rets = np.log(closes[-21:] / closes[-22:-1]) if len(closes) >= 22 else np.log(closes[1:] / closes[:-1])
        hv_20 = float(np.std(log_rets) * np.sqrt(252) * 100)

        # IV rank proxy: percentile of current 20d HV vs 1-yr rolling distribution
        hv_series = []
        for i in range(21, len(closes)):
            lr = np.log(closes[i - 20:i] / closes[i - 21:i - 1])
            if len(lr) == 20:
                hv_series.append(float(np.std(lr) * np.sqrt(252) * 100))
        iv_rank_proxy = int(
            np.sum(np.array(hv_series) <= hv_20) / len(hv_series) * 100
        ) if hv_series else 50

        avg_vol_20 = float(np.mean(volumes[-20:]))
        vol_today  = float(volumes[-1])

        return {
            'ticker':        ticker,
            'price':         round(price, 4),
            'prev_close':    round(prev_close, 4),
            'change_pct':    round((price - prev_close) / prev_close * 100, 2),
            'volume_today':  int(vol_today),
            'avg_volume_20d': int(avg_vol_20),
            'volume_spike':  round(vol_today / avg_vol_20, 2) if avg_vol_20 > 0 else 1.0,
            'ma_50':         round(ma_50, 4)  if ma_50  else None,
            'ma_200':        round(ma_200, 4) if ma_200 else None,
            'hv_20':         round(hv_20, 2),
            'iv_rank_proxy': iv_rank_proxy,
            'above_ma50':    price > ma_50  if ma_50  else None,
            'above_ma200':   price > ma_200 if ma_200 else None,
        }

    return _cached(f'snap:{ticker}', _fetch)


def fetch_batch(tickers: list[str]) -> dict[str, dict]:
    """Fetch snapshots for multiple tickers. Returns {ticker: snapshot}."""
    results = {}
    for ticker in tickers:
        snap = fetch_stock_snapshot(ticker)
        if snap:
            results[ticker] = snap
    return results
