"""
WHAL-91 — Technical Indicator Engine
RSI, MACD, Moving Averages, Volume, ATR, Bollinger Bands
Used to confirm or reject insider signals before paper/live trading.
"""

import logging
import yfinance as yf
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


# ─── Core calculations ──────────────────────────────────────────


def _ohlcv(ticker: str, period: str = '6mo') -> pd.DataFrame:
    """Fetch OHLCV data. Returns empty DataFrame on failure."""
    try:
        df = yf.download(ticker, period=period, progress=False, auto_adjust=True)
        if df.empty:
            return pd.DataFrame()
        # Flatten MultiIndex columns if present
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        return df
    except Exception as e:
        logger.warning(f'_ohlcv({ticker}): {e}')
        return pd.DataFrame()


def calculate_rsi(ticker: str, period: int = 14) -> float | None:
    """RSI(14). Returns float 0-100 or None on error."""
    try:
        df = _ohlcv(ticker)
        if len(df) < period + 1:
            return None
        close = df['Close'].astype(float)
        delta = close.diff()
        gain = delta.clip(lower=0).rolling(period).mean()
        loss = (-delta.clip(upper=0)).rolling(period).mean()
        rs = gain / loss.replace(0, float('nan'))
        rsi = 100 - (100 / (1 + rs))
        val = float(rsi.dropna().iloc[-1])
        return round(val, 2)
    except Exception as e:
        logger.warning(f'calculate_rsi({ticker}): {e}')
        return None


def calculate_macd(ticker: str) -> dict | None:
    """MACD(12,26,9). Returns dict with value, signal, histogram, crossover."""
    try:
        df = _ohlcv(ticker)
        if len(df) < 35:
            return None
        close = df['Close'].astype(float)
        ema12 = close.ewm(span=12, adjust=False).mean()
        ema26 = close.ewm(span=26, adjust=False).mean()
        macd_line   = ema12 - ema26
        signal_line = macd_line.ewm(span=9, adjust=False).mean()
        histogram   = macd_line - signal_line

        macd_val  = float(macd_line.iloc[-1])
        sig_val   = float(signal_line.iloc[-1])
        hist_val  = float(histogram.iloc[-1])
        prev_hist = float(histogram.iloc[-2]) if len(histogram) > 1 else 0

        # Detect crossover in last 2 bars
        if hist_val > 0 and prev_hist <= 0:
            crossover = 'bullish_crossover'
        elif hist_val < 0 and prev_hist >= 0:
            crossover = 'bearish_crossover'
        elif hist_val > 0:
            crossover = 'bullish'
        else:
            crossover = 'bearish'

        return {
            'macd':      round(macd_val, 4),
            'signal':    round(sig_val, 4),
            'histogram': round(hist_val, 4),
            'crossover': crossover,
        }
    except Exception as e:
        logger.warning(f'calculate_macd({ticker}): {e}')
        return None


def calculate_moving_averages(ticker: str) -> dict | None:
    """50-MA, 200-MA, 9-EMA, 21-EMA. Returns dict with values and signals."""
    try:
        df = _ohlcv(ticker, period='1y')
        if len(df) < 50:
            return None
        close = df['Close'].astype(float)
        current = float(close.iloc[-1])

        ma50  = float(close.rolling(50).mean().iloc[-1])
        ma200 = float(close.rolling(200).mean().iloc[-1]) if len(df) >= 200 else None
        ema9  = float(close.ewm(span=9,  adjust=False).mean().iloc[-1])
        ema21 = float(close.ewm(span=21, adjust=False).mean().iloc[-1])
        prev_ema9  = float(close.ewm(span=9,  adjust=False).mean().iloc[-2])
        prev_ema21 = float(close.ewm(span=21, adjust=False).mean().iloc[-2])

        ema_cross = None
        if ema9 > ema21 and prev_ema9 <= prev_ema21:
            ema_cross = 'bullish_crossover'
        elif ema9 < ema21 and prev_ema9 >= prev_ema21:
            ema_cross = 'bearish_crossover'
        elif ema9 > ema21:
            ema_cross = 'bullish'
        else:
            ema_cross = 'bearish'

        return {
            'current':          round(current, 2),
            'ma50':             round(ma50, 2),
            'ma200':            round(ma200, 2) if ma200 else None,
            'ema9':             round(ema9, 2),
            'ema21':            round(ema21, 2),
            'price_vs_50ma':    'above' if current > ma50  else 'below',
            'price_vs_200ma':   ('above' if current > ma200 else 'below') if ma200 else 'insufficient_data',
            'ema_crossover':    ema_cross,
        }
    except Exception as e:
        logger.warning(f'calculate_moving_averages({ticker}): {e}')
        return None


def calculate_volume_ratio(ticker: str, window: int = 20) -> float | None:
    """Current volume vs 20-day average. >1.5 = high conviction."""
    try:
        df = _ohlcv(ticker)
        if len(df) < window + 1:
            return None
        vol = df['Volume'].astype(float)
        avg = float(vol.rolling(window).mean().iloc[-1])
        cur = float(vol.iloc[-1])
        if avg == 0:
            return None
        return round(cur / avg, 2)
    except Exception as e:
        logger.warning(f'calculate_volume_ratio({ticker}): {e}')
        return None


def calculate_atr(ticker: str, period: int = 14) -> float | None:
    """ATR(14). Used to set dynamic stop loss (1.5× ATR below entry)."""
    try:
        df = _ohlcv(ticker)
        if len(df) < period + 1:
            return None
        high  = df['High'].astype(float)
        low   = df['Low'].astype(float)
        close = df['Close'].astype(float)
        prev_close = close.shift(1)
        tr = pd.concat([
            high - low,
            (high - prev_close).abs(),
            (low  - prev_close).abs(),
        ], axis=1).max(axis=1)
        atr = float(tr.rolling(period).mean().dropna().iloc[-1])
        return round(atr, 4)
    except Exception as e:
        logger.warning(f'calculate_atr({ticker}): {e}')
        return None


def calculate_bollinger_bands(ticker: str, period: int = 20, std_mult: float = 2.0) -> dict | None:
    """Bollinger Bands(20, 2). Returns upper/middle/lower and price position."""
    try:
        df = _ohlcv(ticker)
        if len(df) < period:
            return None
        close   = df['Close'].astype(float)
        mid     = close.rolling(period).mean()
        std     = close.rolling(period).std()
        upper   = mid + std_mult * std
        lower   = mid - std_mult * std
        current = float(close.iloc[-1])
        u = float(upper.iloc[-1])
        m = float(mid.iloc[-1])
        l = float(lower.iloc[-1])
        band_width = u - l
        pct_b = (current - l) / band_width if band_width > 0 else 0.5
        if pct_b >= 0.8:
            position = 'near_upper'
        elif pct_b <= 0.2:
            position = 'near_lower'
        else:
            position = 'middle'
        return {
            'upper':    round(u, 2),
            'middle':   round(m, 2),
            'lower':    round(l, 2),
            'pct_b':    round(pct_b, 3),
            'position': position,
        }
    except Exception as e:
        logger.warning(f'calculate_bollinger_bands({ticker}): {e}')
        return None


# ─── Combined technical score ────────────────────────────────────


def get_technical_score(ticker: str) -> dict:
    """
    Run all indicators and return a combined technical score.

    Scoring:
      Each indicator casts a vote: +1 bullish, -1 bearish, 0 neutral
      technical_score = sum / total_votes   (range -1.0 to +1.0)
      BULLISH  >= +0.5
      NEUTRAL   -0.5 to +0.5
      BEARISH  <= -0.5
    """
    ticker = ticker.upper().strip()
    votes  = []
    details = {}

    # RSI
    rsi = calculate_rsi(ticker)
    details['rsi'] = rsi
    if rsi is not None:
        if 40 <= rsi <= 70:
            votes.append(1)   # has momentum, not overbought
        elif rsi > 70:
            votes.append(-1)  # overbought
        else:
            votes.append(-1)  # oversold / weak

    # MACD
    macd = calculate_macd(ticker)
    details['macd'] = macd
    if macd:
        if macd['crossover'] in ('bullish_crossover', 'bullish'):
            votes.append(1)
        else:
            votes.append(-1)

    # Moving Averages
    ma = calculate_moving_averages(ticker)
    details['moving_averages'] = ma
    if ma:
        # Price vs 50 MA
        votes.append(1 if ma['price_vs_50ma'] == 'above' else -1)
        # Price vs 200 MA
        if ma['price_vs_200ma'] not in ('insufficient_data',):
            votes.append(1 if ma['price_vs_200ma'] == 'above' else -1)
        # EMA crossover
        if ma['ema_crossover'] in ('bullish_crossover', 'bullish'):
            votes.append(1)
        else:
            votes.append(-1)

    # Volume
    vol_ratio = calculate_volume_ratio(ticker)
    details['volume_ratio'] = vol_ratio
    if vol_ratio is not None:
        votes.append(1 if vol_ratio >= 1.5 else 0)

    # ATR — used for stop loss, not a directional vote
    atr = calculate_atr(ticker)
    details['atr'] = atr

    # Bollinger Bands
    bb = calculate_bollinger_bands(ticker)
    details['bollinger_bands'] = bb
    if bb:
        if bb['position'] == 'near_lower':
            votes.append(1)   # near support — potential bounce
        elif bb['position'] == 'near_upper':
            votes.append(-1)  # near resistance — potentially extended
        else:
            votes.append(0)

    # Score
    non_zero = [v for v in votes if v != 0]
    score = round(sum(votes) / len(votes), 3) if votes else 0.0

    if score >= 0.5:
        signal = 'BULLISH'
    elif score <= -0.5:
        signal = 'BEARISH'
    else:
        signal = 'NEUTRAL'

    # Dynamic stop loss (1.5× ATR below current price)
    current_price = details.get('moving_averages', {}) or {}
    cur_px = current_price.get('current')
    dynamic_stop = round(cur_px - 1.5 * atr, 2) if (cur_px and atr) else None

    return {
        'ticker':            ticker,
        'rsi':               rsi,
        'macd_signal':       macd.get('crossover') if macd else None,
        'macd_histogram':    macd.get('histogram') if macd else None,
        'price_vs_50ma':     ma.get('price_vs_50ma') if ma else None,
        'price_vs_200ma':    ma.get('price_vs_200ma') if ma else None,
        'ema_crossover':     ma.get('ema_crossover') if ma else None,
        'current_price':     ma.get('current') if ma else None,
        'ma50':              ma.get('ma50') if ma else None,
        'ma200':             ma.get('ma200') if ma else None,
        'volume_ratio':      vol_ratio,
        'atr':               atr,
        'bollinger_position': bb.get('position') if bb else None,
        'technical_score':   score,
        'technical_signal':  signal,
        'votes_bullish':     votes.count(1),
        'votes_bearish':     votes.count(-1),
        'total_votes':       len([v for v in votes if v != 0]),
        'dynamic_stop_loss': dynamic_stop,
        'details':           details,
    }
