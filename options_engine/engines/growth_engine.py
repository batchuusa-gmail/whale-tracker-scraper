"""
APEX-65 — Growth Engine (Engine B): Directional Options Buying.
Captures breakout and trend moves using ATM/slightly-ITM calls (bullish)
or puts (bearish). Filters for low IV rank, volume spike, and MA/RSI trend.
"""
import logging
from dataclasses import dataclass, asdict
from datetime import datetime, date

import numpy as np

from options_engine.data.stock_fetcher import fetch_stock_snapshot
from options_engine.data.options_fetcher import fetch_options_chain
from options_engine.data.earnings_fetcher import has_earnings_within
from options_engine.analytics.greeks import enrich_chain_with_greeks
from options_engine.analytics.pop import enrich_chain_with_pop
from options_engine.analytics.contract_selector import select_growth_contract

logger = logging.getLogger(__name__)


@dataclass
class GrowthTrade:
    ticker:          str
    strategy:        str    # 'GROWTH'
    direction:       str    # 'bullish' or 'bearish'
    option_type:     str    # 'call' or 'put'
    strike:          float
    expiry:          str
    dte:             int
    premium:         float  # debit paid per share
    delta:           float
    gamma:           float
    vega:            float
    theta:           float
    iv_rank_proxy:   int
    volume_spike:    float
    rsi:             float
    breakout:        bool   # price broke above recent resistance
    earnings_risk:   bool
    stock_price:     float
    max_loss:        float  # = premium * 100 per contract
    scanned_at:      str

    def to_dict(self) -> dict:
        return asdict(self)


# ── Filters ───────────────────────────────────────────────────────────────────

_MAX_IV_RANK    = 40    # buy options when IV is cheap
_MIN_VOL_SPIKE  = 1.3   # volume at least 1.3x average
_MIN_AVG_VOL    = 300_000
_MIN_PREMIUM    = 0.20
_MAX_SPREAD_PCT = 0.12  # 12% max bid-ask spread
_MIN_OI         = 50


def _calc_rsi(closes: list, period: int = 14) -> float:
    if len(closes) < period + 1:
        return 50.0
    gains, losses = [], []
    for i in range(1, len(closes)):
        d = closes[i] - closes[i - 1]
        gains.append(max(d, 0))
        losses.append(max(-d, 0))
    avg_g = sum(gains[-period:]) / period
    avg_l = sum(losses[-period:]) / period
    if avg_l == 0:
        return 100.0
    return round(100 - (100 / (1 + avg_g / avg_l)), 1)


def _detect_breakout(closes: list, lookback: int = 20) -> bool:
    """True if current close breaks above the highest close in the prior lookback window."""
    if len(closes) < lookback + 1:
        return False
    resistance = max(closes[-(lookback + 1):-1])
    return closes[-1] > resistance


def _determine_direction(snap: dict, closes: list, min_vol_spike: float = _MIN_VOL_SPIKE) -> str | None:
    """
    Returns 'bullish' or 'bearish' based on:
    - RSI > 55 + above MA50 + volume spike → bullish
    - RSI < 45 + below MA50 + volume spike → bearish
    - Otherwise None (no clear signal)
    """
    rsi       = _calc_rsi(closes)
    above_ma50 = snap.get('above_ma50')
    vol_spike  = snap.get('volume_spike', 1.0)

    if rsi > 55 and above_ma50 is True and vol_spike >= min_vol_spike:
        return 'bullish'
    if rsi < 45 and above_ma50 is False and vol_spike >= min_vol_spike:
        return 'bearish'
    return None


def _fetch_closes(ticker: str) -> list:
    """Get recent closing prices for RSI/breakout calculation."""
    try:
        import yfinance as yf
        hist = yf.Ticker(ticker).history(period='3mo', auto_adjust=True)
        return list(hist['Close'].values.astype(float))
    except Exception:
        return []


# ── Main scan ─────────────────────────────────────────────────────────────────

def scan_growth(
    tickers: list[str],
    config: dict | None = None,
) -> list[GrowthTrade]:
    """
    Scan tickers for directional growth (options buying) candidates.
    Returns list of GrowthTrade sorted by delta * volume_spike (conviction score).
    """
    cfg = config or {}
    max_iv_rank   = int(cfg.get('max_iv_rank',    _MAX_IV_RANK))
    min_vol_spike = float(cfg.get('min_vol_spike', _MIN_VOL_SPIKE))
    min_avg_vol   = int(cfg.get('min_avg_volume',  _MIN_AVG_VOL))
    min_oi        = int(cfg.get('min_oi',          _MIN_OI))
    min_premium   = float(cfg.get('min_premium',   _MIN_PREMIUM))
    max_spread    = float(cfg.get('max_spread_pct', _MAX_SPREAD_PCT))
    dte_min       = int(cfg.get('dte_min',          20))
    dte_max       = int(cfg.get('dte_max',          60))

    candidates: list[GrowthTrade] = []

    for ticker in tickers:
        try:
            snap = fetch_stock_snapshot(ticker)
            if not snap:
                continue
            if snap['avg_volume_20d'] < min_avg_vol:
                continue
            if snap['iv_rank_proxy'] > max_iv_rank:
                logger.debug(f'growth_engine: {ticker} IV rank {snap["iv_rank_proxy"]} > {max_iv_rank}')
                continue
            if snap['volume_spike'] < min_vol_spike:
                continue

            closes = _fetch_closes(ticker)
            if not closes:
                continue

            direction = _determine_direction(snap, closes, min_vol_spike=min_vol_spike)
            if not direction:
                logger.debug(f'growth_engine: {ticker} no clear directional signal')
                continue

            chain = fetch_options_chain(ticker, min_dte=dte_min, max_dte=dte_max)
            if not chain:
                continue

            # Choose calls (bullish) or puts (bearish)
            df = chain['calls'].copy() if direction == 'bullish' else chain['puts'].copy()
            if df.empty:
                continue

            df = df[df['openInterest'] >= min_oi]
            if df.empty:
                continue

            enrich_chain_with_greeks(df, snap['price'])
            enrich_chain_with_pop(df, mode='growth')

            # Remove illiquid
            if 'spread_pct' in df.columns:
                df = df[df['spread_pct'] <= max_spread]
            if df.empty:
                continue

            contract = select_growth_contract(df, direction)
            if contract is None:
                continue

            premium = float(contract.get('mid') or contract.get('lastPrice') or 0)
            if premium < min_premium:
                continue

            rsi      = _calc_rsi(closes)
            breakout = _detect_breakout(closes)
            expiry_date   = date.fromisoformat(contract['expiry'])
            earnings_risk = has_earnings_within(ticker, expiry_date)

            trade = GrowthTrade(
                ticker        = ticker,
                strategy      = 'GROWTH',
                direction     = direction,
                option_type   = contract['option_type'],
                strike        = float(contract['strike']),
                expiry        = contract['expiry'],
                dte           = int(contract['dte']),
                premium       = round(premium, 4),
                delta         = float(contract.get('delta', 0)),
                gamma         = float(contract.get('gamma', 0)),
                vega          = float(contract.get('vega', 0)),
                theta         = float(contract.get('theta', 0)),
                iv_rank_proxy = snap['iv_rank_proxy'],
                volume_spike  = snap['volume_spike'],
                rsi           = rsi,
                breakout      = breakout,
                earnings_risk = earnings_risk,
                stock_price   = snap['price'],
                max_loss      = round(premium * 100, 2),
                scanned_at    = datetime.utcnow().isoformat(),
            )
            if earnings_risk:
                logger.info(f'growth_engine: {ticker} flagged — earnings within DTE ({contract["expiry"]})')
            candidates.append(trade)
            logger.info(
                f'growth_engine: {ticker} {direction.upper()} {contract["option_type"].upper()} '
                f'{contract["strike"]} exp={contract["expiry"]} prem={premium} delta={trade.delta:.2f}'
            )

        except Exception as e:
            logger.warning(f'growth_engine: error on {ticker}: {e}')

    # Sort by abs(delta) * volume_spike — highest conviction first
    candidates.sort(key=lambda t: abs(t.delta) * t.volume_spike, reverse=True)
    return candidates
