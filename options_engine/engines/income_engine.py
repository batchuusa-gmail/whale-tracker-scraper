"""
APEX-63 — Income Engine (Engine A): Options Selling.
High-probability short put strategy. Targets cash-secured puts
with delta 0.20-0.30, DTE 30-45, IV Rank proxy > 50.
"""
import logging
from dataclasses import dataclass, asdict
from datetime import datetime, date

import pandas as pd

from options_engine.data.stock_fetcher import fetch_stock_snapshot
from options_engine.data.options_fetcher import fetch_options_chain
from options_engine.data.earnings_fetcher import has_earnings_within
from options_engine.analytics.greeks import enrich_chain_with_greeks
from options_engine.analytics.pop import enrich_chain_with_pop
from options_engine.analytics.contract_selector import select_income_contract

logger = logging.getLogger(__name__)


@dataclass
class IncomeTrade:
    ticker:          str
    strategy:        str  # 'INCOME'
    option_type:     str  # 'PUT'
    direction:       str  # 'short'
    strike:          float
    expiry:          str
    dte:             int
    premium:         float   # credit received per share
    delta:           float
    theta:           float
    gamma:           float
    vega:            float
    pop:             float   # probability of profit (0-1)
    expected_return: float   # (premium / max_risk) * pop
    iv_rank_proxy:   int     # 0-100
    spread_pct:      float
    earnings_risk:   bool
    stock_price:     float
    max_risk:        float   # per share = strike - premium
    scanned_at:      str

    def to_dict(self) -> dict:
        return asdict(self)


# ── Filters ───────────────────────────────────────────────────────────────────

_MIN_IV_RANK      = 50     # IV must be elevated for premium selling
_MIN_AVG_VOLUME   = 500_000
_MIN_OI           = 500
_MIN_PRICE        = 15.0   # avoid penny stocks
_MIN_PREMIUM      = 0.30   # min credit per share
_MIN_POP          = 0.65


def _passes_stock_filter(snap: dict, min_iv_rank: int = _MIN_IV_RANK, min_avg_volume: int = _MIN_AVG_VOLUME) -> bool:
    if snap['price'] < _MIN_PRICE:
        return False
    if snap['avg_volume_20d'] < min_avg_volume:
        return False
    if snap['iv_rank_proxy'] < min_iv_rank:
        return False
    if snap.get('above_ma50') is False:
        return False
    return True


# ── Main scan ─────────────────────────────────────────────────────────────────

def scan_income(
    tickers: list[str],
    config: dict | None = None,
) -> list[IncomeTrade]:
    """
    Scan tickers for high-probability income (short put) candidates.
    Returns list of IncomeTrade sorted by expected_return descending.
    """
    cfg = config or {}
    min_iv_rank    = int(cfg.get('min_iv_rank',    _MIN_IV_RANK))
    min_avg_volume = int(cfg.get('min_avg_volume', _MIN_AVG_VOLUME))
    min_oi         = int(cfg.get('min_oi',         _MIN_OI))
    min_premium    = float(cfg.get('min_premium',  _MIN_PREMIUM))
    min_pop        = float(cfg.get('min_pop',       _MIN_POP))
    dte_min        = int(cfg.get('dte_min',         30))
    dte_max        = int(cfg.get('dte_max',         45))

    candidates: list[IncomeTrade] = []

    for ticker in tickers:
        try:
            snap = fetch_stock_snapshot(ticker)
            if not snap:
                continue
            if not _passes_stock_filter(snap, min_iv_rank=min_iv_rank, min_avg_volume=min_avg_volume):
                logger.debug(f'income_engine: {ticker} failed stock filter (iv_rank={snap.get("iv_rank_proxy")} vol={snap.get("avg_volume_20d")})')
                continue

            chain = fetch_options_chain(ticker, min_dte=dte_min, max_dte=dte_max)
            if not chain or chain['puts'].empty:
                continue

            puts = chain['puts'].copy()

            # Only keep contracts with active market quotes (bid > 0 or ask > 0)
            # Contracts with bid=ask=0 have stale/unreliable IV, making greeks useless
            puts = puts[(puts['bid'] > 0) | (puts['ask'] > 0)]
            if puts.empty:
                continue

            enrich_chain_with_greeks(puts, snap['price'])
            enrich_chain_with_pop(puts, mode='income')

            contract = select_income_contract(puts)
            if contract is None:
                continue

            premium = float(contract.get('mid') or contract.get('lastPrice') or 0)
            if premium < min_premium:
                continue

            pop = float(contract.get('pop', 0))
            if pop < min_pop:
                continue

            strike   = float(contract['strike'])
            max_risk = max(strike - premium, 0.01)

            expiry_date = date.fromisoformat(contract['expiry'])
            if has_earnings_within(ticker, expiry_date):
                logger.info(f'income_engine: {ticker} skipped — earnings within DTE ({contract["expiry"]})')
                continue

            trade = IncomeTrade(
                ticker          = ticker,
                strategy        = 'INCOME',
                option_type     = 'PUT',
                direction       = 'short',
                strike          = strike,
                expiry          = contract['expiry'],
                dte             = int(contract['dte']),
                premium         = round(premium, 4),
                delta           = float(contract.get('delta', 0)),
                theta           = float(contract.get('theta', 0)),
                gamma           = float(contract.get('gamma', 0)),
                vega            = float(contract.get('vega', 0)),
                pop             = pop,
                expected_return = float(contract.get('expected_return', 0)),
                iv_rank_proxy   = snap['iv_rank_proxy'],
                spread_pct      = float(contract.get('spread_pct', 0)),
                earnings_risk   = False,
                stock_price     = snap['price'],
                max_risk        = round(max_risk, 4),
                scanned_at      = datetime.utcnow().isoformat(),
            )
            candidates.append(trade)
            logger.info(
                f'income_engine: {ticker} PUT {strike} exp={contract["expiry"]} '
                f'prem={premium} delta={trade.delta:.2f} POP={pop:.0%}'
            )

        except Exception as e:
            logger.warning(f'income_engine: error on {ticker}: {e}')

    candidates.sort(key=lambda t: t.expected_return, reverse=True)
    return candidates
