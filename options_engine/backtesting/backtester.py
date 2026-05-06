"""
APEX-72 — Backtesting module: historical replay with PnL tracking.

Usage:
  python -m options_engine.backtesting.backtester \\
      --tickers AAPL MSFT TSLA \\
      --start-date 2024-01-01 \\
      --end-date   2024-06-30 \\
      --strategy   income      \\
      --output     backtest_results.csv

NOTE: yfinance historical options data is limited and unreliable.
Expiry chain snapshots are not available historically via yfinance.
This backtester uses OHLCV price data + synthetic options pricing
(Black-Scholes at time of signal) to simulate P&L at expiration
(intrinsic value method). Results are approximate — not production.
"""
import argparse
import csv
import logging
import sys
from dataclasses import dataclass, fields, asdict
from datetime import date, timedelta

import numpy as np
import yfinance as yf

from options_engine.analytics.greeks import compute_greeks

logger = logging.getLogger(__name__)


@dataclass
class BacktestResult:
    ticker:       str
    strategy:     str
    signal_date:  str
    option_type:  str
    direction:    str
    strike:       float
    expiry:       str
    dte_at_entry: int
    premium:      float
    entry_price:  float   # stock price at signal
    exit_price:   float   # stock price at expiration
    intrinsic:    float   # option value at expiration
    pnl:          float   # per-share P&L
    pnl_pct:      float   # P&L as % of max_risk (income) or premium (growth)
    outcome:      str     # WIN / LOSS / BREAK_EVEN
    score:        int     # signal score at time of entry (if available)


def _fetch_price_history(ticker: str, start: str, end: str) -> dict[str, float]:
    """Returns {date_str: close_price} for date range."""
    try:
        hist = yf.Ticker(ticker).history(start=start, end=end, auto_adjust=True)
        return {str(d.date()): float(c) for d, c in zip(hist.index, hist['Close'])}
    except Exception as e:
        logger.warning(f'backtester: price history {ticker}: {e}')
        return {}


def _calc_hv(closes: list) -> float:
    """20-day annualised historical volatility."""
    if len(closes) < 21:
        return 0.25
    log_rets = np.log(np.array(closes[-21:]) / np.array(closes[-22:-1]))
    return float(np.std(log_rets) * np.sqrt(252))


def _synthetic_premium(
    option_type: str, spot: float, strike: float, dte: int, hv: float,
) -> float:
    """
    Estimate fair option premium via Black-Scholes at signal date.
    Used because historical bid/ask is unavailable via yfinance.
    """
    from scipy.stats import norm
    import math
    T = max(dte, 1) / 365
    r = 0.0525
    sigma = max(hv, 0.05)
    d1 = (math.log(spot / strike) + (r + 0.5 * sigma**2) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    if option_type == 'call':
        return max(0, spot * norm.cdf(d1) - strike * math.exp(-r * T) * norm.cdf(d2))
    else:
        return max(0, strike * math.exp(-r * T) * norm.cdf(-d2) - spot * norm.cdf(-d1))


def _intrinsic_at_expiry(option_type: str, strike: float, expiry_price: float) -> float:
    if option_type == 'call':
        return max(0.0, expiry_price - strike)
    else:
        return max(0.0, strike - expiry_price)


def _simulate_income_signal(
    ticker: str, signal_date: str, prices: dict[str, float], closes_up_to_signal: list,
) -> BacktestResult | None:
    """Short put: sell ATM-ish put, hold to expiration, PnL = premium - intrinsic at expiry."""
    spot = prices.get(signal_date)
    if not spot:
        return None

    hv     = _calc_hv(closes_up_to_signal)
    dte    = 35
    strike = round(spot * 0.95, 0)  # ~5% OTM put (delta ~-0.25)
    premium = _synthetic_premium('put', spot, strike, dte, hv)

    # Expiration date
    exp_date = (date.fromisoformat(signal_date) + timedelta(days=dte)).isoformat()
    exp_price = prices.get(exp_date)
    if not exp_price:
        # Find nearest available date within ±3 days
        for offset in range(1, 5):
            for d in [date.fromisoformat(exp_date) + timedelta(days=o) for o in [offset, -offset]]:
                exp_price = prices.get(str(d))
                if exp_price:
                    exp_date = str(d)
                    break
            if exp_price:
                break
    if not exp_price:
        return None

    intrinsic = _intrinsic_at_expiry('put', strike, exp_price)
    pnl       = round(premium - intrinsic, 4)
    max_risk  = max(strike - premium, 0.01)
    pnl_pct   = round(pnl / max_risk * 100, 2)
    outcome   = 'WIN' if pnl > 0.01 else ('LOSS' if pnl < -0.01 else 'BREAK_EVEN')

    return BacktestResult(
        ticker=ticker, strategy='INCOME', signal_date=signal_date,
        option_type='put', direction='short',
        strike=strike, expiry=exp_date, dte_at_entry=dte,
        premium=round(premium, 4), entry_price=spot, exit_price=exp_price,
        intrinsic=round(intrinsic, 4), pnl=pnl, pnl_pct=pnl_pct,
        outcome=outcome, score=0,
    )


def _simulate_growth_signal(
    ticker: str, signal_date: str, direction: str,
    prices: dict[str, float], closes_up_to_signal: list,
) -> BacktestResult | None:
    """Long call/put: buy ATM option, hold to expiration, PnL = intrinsic - premium."""
    spot = prices.get(signal_date)
    if not spot:
        return None

    hv     = _calc_hv(closes_up_to_signal)
    dte    = 30
    option_type = 'call' if direction == 'bullish' else 'put'
    strike = round(spot, 0)  # ATM
    premium = _synthetic_premium(option_type, spot, strike, dte, hv)

    exp_date = (date.fromisoformat(signal_date) + timedelta(days=dte)).isoformat()
    exp_price = prices.get(exp_date)
    if not exp_price:
        for offset in range(1, 5):
            for d in [date.fromisoformat(exp_date) + timedelta(days=o) for o in [offset, -offset]]:
                exp_price = prices.get(str(d))
                if exp_price:
                    exp_date = str(d)
                    break
            if exp_price:
                break
    if not exp_price:
        return None

    intrinsic = _intrinsic_at_expiry(option_type, strike, exp_price)
    pnl       = round(intrinsic - premium, 4)
    pnl_pct   = round(pnl / max(premium, 0.01) * 100, 2)
    outcome   = 'WIN' if pnl > 0.01 else ('LOSS' if pnl < -0.01 else 'BREAK_EVEN')

    return BacktestResult(
        ticker=ticker, strategy='GROWTH', signal_date=signal_date,
        option_type=option_type, direction=direction,
        strike=strike, expiry=exp_date, dte_at_entry=dte,
        premium=round(premium, 4), entry_price=spot, exit_price=exp_price,
        intrinsic=round(intrinsic, 4), pnl=pnl, pnl_pct=pnl_pct,
        outcome=outcome, score=0,
    )


def run_backtest(
    tickers: list[str],
    start_date: str,
    end_date: str,
    strategy: str = 'income',  # 'income' or 'growth'
    output_path: str = 'backtest_results.csv',
) -> list[BacktestResult]:
    """
    Replay scanner over historical date range and compute simulated PnL.
    Fires a signal on every Monday (weekly scan cadence) in range.
    """
    results: list[BacktestResult] = []

    for ticker in tickers:
        logger.info(f'backtester: running {ticker} {strategy} {start_date}→{end_date}')
        prices = _fetch_price_history(
            ticker,
            start=(date.fromisoformat(start_date) - timedelta(days=60)).isoformat(),
            end=end_date,
        )
        if not prices:
            continue

        sorted_dates = sorted(prices.keys())

        # Iterate over Mondays in range
        cur = date.fromisoformat(start_date)
        end = date.fromisoformat(end_date)

        while cur <= end:
            if cur.weekday() == 0:  # Monday
                signal_date = str(cur)
                closes_up_to_signal = [prices[d] for d in sorted_dates if d <= signal_date]

                try:
                    if strategy == 'income':
                        result = _simulate_income_signal(ticker, signal_date, prices, closes_up_to_signal)
                    else:
                        # Simple direction: bullish if price above 50d MA
                        if len(closes_up_to_signal) >= 50:
                            ma50 = float(np.mean(closes_up_to_signal[-50:]))
                            direction = 'bullish' if closes_up_to_signal[-1] > ma50 else 'bearish'
                        else:
                            direction = 'bullish'
                        result = _simulate_growth_signal(ticker, signal_date, direction, prices, closes_up_to_signal)

                    if result:
                        results.append(result)
                except Exception as e:
                    logger.warning(f'backtester: {ticker} {signal_date}: {e}')

            cur += timedelta(days=1)

    # Write CSV
    if results:
        fieldnames = [f.name for f in fields(BacktestResult)]
        with open(output_path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for r in results:
                writer.writerow(asdict(r))

        wins   = sum(1 for r in results if r.outcome == 'WIN')
        losses = sum(1 for r in results if r.outcome == 'LOSS')
        total_pnl = sum(r.pnl for r in results)
        win_rate  = wins / len(results) * 100 if results else 0
        logger.info(
            f'backtester: {len(results)} trades | win rate {win_rate:.1f}% | '
            f'total PnL/share ${total_pnl:.2f} | saved → {output_path}'
        )

    return results


def main():
    parser = argparse.ArgumentParser(description='Options Engine Backtester')
    parser.add_argument('--tickers',    nargs='+', required=True)
    parser.add_argument('--start-date', required=True)
    parser.add_argument('--end-date',   required=True)
    parser.add_argument('--strategy',   choices=['income', 'growth'], default='income')
    parser.add_argument('--output',     default='backtest_results.csv')
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format='%(levelname)s %(message)s')
    results = run_backtest(
        tickers    = args.tickers,
        start_date = args.start_date,
        end_date   = args.end_date,
        strategy   = args.strategy,
        output_path= args.output,
    )
    print(f'\n{len(results)} backtest trades written to {args.output}')

    if results:
        wins     = sum(1 for r in results if r.outcome == 'WIN')
        total_pnl = sum(r.pnl for r in results)
        print(f'Win rate: {wins / len(results) * 100:.1f}%')
        print(f'Total PnL/share: ${total_pnl:.2f}')
        avg_pnl_pct = sum(r.pnl_pct for r in results) / len(results)
        print(f'Avg PnL %: {avg_pnl_pct:.1f}%')


if __name__ == '__main__':
    main()
