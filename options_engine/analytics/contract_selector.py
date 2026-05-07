"""
APEX-61 — Optimal contract selector for Income and Growth modes.
Given a filtered options chain DataFrame, finds the single best-fit contract.
"""
import logging
import pandas as pd

logger = logging.getLogger(__name__)


def select_income_contract(puts_df: pd.DataFrame) -> pd.Series | None:
    """
    Income mode — find best short put:
      delta closest to -0.25, within [-0.30, -0.20], DTE 30-45.
    Excludes illiquid contracts (spread_pct > 10%).
    Returns single best contract row or None.
    """
    if puts_df is None or puts_df.empty:
        return None

    df = puts_df.copy()

    # Require computed greeks
    if 'delta' not in df.columns:
        logger.warning('contract_selector: delta column missing — run enrich_chain_with_greeks first')
        return None

    # DTE filter 30-45
    df = df[(df['dte'] >= 30) & (df['dte'] <= 45)]

    # Delta filter [-0.30, -0.20]
    df = df[(df['delta'] >= -0.30) & (df['delta'] <= -0.20)]

    # Exclude illiquid
    if 'illiquid' in df.columns:
        df = df[~df['illiquid']]

    if df.empty:
        return None

    # Pick contract whose delta is closest to target -0.25
    df = df.copy()
    df['_dist'] = (df['delta'] - (-0.25)).abs()
    best = df.sort_values('_dist').iloc[0]
    return best.drop(labels=['_dist'])


def select_growth_contract(
    chain_df: pd.DataFrame,
    direction: str,   # 'bullish' or 'bearish'
) -> pd.Series | None:
    """
    Growth mode — find best ATM/slightly-ITM directional contract:
      Calls for bullish: delta closest to +0.50, DTE 20-60.
      Puts for bearish:  delta closest to -0.50, DTE 20-60.
    Excludes illiquid contracts.
    Returns single best contract row or None.
    """
    if chain_df is None or chain_df.empty:
        return None

    df = chain_df.copy()

    if 'delta' not in df.columns:
        logger.warning('contract_selector: delta column missing — run enrich_chain_with_greeks first')
        return None

    # Filter to correct option type
    if direction == 'bullish':
        df = df[df['option_type'] == 'call']
        target_delta = 0.50
    else:
        df = df[df['option_type'] == 'put']
        target_delta = -0.50

    # DTE filter 20-60
    df = df[(df['dte'] >= 20) & (df['dte'] <= 60)]

    # Exclude illiquid
    if 'illiquid' in df.columns:
        df = df[~df['illiquid']]

    if df.empty:
        return None

    df = df.copy()
    df['_dist'] = (df['delta'] - target_delta).abs()
    best = df.sort_values('_dist').iloc[0]
    return best.drop(labels=['_dist'])
