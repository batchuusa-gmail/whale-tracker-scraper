"""
APEX-60 — Probability of Profit, Expected Return, and bid-ask spread analyzer.
"""

# Bid-ask spread threshold above which a contract is flagged ILLIQUID
_SPREAD_PCT_THRESHOLD = 0.10  # 10%


def calc_pop(delta: float) -> float:
    """
    POP for short options = 1 - abs(delta).
    Simplified method — accurate enough for delta 0.15–0.35 range.
    """
    return round(max(0.0, min(1.0, 1.0 - abs(delta))), 4)


def calc_expected_return(premium: float, max_risk: float, pop: float) -> float:
    """
    Expected return = (premium / max_risk) * POP.
    max_risk for a cash-secured short put = strike * 100 - premium * 100.
    Returns as a fraction (e.g. 0.045 = 4.5%).
    """
    if max_risk <= 0:
        return 0.0
    return round((premium / max_risk) * pop, 4)


def calc_spread_analysis(bid: float, ask: float) -> dict:
    """
    Returns spread, mid, spread_pct, and ILLIQUID flag.
    """
    bid = bid or 0.0
    ask = ask or 0.0
    mid    = round((bid + ask) / 2, 4)
    spread = round(ask - bid, 4)
    spread_pct = round(spread / mid, 4) if mid > 0 else 1.0
    return {
        'spread':     spread,
        'mid':        mid,
        'spread_pct': spread_pct,
        'illiquid':   spread_pct > _SPREAD_PCT_THRESHOLD,
    }


def enrich_chain_with_pop(df, mode: str = 'income') -> None:
    """
    Add pop, expected_return, spread_pct, illiquid columns to a chain DataFrame in-place.
    mode = 'income' (short put) or 'growth' (long call/put).
    """
    pops, exp_rets, spread_pcts, illiquids = [], [], [], []

    for _, row in df.iterrows():
        delta   = float(row.get('delta', 0) or 0)
        bid     = float(row.get('bid', 0)   or 0)
        ask     = float(row.get('ask', 0)   or 0)
        strike  = float(row.get('strike', 0)or 0)
        premium = float(row.get('mid', 0)   or row.get('lastPrice', 0) or 0)

        pop = calc_pop(delta)
        spread_info = calc_spread_analysis(bid, ask)

        if mode == 'income':
            # Max risk = strike - premium per share (cash-secured put)
            max_risk = max(strike - premium, 0.01)
            exp_ret  = calc_expected_return(premium, max_risk, pop)
        else:
            # Long option: max loss = premium paid
            exp_ret = 0.0  # growth engine uses directional upside, not EV formula

        pops.append(pop)
        exp_rets.append(exp_ret)
        spread_pcts.append(spread_info['spread_pct'])
        illiquids.append(spread_info['illiquid'])

    df['pop']             = pops
    df['expected_return'] = exp_rets
    df['spread_pct']      = spread_pcts
    df['illiquid']        = illiquids
