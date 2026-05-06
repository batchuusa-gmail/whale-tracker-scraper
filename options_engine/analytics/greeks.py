"""
APEX-59 — Black-Scholes Greeks calculator.
Tries py_vollib first; falls back to a pure-Python BS implementation
if the library is unavailable or throws.
"""
import logging
import math
from scipy.stats import norm

logger = logging.getLogger(__name__)

_RISK_FREE_RATE = 0.0525  # 5.25% — update via config if needed

try:
    from py_vollib.black_scholes import black_scholes as _bs_price
    from py_vollib.black_scholes.greeks.analytical import (
        delta as _bs_delta,
        theta as _bs_theta,
        gamma as _bs_gamma,
        vega  as _bs_vega,
    )
    _PY_VOLLIB = True
    logger.info('greeks: using py_vollib')
except ImportError:
    _PY_VOLLIB = False
    logger.info('greeks: py_vollib not available, using built-in Black-Scholes')


# ── Pure-Python fallback ──────────────────────────────────────────────────────

def _d1(S, K, T, r, sigma):
    return (math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))


def _bs_greeks_fallback(flag: str, S: float, K: float, T: float, r: float, sigma: float) -> dict:
    """Black-Scholes greeks via scipy norm. flag = 'c' or 'p'."""
    if T <= 0 or sigma <= 0 or S <= 0 or K <= 0:
        return {'delta': 0.0, 'theta': 0.0, 'gamma': 0.0, 'vega': 0.0}

    d1 = _d1(S, K, T, r, sigma)
    d2 = d1 - sigma * math.sqrt(T)

    if flag == 'c':
        delta = norm.cdf(d1)
        theta = (
            -(S * norm.pdf(d1) * sigma) / (2 * math.sqrt(T))
            - r * K * math.exp(-r * T) * norm.cdf(d2)
        ) / 365
    else:
        delta = norm.cdf(d1) - 1
        theta = (
            -(S * norm.pdf(d1) * sigma) / (2 * math.sqrt(T))
            + r * K * math.exp(-r * T) * norm.cdf(-d2)
        ) / 365

    gamma = norm.pdf(d1) / (S * sigma * math.sqrt(T))
    vega  = S * norm.pdf(d1) * math.sqrt(T) / 100  # per 1% IV move

    return {
        'delta': round(delta, 4),
        'theta': round(theta, 4),
        'gamma': round(gamma, 6),
        'vega':  round(vega, 4),
    }


# ── Public API ────────────────────────────────────────────────────────────────

def compute_greeks(
    option_type: str,   # 'call' or 'put'
    spot: float,
    strike: float,
    dte: int,
    implied_vol: float,
    risk_free_rate: float = _RISK_FREE_RATE,
) -> dict:
    """
    Compute Delta, Theta, Gamma, Vega for a single contract.

    Returns dict with keys: delta, theta, gamma, vega, used_fallback
    """
    flag = 'c' if option_type == 'call' else 'p'
    T    = max(dte, 1) / 365.0
    sigma = max(implied_vol, 0.001)

    if _PY_VOLLIB:
        try:
            return {
                'delta':        round(float(_bs_delta(flag, spot, strike, T, risk_free_rate, sigma)), 4),
                'theta':        round(float(_bs_theta(flag, spot, strike, T, risk_free_rate, sigma)), 4),
                'gamma':        round(float(_bs_gamma(flag, spot, strike, T, risk_free_rate, sigma)), 6),
                'vega':         round(float(_bs_vega( flag, spot, strike, T, risk_free_rate, sigma)), 4),
                'used_fallback': False,
            }
        except Exception as e:
            logger.debug(f'py_vollib failed ({e}), falling back')

    result = _bs_greeks_fallback(flag, spot, strike, T, risk_free_rate, sigma)
    result['used_fallback'] = True
    return result


def enrich_chain_with_greeks(df, spot: float) -> None:
    """Add delta/theta/gamma/vega columns to an options chain DataFrame in-place."""
    import pandas as pd

    greeks_rows = []
    for _, row in df.iterrows():
        g = compute_greeks(
            option_type  = row['option_type'],
            spot         = spot,
            strike       = float(row['strike']),
            dte          = int(row['dte']),
            implied_vol  = float(row['impliedVolatility']),
        )
        greeks_rows.append(g)

    gdf = pd.DataFrame(greeks_rows)
    for col in ['delta', 'theta', 'gamma', 'vega']:
        df[col] = gdf[col].values
