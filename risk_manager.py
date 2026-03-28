"""
WHAL-77 — Risk Management Engine
Validates insider trade signals before they reach the user.
"""

# ── Risk thresholds ──────────────────────────────────────────────
MIN_CONFIDENCE       = 85       # reject signals below this
MIN_TRADE_VALUE      = 500_000  # USD — skip small trades
MIN_DAYS_TO_EARNINGS = 2        # no trades 2 days before earnings
MIN_STOCK_PRICE      = 5.0      # skip penny stocks
MAX_POSITION_PCT     = 0.05     # 5% of portfolio per trade
DEFAULT_STOP_LOSS_PCT = 0.10    # 10% stop loss
DEFAULT_TARGET_PCT   = 0.20     # 20% target gain


class RiskViolation(Exception):
    """Raised when a trade fails a risk rule."""
    def __init__(self, rule: str, detail: str):
        self.rule = rule
        self.detail = detail
        super().__init__(detail)


def check_all(
    ticker: str,
    trade_value: float,
    current_price: float,
    confidence: int,
    days_to_earnings: int = 999,
    portfolio_value: float = 100_000,
) -> dict:
    """
    Run all risk checks. Returns a dict with:
      passed       : bool
      violations   : list of {rule, detail}
      position_size: max USD to deploy (0 if failed)
      max_shares   : int based on position_size / current_price
    """
    violations = []

    # Rule 1 — confidence gate
    if confidence < MIN_CONFIDENCE:
        violations.append({
            'rule': 'LOW_CONFIDENCE',
            'detail': f'Signal confidence {confidence}% is below minimum {MIN_CONFIDENCE}%',
        })

    # Rule 2 — minimum trade value
    if trade_value < MIN_TRADE_VALUE:
        violations.append({
            'rule': 'SMALL_TRADE',
            'detail': f'Trade value ${trade_value:,.0f} is below minimum ${MIN_TRADE_VALUE:,.0f}',
        })

    # Rule 3 — earnings proximity
    if 0 <= days_to_earnings < MIN_DAYS_TO_EARNINGS:
        violations.append({
            'rule': 'EARNINGS_PROXIMITY',
            'detail': f'Trade is {days_to_earnings} day(s) from earnings — minimum gap is {MIN_DAYS_TO_EARNINGS} days',
        })

    # Rule 4 — penny stock filter
    if current_price > 0 and current_price < MIN_STOCK_PRICE:
        violations.append({
            'rule': 'PENNY_STOCK',
            'detail': f'{ticker} price ${current_price:.2f} is below penny stock threshold ${MIN_STOCK_PRICE:.2f}',
        })

    passed = len(violations) == 0

    # Position sizing — only meaningful if passed
    position_size = round(portfolio_value * MAX_POSITION_PCT, 2) if passed else 0.0
    max_shares = int(position_size / current_price) if (passed and current_price > 0) else 0

    return {
        'passed': passed,
        'violations': violations,
        'position_size': position_size,
        'max_shares': max_shares,
        'rules_applied': {
            'min_confidence':       MIN_CONFIDENCE,
            'min_trade_value':      MIN_TRADE_VALUE,
            'min_days_to_earnings': MIN_DAYS_TO_EARNINGS,
            'min_stock_price':      MIN_STOCK_PRICE,
            'max_position_pct':     MAX_POSITION_PCT,
        },
    }


def apply_price_levels(current_price: float, is_buy: bool = True) -> dict:
    """
    Calculate default stop loss and target from risk rules.
    Returns: {stop_loss, target_price}
    """
    if current_price <= 0:
        return {'stop_loss': None, 'target_price': None}
    if is_buy:
        stop  = round(current_price * (1 - DEFAULT_STOP_LOSS_PCT), 2)
        target = round(current_price * (1 + DEFAULT_TARGET_PCT), 2)
    else:
        # For sells/shorts — reverse logic
        stop  = round(current_price * (1 + DEFAULT_STOP_LOSS_PCT), 2)
        target = round(current_price * (1 - DEFAULT_TARGET_PCT), 2)
    return {'stop_loss': stop, 'target_price': target}
