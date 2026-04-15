"""
WHAL-101 — Options Flow Conviction Scorer

6-factor conviction score (0-100) on each options contract:
  Factor 1 (+20) — Volume > 5× open interest   (institutional size)
  Factor 2 (+20) — Unusual vs average vol (>3×) (sweep-like activity)
  Factor 3 (+15) — OTM > 3% from spot           (directional intent)
  Factor 4 (+15) — DTE < 30 days                (urgency)
  Factor 5 (+15) — Large premium (>$500k)        (smart money size)
  Factor 6 (+15) — Insider alignment             (same-direction insider buy in last 30d)

Score 75+  = VERY HIGH conviction
Score 50+  = HIGH conviction
Score 30+  = MODERATE conviction
Below 30   = routine flow

Data: yfinance options chains (free) + Supabase insider data
"""

import os
import logging
import threading
from datetime import datetime, timedelta, date

import requests
import yfinance as yf

logger = logging.getLogger(__name__)

# Scan universe: broad market leaders + high-options-volume names
SCAN_UNIVERSE = [
    # Mega-cap tech
    'AAPL', 'MSFT', 'NVDA', 'AMZN', 'GOOGL', 'META', 'TSLA', 'AVGO', 'AMD',
    # Financials
    'JPM', 'GS', 'MS', 'BAC', 'V', 'MA', 'AXP', 'SCHW', 'C',
    # High-options-volume momentum names
    'PLTR', 'COIN', 'HOOD', 'RIVN', 'SNAP', 'RBLX', 'UBER', 'DASH', 'SOFI', 'SQ',
    'CRWD', 'PANW', 'NET', 'DDOG', 'SNOW', 'MDB', 'ZM', 'SPOT', 'ABNB', 'SHOP',
    # ETFs (always high-conviction when unusual)
    'SPY', 'QQQ', 'IWM', 'XLF', 'XLK', 'SOXS', 'SOXL', 'TQQQ', 'SQQQ',
    # Healthcare / biotech
    'LLY', 'MRNA', 'NVAX', 'GILD', 'REGN', 'AMGN', 'PFE',
    # Energy
    'XOM', 'CVX', 'SLB', 'OXY',
    # Semis
    'MU', 'INTC', 'QCOM', 'TXN', 'AMAT', 'LRCX', 'MRVL',
]

# Supabase for insider data
_SUPA_URL = os.environ.get('SUPABASE_URL', 'https://bedurjtazsfbnkisoeee.supabase.co')
_SUPA_KEY = os.environ.get(
    'SUPABASE_KEY',
    'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJlZHVyanRhenNmYm5raXNvZWVlIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzM2NDcxOTUsImV4cCI6MjA4OTIyMzE5NX0.MK4N9dxAIHlXkGP4rLJPq5tHh9UU8L75EB1b8Q7CVmg'
)

_state_lock = threading.Lock()
_state = {
    'flow':         [],
    'last_updated': None,
    'error':        None,
}


# ── Insider alignment helper ─────────────────────────────────────────

def _fetch_insider_buys() -> set[str]:
    """Return tickers with insider BUY in last 30 days."""
    try:
        cutoff = (datetime.utcnow() - timedelta(days=30)).strftime('%Y-%m-%d')
        r = requests.get(
            f'{_SUPA_URL}/rest/v1/filings',
            headers={'apikey': _SUPA_KEY, 'Authorization': f'Bearer {_SUPA_KEY}'},
            params={
                'select':           'ticker,transaction_type',
                'transaction_date': f'gte.{cutoff}',
                'transaction_type': 'eq.Buy',
                'limit':            '500',
            },
            timeout=10,
        )
        if r.status_code == 200:
            return {row['ticker'].upper() for row in r.json() if row.get('ticker')}
    except Exception as e:
        logger.warning(f'insider_buys fetch error: {e}')
    return set()


# ── DTE helper ───────────────────────────────────────────────────────

def _days_to_expiry(expiry_str: str) -> int | None:
    try:
        exp = datetime.strptime(expiry_str, '%Y-%m-%d').date()
        return max(0, (exp - date.today()).days)
    except Exception:
        return None


# ── 6-factor scorer ──────────────────────────────────────────────────

def _score_contract(
    ticker: str,
    opt_type: str,          # 'CALL' or 'PUT'
    strike: float,
    expiry: str,
    volume: int,
    open_interest: int,
    last_price: float,
    avg_vol: float,
    spot: float,
    insider_buys: set[str],
) -> dict:
    """
    Score a single options contract 0-100 across 6 factors.
    Returns full scored contract dict.
    """
    score   = 0
    factors = []
    premium = round(last_price * volume * 100, 0)

    oi = max(open_interest, 1)

    # Factor 1: Volume > 5× OI (+20)
    vol_oi_ratio = volume / oi
    if vol_oi_ratio >= 5.0:
        score += 20
        factors.append(f'VOL/OI {vol_oi_ratio:.1f}×')
    elif vol_oi_ratio >= 2.0:
        score += 10
        factors.append(f'VOL/OI {vol_oi_ratio:.1f}×')

    # Factor 2: Unusual vs average volume (>3×) (+20)
    if avg_vol > 0:
        avg_ratio = volume / avg_vol
        if avg_ratio >= 5.0:
            score += 20
            factors.append(f'SWEEP {avg_ratio:.0f}×avg')
        elif avg_ratio >= 3.0:
            score += 12
            factors.append(f'UNUSUAL {avg_ratio:.1f}×avg')

    # Factor 3: OTM > 3% from spot (+15)
    otm_pct = 0.0
    if spot > 0:
        if opt_type == 'CALL':
            otm_pct = (strike - spot) / spot * 100
        else:
            otm_pct = (spot - strike) / spot * 100

    if otm_pct >= 10.0:
        score += 15
        factors.append(f'DEEP OTM {otm_pct:.0f}%')
    elif otm_pct >= 3.0:
        score += 10
        factors.append(f'OTM {otm_pct:.1f}%')

    # Factor 4: DTE < 30 days (+15)
    dte = _days_to_expiry(expiry)
    if dte is not None:
        if dte <= 7:
            score += 15
            factors.append(f'{dte}DTE URGENT')
        elif dte <= 14:
            score += 12
            factors.append(f'{dte}DTE')
        elif dte <= 30:
            score += 8
            factors.append(f'{dte}DTE')

    # Factor 5: Large premium >$500k (+15)
    if premium >= 5_000_000:
        score += 15
        factors.append(f'${premium/1e6:.1f}M PREM')
    elif premium >= 1_000_000:
        score += 10
        factors.append(f'${premium/1e6:.1f}M PREM')
    elif premium >= 500_000:
        score += 5
        factors.append(f'${premium/1000:.0f}K PREM')

    # Factor 6: Insider alignment (+15)
    # CALL + insider buy = aligned; PUT + no insider buy but bearish context = aligned
    if opt_type == 'CALL' and ticker in insider_buys:
        score += 15
        factors.append('INSIDER ALIGNED')

    # Direction implied by contract type
    direction = 'BULLISH' if opt_type == 'CALL' else 'BEARISH'

    return {
        'ticker':        ticker,
        'type':          opt_type,
        'direction':     direction,
        'strike':        round(strike, 2),
        'spot':          round(spot, 2),
        'expiry':        expiry,
        'dte':           dte,
        'volume':        volume,
        'open_interest': open_interest,
        'vol_oi_ratio':  round(vol_oi_ratio, 2),
        'otm_pct':       round(otm_pct, 1),
        'premium':       premium,
        'last_price':    round(last_price, 2),
        'conviction':    min(score, 100),
        'factors':       factors,
        'unusual':       vol_oi_ratio >= 2.0 or (avg_vol > 0 and volume / avg_vol >= 3.0),
        'insider_aligned': opt_type == 'CALL' and ticker in insider_buys,
        'scanned_at':    datetime.utcnow().isoformat(),
    }


# ── Per-ticker scanner ───────────────────────────────────────────────

def _scan_ticker(ticker: str, insider_buys: set[str]) -> list[dict]:
    """Scan one ticker's options chain and return scored contracts."""
    results = []
    try:
        t    = yf.Ticker(ticker)
        info = t.fast_info
        spot = float(getattr(info, 'last_price', None) or 0)
        if spot <= 0:
            return []

        exps = t.options
        if not exps:
            return []

        # Scan next 3 expiries only
        for exp in exps[:3]:
            try:
                chain = t.option_chain(exp)

                calls = chain.calls if chain.calls is not None and len(chain.calls) > 0 else None
                puts  = chain.puts  if chain.puts  is not None and len(chain.puts)  > 0 else None

                for df, opt_type in [(calls, 'CALL'), (puts, 'PUT')]:
                    if df is None:
                        continue
                    avg_vol = float(df['volume'].replace(0, float('nan')).mean() or 1)
                    for _, row in df.iterrows():
                        vol = int(row.get('volume') or 0)
                        oi  = int(row.get('openInterest') or 0)
                        lp  = float(row.get('lastPrice') or 0)
                        st  = float(row.get('strike') or 0)
                        if vol < 50 or lp <= 0 or st <= 0:
                            continue
                        scored = _score_contract(
                            ticker=ticker,
                            opt_type=opt_type,
                            strike=st,
                            expiry=exp,
                            volume=vol,
                            open_interest=oi,
                            last_price=lp,
                            avg_vol=avg_vol,
                            spot=spot,
                            insider_buys=insider_buys,
                        )
                        if scored['conviction'] >= 30:
                            results.append(scored)
            except Exception as e:
                logger.debug(f'options chain {ticker} {exp}: {e}')

    except Exception as e:
        logger.warning(f'scan_ticker {ticker}: {e}')

    return results


# ── Full scan ────────────────────────────────────────────────────────

def scan_conviction_flow(extra_tickers: list[str] = None) -> list[dict]:
    """
    Scan SCAN_UNIVERSE + insider tickers, score all contracts,
    return top 60 sorted by conviction desc.
    """
    insider_buys = _fetch_insider_buys()
    universe     = list(dict.fromkeys(SCAN_UNIVERSE + (extra_tickers or []) + list(insider_buys)))
    universe     = universe[:50]   # cap to keep runtime reasonable

    all_results = []
    for ticker in universe:
        contracts = _scan_ticker(ticker, insider_buys)
        all_results.extend(contracts)

    # Sort: conviction desc, then premium desc
    all_results.sort(key=lambda x: (-x['conviction'], -x['premium']))
    return all_results[:60]


def get_conviction_flow(redis_get=None, redis_set=None) -> list[dict]:
    """Return cached conviction flow, refreshing if stale (>15 min)."""
    cache_key = 'options:conviction'
    if redis_get:
        cached = redis_get(cache_key)
        if cached:
            return cached

    with _state_lock:
        flow         = _state['flow']
        last_updated = _state['last_updated']

    stale = True
    if last_updated:
        try:
            age   = (datetime.utcnow() - datetime.fromisoformat(last_updated)).total_seconds()
            stale = age > 900   # 15 min TTL
        except Exception:
            pass

    if stale or not flow:
        refresh_conviction_flow()
        with _state_lock:
            flow = _state['flow']

    if redis_set and flow:
        redis_set(cache_key, flow, ttl_seconds=900)

    return flow


def refresh_conviction_flow():
    """Recompute conviction scores. Safe to call from background thread."""
    logger.info('Options conviction scorer: scanning…')
    try:
        flow = scan_conviction_flow()
        with _state_lock:
            _state['flow']         = flow
            _state['last_updated'] = datetime.utcnow().isoformat()
            _state['error']        = None
        logger.info(f'Options conviction scorer: {len(flow)} contracts scored')
    except Exception as e:
        logger.error(f'Options conviction refresh error: {e}')
        with _state_lock:
            _state['error'] = str(e)
