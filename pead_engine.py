"""
WHAL-100 — PEAD (Post-Earnings Announcement Drift) Scoring Engine

Computes SUE (Standardized Unexpected Earnings) for recent earnings surprises
and ranks stocks by expected drift direction and magnitude.

Proven alpha: top/bottom SUE decile = ~5.1% in 3 months (~20% annualized)
Strategy: Enter 1-5 days AFTER earnings, hold 30-60 days

Data source: Financial Modeling Prep (FMP) free tier — 250 calls/day
Kill switch: always-on (read-only, no trading side effects)
"""

import os
import json
import logging
import threading
from datetime import datetime, timedelta

import requests

logger = logging.getLogger(__name__)

FMP_BASE   = 'https://financialmodelingprep.com/stable'
FMP_KEY    = os.environ.get('FMP_API_KEY', '')

# Cache TTL: refresh every 4 hours
_CACHE_TTL = 14400

_state = {
    'scores':      [],
    'last_updated': None,
    'error':        None,
}
_state_lock = threading.Lock()

# Watchlist of high-quality tickers to track for PEAD
# Focused on large/mid-caps with liquid options and analyst coverage
PEAD_UNIVERSE = [
    'AAPL', 'MSFT', 'NVDA', 'AMZN', 'GOOGL', 'META', 'TSLA', 'JPM', 'V', 'UNH',
    'LLY', 'XOM', 'MA', 'AVGO', 'PG', 'HD', 'JNJ', 'MRK', 'COST', 'ABBV',
    'CRM', 'AMD', 'NFLX', 'WMT', 'BAC', 'QCOM', 'ORCL', 'GE', 'NOW', 'PANW',
    'UBER', 'SNOW', 'PLTR', 'DDOG', 'CRWD', 'NET', 'ZS', 'ABNB', 'SHOP', 'SQ',
    'PYPL', 'COIN', 'HOOD', 'SOFI', 'RBLX', 'U', 'SPOT', 'DASH', 'LYFT', 'PINS',
    'AMD', 'MU', 'INTC', 'AMAT', 'LRCX', 'KLAC', 'MRVL', 'TXN', 'ADI', 'NXPI',
    'GS', 'MS', 'C', 'WFC', 'AXP', 'SCHW', 'BLK', 'SPGI', 'CME', 'ICE',
    'PFE', 'MRNA', 'GILD', 'REGN', 'BIIB', 'AMGN', 'BMY', 'VRTX', 'ISRG', 'MDT',
    'BA', 'CAT', 'DE', 'HON', 'RTX', 'LMT', 'NOC', 'GD', 'EMR', 'ETN',
    'SBUX', 'MCD', 'YUM', 'CMG', 'DPZ', 'DRI', 'HLT', 'MAR', 'WYNN', 'MGM',
]


# ── FMP helpers ──────────────────────────────────────────────────────

def _fmp_get(path: str, params: dict = None) -> dict | list | None:
    """Make a FMP stable API call. Returns parsed JSON or None on failure."""
    if not FMP_KEY:
        logger.warning('FMP_API_KEY not set')
        return None
    params = params or {}
    params['apikey'] = FMP_KEY
    try:
        r = requests.get(f'{FMP_BASE}{path}', params=params, timeout=15)
        if r.status_code == 200:
            data = r.json()
            # FMP returns error strings for premium endpoints
            if isinstance(data, str) and 'Premium' in data:
                logger.warning(f'FMP premium endpoint blocked: {path}')
                return None
            return data
        logger.warning(f'FMP {path}: HTTP {r.status_code}')
    except Exception as e:
        logger.error(f'FMP {path}: {e}')
    return None


_YF_HDR  = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'}
_SEC_HDR = {'User-Agent': 'WhaleTracker research@whaletracker.app'}

# CIK map for PEAD universe
_CIK_MAP = {
    'AAPL':'0000320193','MSFT':'0000789019','NVDA':'0001045810',
    'AMZN':'0001018724','META':'0001326801','GOOGL':'0001652044',
    'TSLA':'0001318605','AMD':'0000002488','JPM':'0000019617',
    'BAC':'0000070858','GS':'0000886982','V':'0001403161',
    'MA':'0001141788','LLY':'0000059478','MRNA':'0001682852',
    'PFE':'0000078003','XOM':'0000034088','CVX':'0000093410',
    'CRWD':'0001535527','PANW':'0001327567','NET':'0001477333',
    'CRM':'0001108524','SNOW':'0001640147','MU':'0000723125',
    'INTC':'0000050863','QCOM':'0000804328','AVGO':'0001054374',
    'SOFI':'0001818874','PLTR':'0001321655','COIN':'0001679788',
    'NFLX':'0001065280','UBER':'0001543151','ABNB':'0001559720',
}


def _fetch_earnings_surprise(ticker: str) -> list[dict]:
    """
    Fetch actual quarterly EPS from SEC EDGAR XBRL company facts (free, no key).
    Uses year-ago quarter as the estimate (seasonal naive benchmark).
    Returns list of {date, actual, estimated, surprise, surprise_pct}.
    """
    cik = _CIK_MAP.get(ticker.upper())
    if not cik:
        return []
    try:
        r = requests.get(
            f'https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json',
            headers=_SEC_HDR, timeout=12,
        )
        if r.status_code != 200:
            return []
        eps_entries = (r.json().get('facts', {}).get('us-gaap', {})
                       .get('EarningsPerShareDiluted', {})
                       .get('units', {}).get('USD/shares', []))
        # Keep only quarterly 10-Q filings, deduplicated by period end date, newest first
        seen = set()
        quarterly = []
        for e in sorted(eps_entries, key=lambda x: x.get('filed', ''), reverse=True):
            if e.get('form') != '10-Q' or not e.get('fp', '').startswith('Q'):
                continue
            key = e.get('end', '')
            if key and key not in seen:
                seen.add(key)
                quarterly.append(e)
            if len(quarterly) >= 8:
                break
        if len(quarterly) < 2:
            return []
        results = []
        for i, row in enumerate(quarterly):
            actual = float(row.get('val', 0))
            # Use same quarter from prior year as estimate (index i+4)
            prior_idx = i + 4
            estimated = float(quarterly[prior_idx]['val']) if prior_idx < len(quarterly) else actual
            surprise  = actual - estimated
            surp_pct  = (surprise / abs(estimated) * 100) if estimated != 0 else 0
            results.append({
                'date':         row.get('end', ''),
                'actual':       round(actual, 4),
                'estimated':    round(estimated, 4),
                'surprise':     round(surprise, 4),
                'surprise_pct': round(surp_pct, 2),
            })
        return results
    except Exception as e:
        logger.debug(f'_fetch_earnings_surprise {ticker}: {e}')
    return []


def _compute_sue(surprises: list[dict]) -> float | None:
    """
    SUE = (Most Recent Surprise) / StdDev(last N surprises)
    Uses last 8 quarters for std dev.
    Positive = beat, Negative = miss.
    Returns None if insufficient data.
    """
    if len(surprises) < 2:
        return None
    values = [s['surprise'] for s in surprises]
    recent = values[0]   # most recent quarter

    # Standard deviation of all available surprises
    mean = sum(values) / len(values)
    variance = sum((v - mean) ** 2 for v in values) / len(values)
    std_dev = variance ** 0.5

    if std_dev == 0:
        return None
    return round(recent / std_dev, 3)


def _fetch_earnings_calendar(days_back: int = 30, days_forward: int = 7) -> list[dict]:
    """Fetch upcoming earnings from Yahoo Finance earningsCalendar (free)."""
    # Yahoo Finance doesn't have a clean calendar endpoint — use our insider universe
    # Return a synthetic calendar so PEAD can score known tickers
    today = datetime.utcnow().date()
    results = []
    for ticker in PEAD_UNIVERSE:
        try:
            r = requests.get(
                f'https://query1.finance.yahoo.com/v10/finance/quoteSummary/{ticker}',
                params={'modules': 'calendarEvents'},
                headers=_YF_HDR, timeout=8,
            )
            if r.status_code == 200:
                ev = (r.json().get('quoteSummary', {}).get('result', [{}])[0]
                      .get('calendarEvents', {}).get('earnings', {}))
                dates = ev.get('earningsDate', [])
                if dates:
                    date_str = dates[0].get('fmt', '')
                    if date_str:
                        results.append({'symbol': ticker, 'date': date_str})
        except Exception:
            pass
    return results


def _fetch_quote(ticker: str) -> dict:
    """Fetch current price via Yahoo Finance chart API."""
    try:
        r = requests.get(
            f'https://query1.finance.yahoo.com/v8/finance/chart/{ticker}',
            params={'interval': '1d', 'range': '2d'},
            headers=_YF_HDR, timeout=8,
        )
        if r.status_code == 200:
            meta = r.json().get('chart', {}).get('result', [{}])[0].get('meta', {})
            price = float(meta.get('regularMarketPrice', 0) or 0)
            prev  = float(meta.get('chartPreviousClose', price) or price)
            chg   = round((price - prev) / prev * 100, 2) if prev > 0 else 0
            return {'price': round(price, 2), 'changesPercentage': chg, 'symbol': ticker}
    except Exception as e:
        logger.debug(f'_fetch_quote {ticker}: {e}')
    return {}


# ── SUE decile classification ────────────────────────────────────────

def _sue_label(sue: float) -> str:
    """Map SUE to human-readable label."""
    if sue >= 3.0:   return 'STRONG BEAT'
    if sue >= 1.5:   return 'BEAT'
    if sue >= 0.5:   return 'SLIGHT BEAT'
    if sue >= -0.5:  return 'IN-LINE'
    if sue >= -1.5:  return 'SLIGHT MISS'
    if sue >= -3.0:  return 'MISS'
    return 'STRONG MISS'


def _drift_direction(sue: float) -> str:
    if sue >= 0.5:   return 'LONG'
    if sue <= -0.5:  return 'SHORT'
    return 'NEUTRAL'


def _drift_confidence(sue: float) -> int:
    """0-100 confidence score based on SUE magnitude."""
    abs_sue = abs(sue)
    if abs_sue >= 3.0:  return 90
    if abs_sue >= 2.0:  return 75
    if abs_sue >= 1.5:  return 65
    if abs_sue >= 1.0:  return 50
    if abs_sue >= 0.5:  return 35
    return 10


def _days_since_earnings(earnings_date: str) -> int | None:
    """Return number of days since earnings announcement."""
    try:
        dt = datetime.strptime(earnings_date[:10], '%Y-%m-%d').date()
        return (datetime.utcnow().date() - dt).days
    except Exception:
        return None


# ── Main scoring function ────────────────────────────────────────────

def score_ticker(ticker: str) -> dict | None:
    """
    Compute full PEAD score for a single ticker.
    Returns dict with SUE, drift direction, confidence, entry window, etc.
    """
    ticker = ticker.upper().strip()

    surprises = _fetch_earnings_surprise(ticker)
    if not surprises:
        return None

    sue = _compute_sue(surprises)
    if sue is None:
        return None

    latest   = surprises[0]
    days_ago = _days_since_earnings(latest['date'])

    # PEAD entry window: 1-5 days after earnings
    in_entry_window = days_ago is not None and 1 <= days_ago <= 5
    # Still tradeable: within 90-day drift window
    tradeable = days_ago is not None and days_ago <= 90

    quote      = _fetch_quote(ticker)
    price      = float(quote.get('price') or 0)
    change_pct = float(quote.get('changesPercentage') or 0)
    mkt_cap    = float(quote.get('marketCap') or 0)

    direction  = _drift_direction(sue)
    confidence = _drift_confidence(sue)
    label      = _sue_label(sue)

    # Expected drift: ~5.1% per 3 months for top/bottom decile (scaled)
    abs_sue      = abs(sue)
    expected_pct = round(min(abs_sue * 1.7, 8.0), 1)   # capped at 8%

    return {
        'ticker':           ticker,
        'sue':              sue,
        'sue_label':        label,
        'direction':        direction,
        'confidence':       confidence,
        'expected_drift_pct': expected_pct if direction != 'NEUTRAL' else 0,
        'earnings_date':    latest['date'],
        'days_since_earnings': days_ago,
        'in_entry_window':  in_entry_window,
        'tradeable':        tradeable,
        'actual_eps':       latest['actual'],
        'estimated_eps':    latest['estimated'],
        'surprise_pct':     latest['surprise_pct'],
        'price':            round(price, 2),
        'change_pct':       round(change_pct, 2),
        'mkt_cap_b':        round(mkt_cap / 1e9, 1) if mkt_cap else None,
        'recent_surprises': surprises[:4],
        'scored_at':        datetime.utcnow().isoformat(),
    }


# ── Batch scorer ─────────────────────────────────────────────────────

def compute_pead_scores(tickers: list[str] = None) -> list[dict]:
    """
    Score all tickers in PEAD_UNIVERSE (or provided list).
    Returns sorted list: strong beats/misses first, in entry window highlighted.
    Respects FMP free tier (250 calls/day) — each ticker = 2 calls (surprise + quote).
    """
    universe = tickers or PEAD_UNIVERSE
    # Limit to 80 tickers to stay within FMP free tier
    universe = universe[:80]

    results = []
    for ticker in universe:
        try:
            score = score_ticker(ticker)
            if score and score['direction'] != 'NEUTRAL':
                results.append(score)
        except Exception as e:
            logger.warning(f'PEAD score error {ticker}: {e}')

    # Sort: entry window first, then by abs(SUE) desc
    results.sort(
        key=lambda x: (
            not x['in_entry_window'],   # entry window items first
            -abs(x['sue']),             # then by SUE magnitude
        )
    )
    return results


# ── Cache refresh ────────────────────────────────────────────────────

def refresh_scores():
    """Recompute PEAD scores and cache in memory. Called on schedule."""
    logger.info('PEAD engine: refreshing scores…')
    try:
        scores = compute_pead_scores()
        with _state_lock:
            _state['scores']       = scores
            _state['last_updated'] = datetime.utcnow().isoformat()
            _state['error']        = None
        logger.info(f'PEAD engine: {len(scores)} tradeable scores computed')
    except Exception as e:
        logger.error(f'PEAD engine refresh error: {e}')
        with _state_lock:
            _state['error'] = str(e)


def get_scores(redis_get=None, redis_set=None) -> list[dict]:
    """Return cached PEAD scores, refreshing if stale."""
    cache_key = 'pead:scores'

    if redis_get:
        cached = redis_get(cache_key)
        if cached is not None:
            return cached

    with _state_lock:
        scores       = _state['scores']
        last_updated = _state['last_updated']

    # Refresh if empty or stale (>4 hours)
    stale = True
    if last_updated:
        try:
            age = (datetime.utcnow() - datetime.fromisoformat(last_updated)).total_seconds()
            stale = age > _CACHE_TTL
        except Exception:
            pass

    # Never block — kick off background refresh if stale
    if stale or not scores:
        import threading as _t
        _t.Thread(target=refresh_scores, daemon=True, name='pead-refresh').start()

    if redis_set and scores:
        redis_set(cache_key, scores, ttl_seconds=_CACHE_TTL)

    return scores


def get_ticker_score(ticker: str) -> dict | None:
    """Return PEAD score for a single ticker (always fresh)."""
    return score_ticker(ticker.upper().strip())


def get_earnings_calendar(days_back: int = 14, days_forward: int = 7) -> list[dict]:
    """Return upcoming + recent earnings calendar from FMP."""
    raw = _fetch_earnings_calendar(days_back, days_forward)
    today = datetime.utcnow().date()
    results = []
    for row in raw:
        ticker = (row.get('symbol') or '').upper()
        date_str = row.get('date', '')
        if not ticker or not date_str:
            continue
        try:
            dt      = datetime.strptime(date_str[:10], '%Y-%m-%d').date()
            days_to = (dt - today).days
        except Exception:
            days_to = None
        results.append({
            'ticker':      ticker,
            'date':        date_str[:10],
            'days_to':     days_to,
            'eps_est':     row.get('epsEstimated'),
            'eps_actual':  row.get('epsActual'),
            'revenue_est': row.get('revenueEstimated'),
            'time':        row.get('time', ''),   # 'bmo' or 'amc'
        })
    # Sort by date
    results.sort(key=lambda x: x['date'])
    return results
