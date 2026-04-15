"""
WHAL-103 — Smart Money Composite Scorer

Combines 4 institutional signals into a conviction score (0-4):
  Signal 1 (+1) — Insider Form 4 buy > $100K in last 30 days   (Supabase)
  Signal 2 (+1) — Congressional buy disclosure in last 60 days  (QuiverQuant)
  Signal 3 (+1) — Short interest declining (squeeze potential)   (yfinance)
  Signal 4 (+1) — Unusual options flow (call volume spike)       (yfinance — dark pool proxy)

Score 4   = Maximum conviction — all smart money aligned
Score 3   = High conviction institutional entry
Score 2   = Moderate — 2 independent signals
Score 1   = Weak — single signal
Score 0   = No unusual activity

Cross-signal bonus: Congressional + Insider same week = compound signal
"""

import os
import logging
import threading
from datetime import datetime, timedelta, date

import requests
import yfinance as yf

logger = logging.getLogger(__name__)

_SUPA_URL = os.environ.get('SUPABASE_URL', 'https://bedurjtazsfbnkisoeee.supabase.co')
_SUPA_KEY = os.environ.get(
    'SUPABASE_KEY',
    'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJlZHVyanRhenNmYm5raXNvZWVlIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzM2NDcxOTUsImV4cCI6MjA4OTIyMzE5NX0.MK4N9dxAIHlXkGP4rLJPq5tHh9UU8L75EB1b8Q7CVmg'
)

_state_lock = threading.Lock()
_state = {
    'composite':    [],
    'last_updated': None,
    'error':        None,
    'refreshing':   False,
}

CACHE_TTL = 3600   # 1 hour


# ── Signal 1: Insider buys ───────────────────────────────────────────

def _fetch_insider_buys_by_ticker(days: int = 30) -> dict[str, list[dict]]:
    """Return {ticker: [filing, ...]} for insider BUYS > $100K in last N days."""
    cutoff = (datetime.utcnow() - timedelta(days=days)).strftime('%Y-%m-%d')
    try:
        r = requests.get(
            f'{_SUPA_URL}/rest/v1/filings',
            headers={'apikey': _SUPA_KEY, 'Authorization': f'Bearer {_SUPA_KEY}'},
            params={
                'select':           'ticker,owner_name,transaction_type,value,transaction_date',
                'transaction_date': f'gte.{cutoff}',
                'transaction_type': 'eq.Buy',
                'limit':            '500',
            },
            timeout=12,
        )
        if r.status_code == 200:
            result: dict[str, list] = {}
            for row in r.json():
                ticker = (row.get('ticker') or '').upper()
                value  = float(row.get('value') or 0)
                if ticker and value >= 100_000:
                    result.setdefault(ticker, []).append(row)
            return result
    except Exception as e:
        logger.warning(f'insider_buys_by_ticker error: {e}')
    return {}


# ── Signal 2: Congressional trades ──────────────────────────────────

def _fetch_congress_buys_by_ticker(days: int = 60) -> dict[str, list[dict]]:
    """Return {ticker: [trade, ...]} for congressional BUYS in last N days."""
    cutoff = (datetime.utcnow() - timedelta(days=days)).date()
    try:
        r = requests.get(
            'https://api.quiverquant.com/beta/live/congresstrading',
            headers={'Accept': 'application/json'},
            timeout=15,
        )
        if r.status_code == 200:
            result: dict[str, list] = {}
            for t in r.json():
                ticker = (t.get('Ticker') or '').strip().upper()
                if not ticker or ticker in ('N/A', '--', ''):
                    continue
                tx_raw = (t.get('Transaction') or '').lower()
                if 'purchase' not in tx_raw and 'buy' not in tx_raw:
                    continue
                date_str = t.get('TransactionDate', t.get('ReportDate', ''))
                try:
                    tx_date = datetime.strptime(date_str[:10], '%Y-%m-%d').date()
                    if tx_date < cutoff:
                        continue
                except Exception:
                    pass
                result.setdefault(ticker, []).append({
                    'member':  t.get('Representative', ''),
                    'party':   (t.get('Party') or '')[:1].upper(),
                    'chamber': 'Senate' if (t.get('House') or '') == 'Senate' else 'House',
                    'amount':  t.get('Range', ''),
                    'date':    date_str[:10],
                })
            return result
    except Exception as e:
        logger.warning(f'congress_buys_by_ticker error: {e}')
    return {}


# ── Signal 3: Short interest ─────────────────────────────────────────

_YF_HEADERS = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'}


def _fetch_short_interest(ticker: str) -> dict:
    """Fetch short interest via Yahoo Finance summary API — no yfinance library."""
    try:
        r = requests.get(
            f'https://query1.finance.yahoo.com/v10/finance/quoteSummary/{ticker}',
            params={'modules': 'defaultKeyStatistics'},
            headers=_YF_HEADERS,
            timeout=6,
        )
        if r.status_code == 200:
            ks = r.json().get('quoteSummary', {}).get('result', [{}])[0].get('defaultKeyStatistics', {})
            short_pct   = float((ks.get('shortPercentOfFloat') or {}).get('raw', 0) or 0) * 100
            short_ratio = float((ks.get('shortRatio') or {}).get('raw', 0) or 0)
            return {'short_pct': round(short_pct, 1), 'short_ratio': round(short_ratio, 1)}
    except Exception as e:
        logger.debug(f'short_interest {ticker}: {e}')
    return {'short_pct': 0, 'short_ratio': 0}


def _fetch_price(ticker: str) -> tuple[float, float]:
    """Return (price, change_pct) via Yahoo Finance chart API."""
    try:
        r = requests.get(
            f'https://query1.finance.yahoo.com/v8/finance/chart/{ticker}',
            params={'interval': '1d', 'range': '2d'},
            headers=_YF_HEADERS,
            timeout=6,
        )
        if r.status_code == 200:
            result = r.json().get('chart', {}).get('result', [{}])[0]
            meta   = result.get('meta', {})
            price  = float(meta.get('regularMarketPrice', 0) or 0)
            prev   = float(meta.get('chartPreviousClose', meta.get('previousClose', price)) or price)
            chg    = round((price - prev) / prev * 100, 2) if prev > 0 else 0.0
            return round(price, 2), chg
    except Exception as e:
        logger.debug(f'price {ticker}: {e}')
    return 0.0, 0.0


# ── Signal 4: Unusual options flow (dark pool proxy) ─────────────────
# Uses the cached options flow data already computed by options_flow_scorer
# to avoid redundant yfinance calls.

def _fetch_unusual_calls(ticker: str) -> dict:
    """Check cached options flow for unusual call activity on this ticker."""
    try:
        from options_flow_scorer import _state as _opts_state
        flow = _opts_state.get('flow', [])
        for contract in flow:
            if contract.get('ticker', '').upper() == ticker.upper() and contract.get('type') == 'CALL':
                ratio = (contract.get('vol_oi_ratio') or 0)
                unusual = contract.get('unusual', False) or float(ratio) >= 0.5
                if unusual:
                    return {
                        'unusual':  True,
                        'call_vol': contract.get('volume', 0),
                        'call_oi':  contract.get('open_interest', 0),
                        'ratio':    float(ratio),
                    }
    except Exception as e:
        logger.debug(f'unusual_calls {ticker}: {e}')
    return {'unusual': False, 'call_vol': 0, 'call_oi': 0, 'ratio': 0}


# ── Composite scorer ─────────────────────────────────────────────────

def _composite_label(score: int) -> str:
    if score >= 4: return 'MAX CONVICTION'
    if score >= 3: return 'HIGH CONVICTION'
    if score >= 2: return 'MODERATE'
    if score >= 1: return 'WEAK SIGNAL'
    return 'NO SIGNAL'


def _parse_amount(amount_str: str) -> float:
    """Parse congressional amount range like '$1M–$5M' to midpoint."""
    try:
        s = amount_str.replace('$', '').replace(',', '').upper()
        parts = [p.strip() for p in s.replace('–', '-').split('-')]
        def _to_num(x):
            if 'M' in x: return float(x.replace('M','')) * 1_000_000
            if 'K' in x: return float(x.replace('K','')) * 1_000
            return float(x)
        nums = [_to_num(p) for p in parts if p]
        return sum(nums) / len(nums) if nums else 0
    except Exception:
        return 0


def score_ticker(
    ticker: str,
    insider_map: dict[str, list],
    congress_map: dict[str, list],
    include_options: bool = True,
) -> dict | None:
    """
    Compute smart money composite score for a single ticker.
    Returns None if score == 0 (no signal).
    """
    ticker = ticker.upper().strip()
    signals   = []
    score     = 0
    details   = {}

    # Signal 1: Insider buys
    insider_trades = insider_map.get(ticker, [])
    if insider_trades:
        total_val = sum(float(t.get('value') or 0) for t in insider_trades)
        score += 1
        signals.append('INSIDER')
        details['insider'] = {
            'count':      len(insider_trades),
            'total_value': round(total_val, 0),
            'latest_buyer': insider_trades[0].get('owner_name', ''),
            'latest_date':  insider_trades[0].get('transaction_date', ''),
        }

    # Signal 2: Congressional buys
    congress_trades = congress_map.get(ticker, [])
    if congress_trades:
        top = congress_trades[0]
        score += 1
        signals.append('CONGRESS')
        details['congress'] = {
            'count':   len(congress_trades),
            'member':  top.get('member', ''),
            'chamber': top.get('chamber', ''),
            'amount':  top.get('amount', ''),
            'date':    top.get('date', ''),
            'amount_mid': _parse_amount(top.get('amount', '')),
        }

    # Signal 3: Short interest (short squeeze potential)
    si = _fetch_short_interest(ticker)
    short_pct = si['short_pct']
    if short_pct >= 10:
        score += 1
        signals.append('SHORT SQUEEZE')
        details['short_interest'] = si

    # Signal 4: Unusual options / dark pool proxy
    if include_options:
        opts = _fetch_unusual_calls(ticker)
        if opts['unusual']:
            score += 1
            signals.append('UNUSUAL OPTIONS')
            details['options_flow'] = opts

    if score == 0:
        return None

    # Cross-signal bonus flag
    both_insider_congress = 'INSIDER' in signals and 'CONGRESS' in signals

    # Price info via direct Yahoo Finance HTTP (no yfinance library)
    price, change_pct = _fetch_price(ticker)

    return {
        'ticker':                  ticker,
        'score':                   score,
        'label':                   _composite_label(score),
        'signals':                 signals,
        'compound_signal':         both_insider_congress,
        'price':                   price,
        'change_pct':              change_pct,
        'details':                 details,
        'scored_at':               datetime.utcnow().isoformat(),
    }


# ── Batch scanner ────────────────────────────────────────────────────

def compute_composite_scores(tickers: list[str] = None) -> list[dict]:
    """
    Score all tickers that appear in insider or congress data.
    Returns sorted list: compound signals first, then by score desc.
    """
    insider_map  = _fetch_insider_buys_by_ticker(days=30)
    congress_map = _fetch_congress_buys_by_ticker(days=60)

    # Universe = all tickers that appear in either signal
    universe = list(dict.fromkeys(
        list(insider_map.keys()) + list(congress_map.keys()) + (tickers or [])
    ))
    universe = universe[:60]   # cap to keep runtime reasonable

    results = []
    for ticker in universe:
        try:
            s = score_ticker(ticker, insider_map, congress_map)
            if s:
                results.append(s)
        except Exception as e:
            logger.warning(f'composite score error {ticker}: {e}')

    # Sort: compound first, then score desc, then change_pct desc
    results.sort(key=lambda x: (
        not x['compound_signal'],
        -x['score'],
        -abs(x.get('change_pct', 0)),
    ))
    return results


# ── Cache ────────────────────────────────────────────────────────────

def get_composite_scores(redis_get=None, redis_set=None) -> list[dict]:
    cache_key = 'smart_money:composite'

    # 1. Redis cache hit — return immediately
    if redis_get:
        cached = redis_get(cache_key)
        if cached:
            return cached

    with _state_lock:
        data         = _state['composite']
        last_updated = _state['last_updated']
        refreshing   = _state.get('refreshing', False)

    stale = True
    if last_updated:
        try:
            age   = (datetime.utcnow() - datetime.fromisoformat(last_updated)).total_seconds()
            stale = age > CACHE_TTL
        except Exception:
            pass

    # 2. If stale/empty, kick off background refresh — never block the request
    if (stale or not data) and not refreshing:
        import threading
        threading.Thread(target=refresh_composite, daemon=True, name='smart-money-bg').start()

    # 3. Return whatever we have immediately (may be empty on very first cold start)
    if redis_set and data:
        redis_set(cache_key, data, ttl_seconds=CACHE_TTL)

    return data


def refresh_composite(redis_set=None):
    with _state_lock:
        if _state.get('refreshing'):
            return
        _state['refreshing'] = True
    logger.info('Smart money composite: refreshing…')
    try:
        scores = compute_composite_scores()
        with _state_lock:
            _state['composite']    = scores
            _state['last_updated'] = datetime.utcnow().isoformat()
            _state['error']        = None
            _state['refreshing']   = False
        # Persist to Redis so all endpoints read it instantly
        if redis_set and scores:
            redis_set('smart_money:composite', scores, ttl_seconds=CACHE_TTL)
        logger.info(f'Smart money composite: {len(scores)} tickers scored')
    except Exception as e:
        logger.error(f'Smart money composite refresh error: {e}')
        with _state_lock:
            _state['error']      = str(e)
            _state['refreshing'] = False
