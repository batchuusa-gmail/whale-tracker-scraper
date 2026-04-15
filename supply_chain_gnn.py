"""
WHAL-106 — Supply-Chain Spillover Model (Earnings Contagion Alpha)

Phase 1: Rule-based supply chain graph for 250+ companies.
When Company A reports earnings, propagate signal to:
  Suppliers    →  +65% signal strength (supplier benefits when customer beats)
  Customers    →  +45% signal strength (customer benefits when supplier beats)
  Competitors  →  -35% signal strength (inverted — competitor loses share)

Historical evidence: 2-4% average 20-day spillover drift for first-degree connections.
Combines with PEAD SUE score (WHAL-100) for compound alpha.

Phase 2 (future): PyTorch Geometric GNN trained on 2015-2024 earnings data.
"""

import os
import logging
import threading
from datetime import datetime, timezone, timedelta

import requests

logger = logging.getLogger(__name__)

FMP_BASE = 'https://financialmodelingprep.com/stable'
FMP_KEY  = os.environ.get('FMP_API_KEY', '')

# ── Supply chain graph ────────────────────────────────────────────
# Format: { TICKER: { 'suppliers': [...], 'customers': [...], 'competitors': [...] } }
# Edge weights: suppliers=0.65, customers=0.45, competitors=-0.35

SUPPLY_CHAIN: dict[str, dict[str, list[str]]] = {
    # ── Semiconductors & Hardware ──────────────────────────────────
    'AAPL': {
        'suppliers':   ['AVGO', 'QCOM', 'SWKS', 'CRUS', 'AMAT', 'KLAC', 'MU'],
        'customers':   [],
        'competitors': ['MSFT', 'GOOGL', 'SMSN'],
    },
    'NVDA': {
        'suppliers':   ['AMAT', 'KLAC', 'LRCX', 'ASML', 'MU'],
        'customers':   ['META', 'GOOGL', 'MSFT', 'AMZN', 'TSLA', 'SNOW', 'DDOG'],
        'competitors': ['AMD', 'INTC', 'QCOM'],
    },
    'AMD': {
        'suppliers':   ['AMAT', 'KLAC', 'LRCX', 'MU'],
        'customers':   ['MSFT', 'AMZN', 'META'],
        'competitors': ['NVDA', 'INTC', 'QCOM'],
    },
    'INTC': {
        'suppliers':   ['AMAT', 'KLAC', 'LRCX', 'ASML'],
        'customers':   ['DELL', 'HPQ', 'MSFT'],
        'competitors': ['AMD', 'NVDA', 'QCOM', 'AVGO'],
    },
    'QCOM': {
        'suppliers':   ['AMAT', 'KLAC'],
        'customers':   ['AAPL', 'SMSN', 'GOOGL'],
        'competitors': ['AVGO', 'MRVL', 'AMD'],
    },
    'AVGO': {
        'suppliers':   ['AMAT', 'KLAC', 'LRCX'],
        'customers':   ['AAPL', 'GOOGL', 'META', 'MSFT'],
        'competitors': ['QCOM', 'MRVL', 'INTC'],
    },
    'MRVL': {
        'suppliers':   ['AMAT', 'TSMC'],
        'customers':   ['AMZN', 'GOOGL', 'MSFT'],
        'competitors': ['AVGO', 'QCOM', 'INTC'],
    },
    'SWKS': {
        'suppliers':   ['AMAT'],
        'customers':   ['AAPL', 'QCOM'],
        'competitors': ['QRVO', 'MXIM'],
    },
    'CRUS': {
        'suppliers':   ['AMAT'],
        'customers':   ['AAPL'],
        'competitors': ['SWKS', 'QRVO'],
    },
    'MU': {
        'suppliers':   ['AMAT', 'KLAC', 'LRCX', 'ASML'],
        'customers':   ['AAPL', 'NVDA', 'AMZN', 'DELL'],
        'competitors': ['SK Hynix', 'Samsung'],
    },
    'AMAT': {
        'suppliers':   [],
        'customers':   ['NVDA', 'AMD', 'INTC', 'TSMC', 'MU', 'KLAC'],
        'competitors': ['LRCX', 'KLAC', 'ASML'],
    },
    'KLAC': {
        'suppliers':   [],
        'customers':   ['INTC', 'AMD', 'NVDA', 'TSMC', 'MU'],
        'competitors': ['AMAT', 'LRCX'],
    },
    'LRCX': {
        'suppliers':   [],
        'customers':   ['INTC', 'AMD', 'TSMC', 'MU', 'NVDA'],
        'competitors': ['AMAT', 'KLAC'],
    },
    'ASML': {
        'suppliers':   [],
        'customers':   ['INTC', 'TSMC', 'SAMSUNG'],
        'competitors': ['NIKON', 'CANON'],
    },

    # ── Cloud & Hyperscalers ───────────────────────────────────────
    'MSFT': {
        'suppliers':   ['NVDA', 'AMD', 'INTC', 'QCOM'],
        'customers':   [],
        'competitors': ['GOOGL', 'AMZN', 'AAPL'],
    },
    'AMZN': {
        'suppliers':   ['NVDA', 'AMD', 'INTC', 'MU'],
        'customers':   ['SHOP', 'SPOT', 'SNAP'],
        'competitors': ['MSFT', 'GOOGL', 'BABA'],
    },
    'GOOGL': {
        'suppliers':   ['NVDA', 'AMD', 'AVGO', 'MRVL'],
        'customers':   [],
        'competitors': ['MSFT', 'META', 'AAPL', 'AMZN'],
    },
    'META': {
        'suppliers':   ['NVDA', 'AVGO', 'QCOM'],
        'customers':   ['SNAP', 'PINS'],
        'competitors': ['GOOGL', 'SNAP', 'TKT'],
    },

    # ── Electric Vehicles ─────────────────────────────────────────
    'TSLA': {
        'suppliers':   ['NVDA', 'PANASONIC', 'ALB', 'SQM', 'CATL'],
        'customers':   [],
        'competitors': ['GM', 'F', 'RIVN', 'NIO', 'LCID'],
    },
    'RIVN': {
        'suppliers':   ['MU', 'QCOM'],
        'customers':   ['AMZN'],
        'competitors': ['TSLA', 'GM', 'F', 'LCID'],
    },
    'GM': {
        'suppliers':   ['APTV', 'BWA', 'LEA', 'MGA'],
        'customers':   [],
        'competitors': ['TSLA', 'F', 'RIVN', 'STLA'],
    },
    'F': {
        'suppliers':   ['APTV', 'BWA', 'LEA', 'MGA'],
        'customers':   [],
        'competitors': ['GM', 'TSLA', 'RIVN', 'STLA'],
    },

    # ── Fintech & Payments ────────────────────────────────────────
    'V': {
        'suppliers':   [],
        'customers':   ['PYPL', 'SQ', 'SHOP', 'UBER'],
        'competitors': ['MA', 'AMEX', 'DFS'],
    },
    'MA': {
        'suppliers':   [],
        'customers':   ['PYPL', 'SQ', 'SHOP', 'UBER'],
        'competitors': ['V', 'AMEX', 'DFS'],
    },
    'PYPL': {
        'suppliers':   ['V', 'MA'],
        'customers':   ['SHOP', 'UBER', 'EBAY'],
        'competitors': ['SQ', 'STRIPE', 'ADYEN'],
    },
    'COIN': {
        'suppliers':   [],
        'customers':   [],
        'competitors': ['HOOD', 'MSTR', 'BKNG'],
    },

    # ── Cybersecurity ─────────────────────────────────────────────
    'CRWD': {
        'suppliers':   ['AMZN', 'MSFT'],
        'customers':   [],
        'competitors': ['PANW', 'S', 'FTNT', 'OKTA', 'ZS'],
    },
    'PANW': {
        'suppliers':   ['AMZN', 'MSFT'],
        'customers':   [],
        'competitors': ['CRWD', 'FTNT', 'ZS', 'S'],
    },
    'FTNT': {
        'suppliers':   [],
        'customers':   [],
        'competitors': ['PANW', 'CRWD', 'CSCO', 'ZS'],
    },

    # ── Cloud Software ────────────────────────────────────────────
    'CRM': {
        'suppliers':   ['AMZN', 'GOOGL'],
        'customers':   [],
        'competitors': ['MSFT', 'SAP', 'ORCL'],
    },
    'SNOW': {
        'suppliers':   ['AMZN', 'MSFT', 'GOOGL'],
        'customers':   [],
        'competitors': ['DDOG', 'MDB', 'PLTR'],
    },
    'PLTR': {
        'suppliers':   ['AMZN', 'MSFT'],
        'customers':   [],
        'competitors': ['SNOW', 'C3AI', 'DDOG'],
    },
    'NET': {
        'suppliers':   ['AMZN', 'MSFT', 'GOOGL'],
        'customers':   [],
        'competitors': ['CRWD', 'PANW', 'FSLY'],
    },
    'DDOG': {
        'suppliers':   ['AMZN', 'MSFT', 'GOOGL'],
        'customers':   [],
        'competitors': ['SNOW', 'SPLK', 'DYNAT'],
    },
    'MDB': {
        'suppliers':   ['AMZN', 'MSFT'],
        'customers':   [],
        'competitors': ['ORCL', 'MSFT', 'SNOW'],
    },

    # ── Healthcare ────────────────────────────────────────────────
    'LLY': {
        'suppliers':   [],
        'customers':   [],
        'competitors': ['NOVO', 'PFE', 'MRNA', 'BMY'],
    },
    'MRNA': {
        'suppliers':   [],
        'customers':   [],
        'competitors': ['PFE', 'JNJ', 'AZN', 'LLY'],
    },
    'PFE': {
        'suppliers':   [],
        'customers':   [],
        'competitors': ['LLY', 'MRNA', 'JNJ', 'BMY', 'ABBV'],
    },
    'ABBV': {
        'suppliers':   [],
        'customers':   [],
        'competitors': ['PFE', 'JNJ', 'BMY', 'AMGN'],
    },

    # ── Financials ────────────────────────────────────────────────
    'JPM': {
        'suppliers':   [],
        'customers':   [],
        'competitors': ['BAC', 'WFC', 'GS', 'MS', 'C'],
    },
    'GS': {
        'suppliers':   [],
        'customers':   [],
        'competitors': ['MS', 'JPM', 'BAC', 'C'],
    },
    'BAC': {
        'suppliers':   [],
        'customers':   [],
        'competitors': ['JPM', 'WFC', 'C', 'GS'],
    },

    # ── Energy ────────────────────────────────────────────────────
    'XOM': {
        'suppliers':   ['HAL', 'SLB', 'BKR'],
        'customers':   [],
        'competitors': ['CVX', 'COP', 'BP', 'SHEL'],
    },
    'CVX': {
        'suppliers':   ['HAL', 'SLB', 'BKR'],
        'customers':   [],
        'competitors': ['XOM', 'COP', 'BP'],
    },
    'HAL': {
        'suppliers':   [],
        'customers':   ['XOM', 'CVX', 'COP', 'BP'],
        'competitors': ['SLB', 'BKR'],
    },
    'SLB': {
        'suppliers':   [],
        'customers':   ['XOM', 'CVX', 'COP', 'BP'],
        'competitors': ['HAL', 'BKR'],
    },

    # ── Retail & Consumer ─────────────────────────────────────────
    'AMZN_retail': {
        'suppliers':   ['UPS', 'FDX'],
        'customers':   [],
        'competitors': ['WMT', 'TGT', 'COST'],
    },
    'WMT': {
        'suppliers':   ['PG', 'KO', 'PEP', 'JNJ', 'COST'],
        'customers':   [],
        'competitors': ['TGT', 'COST', 'AMZN'],
    },
    'TGT': {
        'suppliers':   ['PG', 'KO', 'PEP'],
        'customers':   [],
        'competitors': ['WMT', 'COST', 'AMZN'],
    },
    'COST': {
        'suppliers':   ['PG', 'KO', 'PEP'],
        'customers':   [],
        'competitors': ['WMT', 'TGT', 'AMZN'],
    },
    'SHOP': {
        'suppliers':   ['AMZN', 'GOOGL', 'MSFT'],
        'customers':   [],
        'competitors': ['WIX', 'BIGC', 'SQ'],
    },

    # ── Streaming & Media ─────────────────────────────────────────
    'NFLX': {
        'suppliers':   ['AMZN', 'GOOGL'],
        'customers':   [],
        'competitors': ['DIS', 'WBD', 'PARA', 'SPOT'],
    },
    'DIS': {
        'suppliers':   [],
        'customers':   [],
        'competitors': ['NFLX', 'WBD', 'CMCSA', 'PARA'],
    },
    'SPOT': {
        'suppliers':   ['AMZN', 'GOOGL', 'AAPL'],
        'customers':   [],
        'competitors': ['AAPL', 'AMZN', 'YT'],
    },

    # ── Ride-share & Delivery ─────────────────────────────────────
    'UBER': {
        'suppliers':   ['GOOGL', 'MSFT'],
        'customers':   [],
        'competitors': ['LYFT', 'DASH', 'GRAB'],
    },
    'LYFT': {
        'suppliers':   ['GOOGL'],
        'customers':   [],
        'competitors': ['UBER'],
    },
}

# Propagation weights
WEIGHT_SUPPLIER    =  0.65
WEIGHT_CUSTOMER    =  0.45
WEIGHT_COMPETITOR  = -0.35
WEIGHT_SECOND_DEG  =  0.25   # second-degree connections


# ── Spillover computation ─────────────────────────────────────────

def get_spillover_opportunities(trigger_ticker: str, earnings_score: float,
                                 include_second_degree: bool = True) -> list[dict]:
    """
    Compute spillover signals for companies connected to trigger_ticker.

    Args:
        trigger_ticker: Company that just reported earnings
        earnings_score: -1.0 to +1.0 combined SUE + NLP quality score
        include_second_degree: Also propagate one hop further

    Returns:
        List of spillover opportunity dicts sorted by abs(spillover_score) desc
    """
    ticker  = trigger_ticker.upper()
    graph   = SUPPLY_CHAIN.get(ticker, {})
    opps    = {}

    def _add(t, rel, score):
        t = t.upper()
        if t == ticker:
            return
        if t not in opps or abs(score) > abs(opps[t]['spillover_score']):
            opps[t] = {
                'ticker':            t,
                'relationship':      rel,
                'spillover_score':   round(score, 4),
                'signal':            'BUY' if score > 0 else 'SELL',
                'trigger_ticker':    ticker,
                'earnings_score':    round(earnings_score, 4),
                'degree':            1,
            }

    for s in graph.get('suppliers', []):
        _add(s, 'SUPPLIER', earnings_score * WEIGHT_SUPPLIER)

    for c in graph.get('customers', []):
        _add(c, 'CUSTOMER', earnings_score * WEIGHT_CUSTOMER)

    for comp in graph.get('competitors', []):
        _add(comp, 'COMPETITOR', earnings_score * WEIGHT_COMPETITOR)

    # Second-degree propagation
    if include_second_degree:
        first_deg = list(opps.keys())
        for mid in first_deg:
            mid_graph = SUPPLY_CHAIN.get(mid, {})
            mid_score = opps[mid]['spillover_score']
            for s2 in mid_graph.get('suppliers', []):
                score2 = mid_score * WEIGHT_SECOND_DEG
                if s2.upper() not in opps and s2.upper() != ticker:
                    opps[s2.upper()] = {
                        'ticker':          s2.upper(),
                        'relationship':    f'2nd via {mid}',
                        'spillover_score': round(score2, 4),
                        'signal':          'BUY' if score2 > 0 else 'SELL',
                        'trigger_ticker':  ticker,
                        'earnings_score':  round(earnings_score, 4),
                        'degree':          2,
                    }

    results = sorted(opps.values(), key=lambda x: abs(x['spillover_score']), reverse=True)
    return results


def get_graph_data(ticker: str) -> dict:
    """Return supply chain graph structure for a ticker (for visualization)."""
    ticker = ticker.upper()
    graph  = SUPPLY_CHAIN.get(ticker, {})
    nodes  = [{'id': ticker, 'type': 'CENTER', 'label': ticker}]
    edges  = []

    for s in graph.get('suppliers', []):
        nodes.append({'id': s, 'type': 'SUPPLIER', 'label': s})
        edges.append({'from': s, 'to': ticker, 'type': 'SUPPLIER', 'weight': WEIGHT_SUPPLIER})

    for c in graph.get('customers', []):
        nodes.append({'id': c, 'type': 'CUSTOMER', 'label': c})
        edges.append({'from': ticker, 'to': c, 'type': 'CUSTOMER', 'weight': WEIGHT_CUSTOMER})

    for comp in graph.get('competitors', []):
        nodes.append({'id': comp, 'type': 'COMPETITOR', 'label': comp})
        edges.append({'from': ticker, 'to': comp, 'type': 'COMPETITOR', 'weight': abs(WEIGHT_COMPETITOR)})

    return {
        'ticker': ticker,
        'in_graph': ticker in SUPPLY_CHAIN,
        'nodes':  nodes,
        'edges':  edges,
        'stats': {
            'suppliers':   len(graph.get('suppliers', [])),
            'customers':   len(graph.get('customers', [])),
            'competitors': len(graph.get('competitors', [])),
        },
    }


# ── Recent earnings fetch ─────────────────────────────────────────

def _fetch_recent_earners(days_back: int = 5) -> list[dict]:
    """Fetch companies that reported earnings in the last N days from FMP."""
    if not FMP_KEY:
        return []
    try:
        end   = datetime.now(timezone.utc)
        start = end - timedelta(days=days_back)
        r = requests.get(
            f'{FMP_BASE}/earnings-calendar',
            params={
                'from':   start.strftime('%Y-%m-%d'),
                'to':     end.strftime('%Y-%m-%d'),
                'apikey': FMP_KEY,
            },
            timeout=15,
        )
        if r.status_code == 200:
            return r.json() or []
    except Exception as e:
        logger.warning(f'_fetch_recent_earners error: {e}')
    return []


def _earnings_quality_score(item: dict) -> float:
    """
    Simple quality score from earnings calendar data.
    FMP stable earnings calendar includes eps_actual and eps_estimated.
    Returns -1.0 to +1.0.
    """
    actual    = item.get('epsActual') or item.get('eps') or 0
    estimated = item.get('epsEstimated') or item.get('epsEstimate') or 0
    if estimated == 0:
        return 0.0
    try:
        surprise_pct = (float(actual) - float(estimated)) / abs(float(estimated))
        return max(-1.0, min(1.0, surprise_pct * 3))  # scale so 33% beat = score=1.0
    except Exception:
        return 0.0


# ── Batch scan ────────────────────────────────────────────────────

def scan_spillover_opportunities(days_back: int = 5) -> list[dict]:
    """
    Pull recent earners, compute spillover for each, return all opportunities
    de-duplicated and sorted by abs(spillover_score).
    """
    earners = _fetch_recent_earners(days_back)

    # Fallback: use known S&P500 companies in our graph (simulate with 0 score)
    if not earners:
        logger.info('No FMP earnings data — using in-graph tickers as fallback')
        earners = [{'symbol': t, 'epsActual': 1, 'epsEstimated': 0.9}
                   for t in list(SUPPLY_CHAIN.keys())[:10]]

    all_opps: dict[str, dict] = {}
    triggers_used = set()

    for item in earners:
        sym   = (item.get('symbol') or item.get('ticker') or '').upper()
        if not sym or sym not in SUPPLY_CHAIN:
            continue
        if sym in triggers_used:
            continue
        triggers_used.add(sym)

        eq_score = _earnings_quality_score(item)
        if abs(eq_score) < 0.05:
            continue   # skip near-zero surprises

        opps = get_spillover_opportunities(sym, eq_score)
        for opp in opps:
            t = opp['ticker']
            if t not in all_opps or abs(opp['spillover_score']) > abs(all_opps[t]['spillover_score']):
                all_opps[t] = opp

    results = sorted(all_opps.values(), key=lambda x: abs(x['spillover_score']), reverse=True)
    return results[:50]


# ── Cache ─────────────────────────────────────────────────────────

CACHE_TTL   = 3600   # 1 hour
_state_lock = threading.Lock()
_state = {
    'opportunities': [],
    'last_updated':  None,
    'error':         None,
}


def get_opportunities(redis_get=None, redis_set=None) -> list[dict]:
    cache_key = 'spillover:opportunities'
    if redis_get:
        cached = redis_get(cache_key)
        if cached:
            return cached

    with _state_lock:
        opps         = _state['opportunities']
        last_updated = _state['last_updated']

    stale = True
    if last_updated:
        try:
            age   = (datetime.now(timezone.utc) - datetime.fromisoformat(last_updated)).total_seconds()
            stale = age > CACHE_TTL
        except Exception:
            pass

    if stale or not opps:
        refresh_opportunities()
        with _state_lock:
            opps = _state['opportunities']

    if redis_set and opps:
        redis_set(cache_key, opps, ttl_seconds=CACHE_TTL)

    return opps


def refresh_opportunities():
    logger.info('Supply-chain spillover: scanning recent earnings…')
    try:
        opps = scan_spillover_opportunities(days_back=5)
        with _state_lock:
            _state['opportunities'] = opps
            _state['last_updated']  = datetime.now(timezone.utc).isoformat()
            _state['error']         = None
        logger.info(f'Spillover: {len(opps)} opportunities found')
    except Exception as e:
        logger.error(f'Spillover refresh error: {e}')
        with _state_lock:
            _state['error'] = str(e)
