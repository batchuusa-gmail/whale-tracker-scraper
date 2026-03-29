"""
WHAL-82/83/84/85/86/87 — Vector Score Engine
Combines 5 signal dimensions into a composite Whale Score (0–100).
Each dimension: Insider Flow, Options Flow, Dark Pool, Congressional, Sentiment.
Claude API generates a plain-English narrative for the combined score.
"""

import os
import logging
import requests
from datetime import datetime, timezone, timedelta

logger = logging.getLogger(__name__)

ANTHROPIC_KEY = os.environ.get('ANTHROPIC_API_KEY', '')

SUPABASE_URL = os.environ.get('SUPABASE_URL', 'https://bedurjtazsfbnkisoeee.supabase.co')
SUPABASE_KEY = os.environ.get(
    'SUPABASE_KEY',
    'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJlZHVyanRhenNmYm5raXNvZWVlIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzM2NDcxOTUsImV4cCI6MjA4OTIyMzE5NX0.MK4N9dxAIHlXkGP4rLJPq5tHh9UU8L75EB1b8Q7CVmg'
)
_SUPA_HEADERS = {
    'apikey': SUPABASE_KEY,
    'Authorization': f'Bearer {SUPABASE_KEY}',
    'Content-Type': 'application/json',
}

# ── Dimension weights (must sum to 1.0) ──────────────────────────
WEIGHTS = {
    'insider':     0.35,
    'options':     0.25,
    'darkpool':    0.15,
    'congress':    0.15,
    'sentiment':   0.10,
}

# ── Whale Grade thresholds ────────────────────────────────────────
def whale_grade(score: float) -> str:
    if score >= 90: return 'A+'
    if score >= 80: return 'A'
    if score >= 70: return 'B+'
    if score >= 60: return 'B'
    if score >= 50: return 'C'
    if score >= 35: return 'D'
    return 'F'


# ── WHAL-82: Insider Flow Score ───────────────────────────────────

def score_insider(ticker: str) -> dict:
    """
    Score 0–100 based on recent insider buys vs sells.
    Factors: buy/sell ratio, trade value, insider role, recency.
    """
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
        r = requests.get(
            f'{SUPABASE_URL}/rest/v1/filings',
            headers=_SUPA_HEADERS,
            params={
                'ticker': f'eq.{ticker.upper()}',
                'filed_at': f'gte.{cutoff}',
                'select': 'transaction_type,value,owner_type,filed_at',
                'limit': '50',
            },
            timeout=8,
        )
        rows = r.json() if r.status_code == 200 else []
        if not isinstance(rows, list):
            rows = []
    except Exception as e:
        logger.error(f'Insider score fetch error {ticker}: {e}')
        rows = []

    if not rows:
        return {'score': 50, 'detail': 'No recent insider filings', 'buys': 0, 'sells': 0, 'buy_value': 0}

    buys = [f for f in rows if (f.get('transaction_type') or '').lower() == 'buy']
    sells = [f for f in rows if (f.get('transaction_type') or '').lower() == 'sell']

    buy_value = sum(float(f.get('value') or 0) for f in buys)
    sell_value = sum(float(f.get('value') or 0) for f in sells)

    # Base ratio score
    total_val = buy_value + sell_value
    if total_val > 0:
        ratio_score = (buy_value / total_val) * 100
    elif buys:
        ratio_score = 70
    else:
        ratio_score = 30

    # Boost for high-value buys
    if buy_value >= 5_000_000:
        ratio_score = min(100, ratio_score + 15)
    elif buy_value >= 1_000_000:
        ratio_score = min(100, ratio_score + 8)

    # Boost for C-suite buyers
    exec_buys = [f for f in buys if any(
        kw in (f.get('owner_type') or '').lower()
        for kw in ('ceo', 'cfo', 'coo', 'president', 'director')
    )]
    if exec_buys:
        ratio_score = min(100, ratio_score + 10)

    score = round(max(0, min(100, ratio_score)))
    return {
        'score': score,
        'detail': f'{len(buys)} buys / {len(sells)} sells in 30d, ${buy_value:,.0f} buy value',
        'buys': len(buys),
        'sells': len(sells),
        'buy_value': buy_value,
    }


# ── WHAL-83: Options Flow Score ───────────────────────────────────

def score_options(ticker: str) -> dict:
    """
    Score 0–100 based on unusual options activity.
    Bullish: high call volume, low put/call ratio, unusual call sweeps.
    Bearish: high put volume, high put/call ratio.
    """
    try:
        import yfinance as yf
        t = yf.Ticker(ticker.upper())
        exps = t.options
        if not exps:
            return {'score': 50, 'detail': 'No options data', 'call_vol': 0, 'put_vol': 0, 'pc_ratio': 1.0}

        total_call_vol = 0
        total_put_vol = 0
        unusual_calls = 0
        unusual_puts = 0

        for exp in exps[:2]:
            try:
                chain = t.option_chain(exp)
                cv = int((chain.calls['volume'].sum() or 0))
                pv = int((chain.puts['volume'].sum() or 0))
                total_call_vol += cv
                total_put_vol += pv

                avg_call = chain.calls['volume'].mean() or 1
                avg_put = chain.puts['volume'].mean() or 1
                unusual_calls += int((chain.calls['volume'] > avg_call * 3).sum())
                unusual_puts += int((chain.puts['volume'] > avg_put * 3).sum())
            except Exception:
                pass

        if total_call_vol == 0 and total_put_vol == 0:
            return {'score': 50, 'detail': 'No options volume', 'call_vol': 0, 'put_vol': 0, 'pc_ratio': 1.0}

        pc_ratio = (total_put_vol / total_call_vol) if total_call_vol > 0 else 2.0

        # Low put/call = bullish
        if pc_ratio < 0.5:
            base = 80
        elif pc_ratio < 0.8:
            base = 65
        elif pc_ratio < 1.2:
            base = 50
        elif pc_ratio < 2.0:
            base = 35
        else:
            base = 20

        # Unusual call activity boosts
        base = min(100, base + unusual_calls * 3)
        base = max(0, base - unusual_puts * 3)

        score = round(max(0, min(100, base)))
        return {
            'score': score,
            'detail': f'P/C ratio {pc_ratio:.2f}, {unusual_calls} unusual calls, {unusual_puts} unusual puts',
            'call_vol': total_call_vol,
            'put_vol': total_put_vol,
            'pc_ratio': round(pc_ratio, 2),
        }

    except Exception as e:
        logger.warning(f'Options score error {ticker}: {e}')
        return {'score': 50, 'detail': 'Options data unavailable', 'call_vol': 0, 'put_vol': 0, 'pc_ratio': 1.0}


# ── WHAL-84: Dark Pool Score ──────────────────────────────────────

def score_darkpool(ticker: str) -> dict:
    """
    Score 0–100 based on short interest + dark pool proxy signals.
    Low short interest + high price momentum = bullish dark pool signal.
    High short interest = potential squeeze (both bullish and cautious).
    """
    try:
        import yfinance as yf
        info = yf.Ticker(ticker.upper()).info
        short_pct = float(info.get('shortPercentOfFloat') or 0)
        short_ratio = float(info.get('shortRatio') or 0)
        price = float(info.get('currentPrice') or info.get('regularMarketPrice') or 0)
        ma50 = float(info.get('fiftyDayAverage') or 0)
        ma200 = float(info.get('twoHundredDayAverage') or 0)
    except Exception as e:
        logger.warning(f'Dark pool score error {ticker}: {e}')
        return {'score': 50, 'detail': 'Dark pool data unavailable', 'short_pct': 0}

    score = 50

    # Price above moving averages = bullish dark pool accumulation
    if ma50 > 0 and price > ma50:
        score += 10
    if ma200 > 0 and price > ma200:
        score += 10

    # Short squeeze potential (high short + buying = bullish)
    if short_pct > 0.20:
        score += 8   # squeeze potential
    elif short_pct < 0.05:
        score += 5   # low shorting = clean
    elif short_pct > 0.35:
        score -= 10  # too much short pressure

    # Short ratio (days to cover)
    if short_ratio > 5:
        score += 5  # squeeze fuel

    score = round(max(0, min(100, score)))
    detail = f'Short {short_pct*100:.1f}% of float, ratio {short_ratio:.1f}d'
    if ma50 > 0:
        detail += f', price {"above" if price > ma50 else "below"} 50MA'

    return {'score': score, 'detail': detail, 'short_pct': round(short_pct * 100, 1)}


# ── WHAL-85: Congressional Score ─────────────────────────────────

def score_congress(ticker: str) -> dict:
    """
    Score 0–100 based on recent congressional trades.
    Multiple senior members buying = strong signal.
    """
    try:
        r = requests.get(
            'https://api.quiverquant.com/beta/live/congresstrading',
            headers={'Accept': 'application/json'},
            timeout=10,
        )
        all_trades = r.json() if r.status_code == 200 else []
    except Exception:
        all_trades = []

    # Filter to this ticker within last 90 days
    cutoff = datetime.now(timezone.utc) - timedelta(days=90)
    ticker_upper = ticker.upper()
    recent = []
    for t in (all_trades or []):
        if (t.get('Ticker') or '').upper() != ticker_upper:
            continue
        try:
            d = datetime.strptime(
                t.get('TransactionDate') or t.get('ReportDate', '2000-01-01'),
                '%Y-%m-%d'
            ).replace(tzinfo=timezone.utc)
            if d >= cutoff:
                recent.append(t)
        except Exception:
            pass

    if not recent:
        return {'score': 50, 'detail': 'No recent congressional trades', 'buys': 0, 'sells': 0}

    buys = [t for t in recent if 'purchase' in (t.get('Transaction') or '').lower() or 'buy' in (t.get('Transaction') or '').lower()]
    sells = [t for t in recent if 'sale' in (t.get('Transaction') or '').lower()]

    if not buys and not sells:
        score = 50
    elif buys and not sells:
        score = min(100, 65 + len(buys) * 8)
    elif sells and not buys:
        score = max(0, 35 - len(sells) * 8)
    else:
        score = round(50 + (len(buys) - len(sells)) / (len(buys) + len(sells)) * 40)

    # Senate buys weighted higher
    senate_buys = [t for t in buys if (t.get('House') or '') == 'Senate']
    if senate_buys:
        score = min(100, score + 5)

    members = list(set(t.get('Representative', '') for t in recent if t.get('Representative')))
    detail = f'{len(buys)} buys, {len(sells)} sells by {len(members)} member(s)'
    return {'score': round(score), 'detail': detail, 'buys': len(buys), 'sells': len(sells)}


# ── WHAL-86: Sentiment Score ──────────────────────────────────────

def score_sentiment(ticker: str) -> dict:
    """
    Score 0–100 from Reddit/social sentiment via existing sentiment endpoint logic.
    Uses PRAW-based scoring if available, otherwise returns neutral.
    """
    try:
        r = requests.get(
            f'http://localhost:5000/api/sentiment/{ticker.upper()}',
            timeout=5,
        )
        if r.status_code == 200:
            data = r.json()
            raw = float(data.get('score') or data.get('sentiment_score') or 0.5)
            # sentiment score is 0-1, convert to 0-100
            score = round(raw * 100)
            mentions = data.get('mentions', 0)
            bullish_pct = data.get('bullish_pct', round(raw * 100))
            return {
                'score': score,
                'detail': f'{bullish_pct:.0f}% bullish, {mentions} mentions',
                'raw_score': raw,
            }
    except Exception:
        pass

    # Fallback: neutral sentiment
    return {'score': 50, 'detail': 'Sentiment data unavailable', 'raw_score': 0.5}


# ── WHAL-87: Claude Narrative ─────────────────────────────────────

def generate_narrative(ticker: str, company: str, scores: dict, total: float, grade: str) -> str:
    """
    Generate a 2-3 sentence plain English narrative using Claude API.
    """
    key = ANTHROPIC_KEY
    if not key:
        return _fallback_narrative(ticker, scores, total, grade)

    insider = scores.get('insider', {})
    options = scores.get('options', {})
    congress = scores.get('congress', {})
    sentiment = scores.get('sentiment', {})

    prompt = f"""You are a concise financial analyst. Write exactly 2 sentences summarizing the Whale Score for {company} ({ticker}).

Whale Score: {total:.0f}/100 (Grade: {grade})
- Insider Flow: {insider.get('score', 50)}/100 — {insider.get('detail', '')}
- Options Flow: {options.get('score', 50)}/100 — {options.get('detail', '')}
- Congressional: {congress.get('score', 50)}/100 — {congress.get('detail', '')}
- Sentiment: {sentiment.get('score', 50)}/100 — {sentiment.get('detail', '')}

Be direct, factual, no disclaimers. Start with the most significant signal. Second sentence gives the outlook."""

    try:
        r = requests.post(
            'https://api.anthropic.com/v1/messages',
            headers={
                'x-api-key': key,
                'anthropic-version': '2023-06-01',
                'content-type': 'application/json',
            },
            json={
                'model': 'claude-haiku-4-5-20251001',
                'max_tokens': 120,
                'messages': [{'role': 'user', 'content': prompt}],
            },
            timeout=12,
        )
        if r.status_code == 200:
            return r.json()['content'][0]['text'].strip()
    except Exception as e:
        logger.error(f'Claude narrative error {ticker}: {e}')

    return _fallback_narrative(ticker, scores, total, grade)


def _fallback_narrative(ticker: str, scores: dict, total: float, grade: str) -> str:
    insider_s = scores.get('insider', {}).get('score', 50)
    opts_s = scores.get('options', {}).get('score', 50)
    cong_s = scores.get('congress', {}).get('score', 50)

    if total >= 70:
        tone = 'bullish'
        outlook = 'Multiple signals align for upside momentum.'
    elif total >= 50:
        tone = 'mixed'
        outlook = 'Watch for confirmation before entering.'
    else:
        tone = 'bearish'
        outlook = 'Signals suggest caution or reduced exposure.'

    strongest = max(scores, key=lambda k: scores[k].get('score', 50))
    return (
        f"{ticker} shows a {tone} composite signal (Grade {grade}) led by "
        f"{strongest.replace('darkpool', 'dark pool')} activity. {outlook}"
    )


# ── Composite score ───────────────────────────────────────────────

def compute_vector_score(ticker: str, company: str = '') -> dict:
    """
    Run all 5 scoring dimensions and return the full VectorScore payload.
    """
    ticker = ticker.upper()
    if not company:
        company = ticker

    insider  = score_insider(ticker)
    options  = score_options(ticker)
    darkpool = score_darkpool(ticker)
    congress = score_congress(ticker)
    sentiment = score_sentiment(ticker)

    scores = {
        'insider':   insider,
        'options':   options,
        'darkpool':  darkpool,
        'congress':  congress,
        'sentiment': sentiment,
    }

    total = round(
        insider['score']   * WEIGHTS['insider'] +
        options['score']   * WEIGHTS['options'] +
        darkpool['score']  * WEIGHTS['darkpool'] +
        congress['score']  * WEIGHTS['congress'] +
        sentiment['score'] * WEIGHTS['sentiment'],
        1
    )

    grade = whale_grade(total)
    narrative = generate_narrative(ticker, company, scores, total, grade)

    return {
        'ticker':    ticker,
        'company':   company,
        'total_score': total,
        'grade':     grade,
        'narrative': narrative,
        'dimensions': {
            'insider':   {'score': insider['score'],   'detail': insider['detail'],   'weight': WEIGHTS['insider']},
            'options':   {'score': options['score'],   'detail': options['detail'],   'weight': WEIGHTS['options']},
            'darkpool':  {'score': darkpool['score'],  'detail': darkpool['detail'],  'weight': WEIGHTS['darkpool']},
            'congress':  {'score': congress['score'],  'detail': congress['detail'],  'weight': WEIGHTS['congress']},
            'sentiment': {'score': sentiment['score'], 'detail': sentiment['detail'], 'weight': WEIGHTS['sentiment']},
        },
        'generated_at': datetime.now(timezone.utc).isoformat(),
    }
