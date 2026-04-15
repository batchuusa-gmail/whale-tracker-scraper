"""
WHAL-105 — LLM Earnings Transcript NLP (Qualitative Surprise Detector)

Fetches earnings call transcripts from FMP, sends to Claude Haiku for analysis:
  sentiment_score  : -1.0 (very bearish) to +1.0 (very bullish)
  guidance_tone    : 'raised' | 'maintained' | 'lowered' | 'withdrawn'
  qualitative_surprise : 'BEAT' | 'MISS' | 'NEUTRAL'
  risk_flags       : list of bearish language snippets
  demand_signals   : list of bullish demand language snippets
  reasoning        : 2-3 sentence AI summary

Combines with PEAD SUE score (WHAL-100) to produce NLP-enhanced drift conviction.

Cost: ~$0.001 per transcript with Claude Haiku — essentially free.
"""

import os
import json
import logging
import threading
from datetime import datetime, timezone, timedelta

import requests
import anthropic

logger = logging.getLogger(__name__)

FMP_BASE      = 'https://financialmodelingprep.com/stable'
FMP_KEY       = os.environ.get('FMP_API_KEY', '')
ANTHROPIC_KEY = os.environ.get('ANTHROPIC_API_KEY', '')

# Tickers to scan on refresh (earnings-calendar driven in production)
SCAN_UNIVERSE = [
    'AAPL', 'MSFT', 'NVDA', 'AMZN', 'GOOGL', 'META', 'TSLA', 'AMD', 'PLTR', 'COIN',
    'JPM', 'GS', 'BAC', 'V', 'MA', 'LLY', 'MRNA', 'PFE', 'XOM', 'CVX',
    'CRWD', 'PANW', 'NET', 'CRM', 'SNOW', 'MU', 'INTC', 'QCOM', 'AVGO', 'SOFI',
]

CACHE_TTL = 86400   # 24 h — transcripts don't change

_state_lock = threading.Lock()
_state = {
    'scores':       [],
    'last_updated': None,
    'error':        None,
    'refreshing':   False,
}

# In-process cache per ticker
_ticker_cache: dict[str, dict] = {}


# ── FMP transcript fetch ──────────────────────────────────────────────

def _fetch_transcript(ticker: str) -> str | None:
    """Fetch latest 8-K earnings text from SEC EDGAR submissions API (free, reliable)."""
    import re as _re
    _hdrs = {'User-Agent': 'WhaleTracker research@whaletracker.app'}

    # CIK map for our scan universe — avoids ticker→CIK lookup API call
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
    }

    ticker = ticker.upper()
    cik = _CIK_MAP.get(ticker)
    if not cik:
        logger.debug(f'_fetch_transcript: no CIK mapping for {ticker}')
        return None

    try:
        # 1. Get recent filings list
        r = requests.get(
            f'https://data.sec.gov/submissions/CIK{cik}.json',
            headers=_hdrs, timeout=10,
        )
        if r.status_code != 200:
            return None

        filings = r.json().get('filings', {}).get('recent', {})
        forms       = filings.get('form', [])
        accessions  = filings.get('accessionNumber', [])
        docs        = filings.get('primaryDocument', [])

        # 2. Find most recent 8-K with item 2.02 (Results of Operations = earnings)
        items_list  = filings.get('items', [])
        accession = primary_doc = None
        for i, form in enumerate(forms[:200]):
            if form == '8-K' and '2.02' in str(items_list[i] if i < len(items_list) else ''):
                accession   = accessions[i]
                primary_doc = docs[i]
                break

        if not accession or not primary_doc:
            return None

        # 3. Fetch the document
        cik_short = cik.lstrip('0')
        adsh      = accession.replace('-', '')
        doc_url   = f'https://www.sec.gov/Archives/edgar/data/{cik_short}/{adsh}/{primary_doc}'
        doc_r = requests.get(doc_url, headers=_hdrs, timeout=15)
        if doc_r.status_code != 200:
            return None

        # 4. Strip HTML and return clean text
        text = _re.sub(r'<[^>]+>', ' ', doc_r.text)
        text = _re.sub(r'\s+', ' ', text).strip()
        return text[:8000] if len(text) > 100 else None

    except Exception as e:
        logger.warning(f'_fetch_transcript EDGAR {ticker}: {e}')
    return None


def _fetch_earnings_calendar_today() -> list[str]:
    """Return scan universe — always process all known tickers, cache handles staleness."""
    return SCAN_UNIVERSE


# ── Claude Haiku analysis ─────────────────────────────────────────────

def _analyze_with_haiku(ticker: str, transcript: str) -> dict | None:
    """Send transcript excerpt to Claude Haiku and parse structured JSON response."""
    if not ANTHROPIC_KEY:
        logger.warning('ANTHROPIC_API_KEY not set — cannot analyze transcript')
        return None

    # Limit to ~5000 chars to keep tokens low (prepared remarks are most signal-dense)
    excerpt = transcript[:5000]

    prompt = f"""You are an expert equity analyst specializing in earnings call tone analysis.

Analyze this earnings call transcript for {ticker} and return a JSON object with exactly these fields:

{{
  "sentiment_score": <float -1.0 to 1.0, where -1 is very bearish, 0 is neutral, 1 is very bullish>,
  "guidance_tone": <"raised" | "maintained" | "lowered" | "withdrawn" | "not_provided">,
  "qualitative_surprise": <"BEAT" | "MISS" | "NEUTRAL" — tone vs. typical corporate language>,
  "demand_signals": <list of up to 5 specific bullish demand/growth quotes or observations>,
  "risk_flags": <list of up to 5 specific bearish/cautionary phrases or concerns>,
  "confidence": <float 0.0 to 1.0 — how confident you are in the analysis given transcript quality>,
  "reasoning": <2-3 sentence plain-English summary of the overall tone and key takeaways>
}}

Transcript excerpt:
{excerpt}

Return ONLY valid JSON, no markdown, no explanation outside the JSON."""

    try:
        client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)
        message = client.messages.create(
            model='claude-haiku-4-5-20251001',
            max_tokens=600,
            messages=[{'role': 'user', 'content': prompt}],
        )
        raw = message.content[0].text.strip()
        # Strip markdown code fences if present
        if raw.startswith('```'):
            raw = raw.split('```')[1]
            if raw.startswith('json'):
                raw = raw[4:]
        return json.loads(raw)
    except json.JSONDecodeError as e:
        logger.warning(f'Haiku JSON parse error for {ticker}: {e}')
    except Exception as e:
        logger.warning(f'Haiku analysis error for {ticker}: {e}')
    return None


# ── SUE integration ───────────────────────────────────────────────────

def _get_sue_score(ticker: str) -> float | None:
    """Pull SUE score from PEAD engine if available."""
    try:
        from pead_engine import score_ticker as pead_score_ticker
        result = pead_score_ticker(ticker)
        if result:
            return result.get('sue')
    except Exception:
        pass
    return None


def _combined_conviction(sentiment_score: float, qualitative_surprise: str,
                          sue: float | None) -> int:
    """
    Combine NLP sentiment with quantitative SUE (if available) into 0-100 conviction.
    Positive = bullish drift, negative = bearish (use abs for display).
    """
    # NLP contribution: 70%
    nlp_score = sentiment_score * 70

    # SUE boost/penalty: 30% (clamp sue to ±3 sigma)
    sue_contribution = 0
    if sue is not None:
        sue_clamped = max(-3.0, min(3.0, sue))
        sue_contribution = (sue_clamped / 3.0) * 30

    raw = nlp_score + sue_contribution
    return int(max(0, min(100, abs(raw))))


# ── Per-ticker scorer ─────────────────────────────────────────────────

def analyze_ticker(ticker: str) -> dict | None:
    """Fetch transcript + run Haiku analysis + combine with SUE. Cached 24h."""
    ticker = ticker.upper()

    # Check in-process cache
    cached = _ticker_cache.get(ticker)
    if cached:
        try:
            age = (datetime.now(timezone.utc) - datetime.fromisoformat(cached['analyzed_at'])).total_seconds()
            if age < CACHE_TTL:
                return cached
        except Exception:
            pass

    transcript = _fetch_transcript(ticker)
    if not transcript:
        return None

    analysis = _analyze_with_haiku(ticker, transcript)
    if not analysis:
        return None

    sue = _get_sue_score(ticker)
    sentiment = float(analysis.get('sentiment_score', 0))
    qual_surprise = analysis.get('qualitative_surprise', 'NEUTRAL')
    conviction = _combined_conviction(sentiment, qual_surprise, sue)

    result = {
        'ticker':               ticker,
        'sentiment_score':      round(sentiment, 3),
        'sentiment_label':      _sentiment_label(sentiment),
        'guidance_tone':        analysis.get('guidance_tone', 'not_provided'),
        'qualitative_surprise': qual_surprise,
        'demand_signals':       analysis.get('demand_signals', []),
        'risk_flags':           analysis.get('risk_flags', []),
        'confidence':           round(float(analysis.get('confidence', 0)), 2),
        'reasoning':            analysis.get('reasoning', ''),
        'sue':                  sue,
        'conviction':           conviction,
        'pead_enhanced_signal': _pead_signal(sentiment, sue, qual_surprise),
        'analyzed_at':          datetime.now(timezone.utc).isoformat(),
        'transcript_chars':     len(transcript),
    }

    _ticker_cache[ticker] = result
    return result


def _sentiment_label(score: float) -> str:
    if score >= 0.5:  return 'VERY BULLISH'
    if score >= 0.2:  return 'BULLISH'
    if score >= -0.2: return 'NEUTRAL'
    if score >= -0.5: return 'BEARISH'
    return 'VERY BEARISH'


def _pead_signal(sentiment: float, sue: float | None, qual_surprise: str) -> str:
    """
    Enhanced PEAD signal combining quantitative EPS surprise with qualitative tone.
    """
    # Both confirm = strong
    if qual_surprise == 'BEAT' and (sue is None or sue >= 1.0):
        return 'STRONG DRIFT UP'
    if qual_surprise == 'MISS' and (sue is None or sue <= -1.0):
        return 'STRONG DRIFT DOWN'

    # Divergence: EPS beat but cautious tone = weakened signal
    if qual_surprise == 'MISS' and sue is not None and sue >= 1.5:
        return 'EPS BEAT / TONE MISS — WEAKENED'
    if qual_surprise == 'BEAT' and sue is not None and sue <= -1.5:
        return 'EPS MISS / TONE BEAT — WEAKENED'

    if sentiment >= 0.2: return 'DRIFT UP'
    if sentiment <= -0.2: return 'DRIFT DOWN'
    return 'NEUTRAL'


# ── Batch scanner ─────────────────────────────────────────────────────

def scan_transcripts(tickers: list[str] = None) -> list[dict]:
    """Analyze a list of tickers, return results sorted by conviction."""
    universe = tickers or SCAN_UNIVERSE
    results = []
    for ticker in universe:
        try:
            score = analyze_ticker(ticker)
            if score:
                results.append(score)
        except Exception as e:
            logger.warning(f'transcript scan error {ticker}: {e}')
    results.sort(key=lambda x: -x['conviction'])
    return results


def scan_today_earners() -> list[dict]:
    """Fetch today's earnings calendar from FMP, analyze each transcript."""
    tickers = _fetch_earnings_calendar_today()
    if not tickers:
        logger.info('No earnings today (or FMP_API_KEY missing)')
        return []
    logger.info(f'Analyzing {len(tickers)} today earners: {tickers[:10]}...')
    return scan_transcripts(tickers[:30])  # cap at 30


# ── Cache layer ───────────────────────────────────────────────────────

def get_scores(redis_get=None, redis_set=None) -> list[dict]:
    """Always returns immediately — never blocks. Background thread fills cache."""
    cache_key = 'nlp:scores'

    # Redis hit — instant return
    if redis_get:
        cached = redis_get(cache_key)
        if cached:
            return cached

    with _state_lock:
        scores       = list(_state['scores'])
        last_updated = _state['last_updated']
        refreshing   = _state.get('refreshing', False)

    stale = True
    if last_updated:
        try:
            age   = (datetime.now(timezone.utc) - datetime.fromisoformat(last_updated)).total_seconds()
            stale = age > CACHE_TTL
        except Exception:
            pass

    # Kick off background refresh if stale — never block
    if (stale or not scores) and not refreshing:
        import threading as _threading
        _threading.Thread(target=refresh_scores, daemon=True, name='nlp-refresh').start()

    if redis_set and scores:
        redis_set(cache_key, scores, ttl_seconds=CACHE_TTL)

    return scores


def get_ticker_analysis(ticker: str) -> dict | None:
    return analyze_ticker(ticker)


def refresh_scores(redis_set=None):
    with _state_lock:
        if _state.get('refreshing'):
            return
        _state['refreshing'] = True
    logger.info('Transcript NLP: scanning earners…')
    try:
        scores = scan_today_earners()
        if not scores:
            scores = scan_transcripts(SCAN_UNIVERSE)
        with _state_lock:
            _state['scores']       = scores
            _state['last_updated'] = datetime.now(timezone.utc).isoformat()
            _state['error']        = None
            _state['refreshing']   = False
        if redis_set and scores:
            redis_set('nlp:scores', scores, ttl_seconds=CACHE_TTL)
        logger.info(f'Transcript NLP: {len(scores)} analyses complete')
    except Exception as e:
        logger.error(f'Transcript NLP refresh error: {e}')
        with _state_lock:
            _state['error']      = str(e)
            _state['refreshing'] = False


# alias used by data warmer
refresh_nlp_scores = refresh_scores
