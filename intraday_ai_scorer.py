"""
WHAL-95 — Intraday AI Scoring Engine
Uses Claude API to evaluate each scanner pick (score >= 60) and generate
a final BUY/SELL/SKIP with entry, stop loss, target, and hold_minutes.

Kill switch: INTRADAY_AI_ENABLED=true/false (Railway env var)
Rate limits:
  - Max 10 Claude calls/minute
  - Only call for scanner score >= 60
  - Cache 5 minutes per ticker
  - Token counter enforces max $5/day spend
"""

import os
import json
import logging
import time
import threading
from datetime import datetime, timedelta

import requests

logger = logging.getLogger(__name__)

ANTHROPIC_KEY     = os.environ.get('ANTHROPIC_API_KEY', '')
MIN_SCANNER_SCORE = 60        # only score stocks above this
MIN_CONFIDENCE    = 0.70      # raised from 0.65 → only high-conviction trades
MAX_CALLS_PER_MIN = 10        # rate limit
MAX_TOKENS_DAY    = 500_000   # ~$5/day at Haiku pricing

# Intraday price targets
TARGET_1_PCT  =  0.025   # +2.5% first target (partial exit)
TARGET_2_PCT  =  0.040   # +4.0% full target
STOP_LOSS_PCT =  0.015   # -1.5% (widened from -0.8% to survive normal noise)
MAX_HOLD_MINS =  120     # never hold >2 hours

# ── Rate limiter ─────────────────────────────────────────────────

_rate_lock    = threading.Lock()
_call_times: list[float] = []
_tokens_today = 0
_tokens_date  = ''


def _can_call_claude() -> tuple[bool, str]:
    """Return (allowed, reason). Enforces per-minute and daily token limits."""
    global _tokens_today, _tokens_date

    if not os.environ.get('INTRADAY_AI_ENABLED', 'false').lower() == 'true':
        return False, 'INTRADAY_AI_ENABLED is not true'

    key = os.environ.get('ANTHROPIC_API_KEY', '') or ANTHROPIC_KEY
    if not key:
        return False, 'ANTHROPIC_API_KEY not set'

    now = time.time()
    today = datetime.utcnow().strftime('%Y-%m-%d')

    with _rate_lock:
        # Reset daily token counter on new day
        if _tokens_date != today:
            _tokens_today = 0
            _tokens_date  = today

        if _tokens_today >= MAX_TOKENS_DAY:
            return False, f'Daily token limit reached ({MAX_TOKENS_DAY:,})'

        # Per-minute rate limit: keep only calls within last 60s
        global _call_times
        _call_times = [t for t in _call_times if now - t < 60]
        if len(_call_times) >= MAX_CALLS_PER_MIN:
            return False, f'Rate limit: {MAX_CALLS_PER_MIN} calls/min'

    return True, 'ok'


def _record_call(tokens_used: int):
    with _rate_lock:
        global _tokens_today
        _call_times.append(time.time())
        _tokens_today += tokens_used


# ── Supabase logging ─────────────────────────────────────────────

_SUPA_URL = os.environ.get('SUPABASE_URL', 'https://bedurjtazsfbnkisoeee.supabase.co')
_SUPA_KEY = os.environ.get(
    'SUPABASE_KEY',
    'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJlZHVyanRhenNmYm5raXNvZWVlIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzM2NDcxOTUsImV4cCI6MjA4OTIyMzE5NX0.MK4N9dxAIHlXkGP4rLJPq5tHh9UU8L75EB1b8Q7CVmg'
)
_SUPA_HEADERS = {
    'apikey': _SUPA_KEY,
    'Authorization': f'Bearer {_SUPA_KEY}',
    'Content-Type': 'application/json',
    'Prefer': 'resolution=merge-duplicates,return=minimal',
}


def _log_score(row: dict):
    try:
        requests.post(
            f'{_SUPA_URL}/rest/v1/intraday_scores',
            headers=_SUPA_HEADERS,
            json=row,
            timeout=10,
        )
    except Exception as e:
        logger.warning(f'intraday_scores log error: {e}')


# ── Alpaca 1-min bars (30 bars = 30 minutes of context) ─────────

_ALPACA_DATA = 'https://data.alpaca.markets'


def _fetch_bars(ticker: str, limit: int = 30) -> list[dict]:
    key    = os.environ.get('ALPACA_KEY', '')
    secret = os.environ.get('ALPACA_SECRET', '')
    if not key or not secret:
        return []
    try:
        r = requests.get(
            f'{_ALPACA_DATA}/v2/stocks/{ticker}/bars',
            headers={'APCA-API-KEY-ID': key, 'APCA-API-SECRET-KEY': secret},
            params={'timeframe': '1Min', 'limit': limit, 'feed': 'sip'},
            timeout=10,
        )
        if r.status_code == 200:
            return r.json().get('bars', [])
    except Exception as e:
        logger.warning(f'_fetch_bars({ticker}): {e}')
    return []


# ── Indicator calculations from bars ────────────────────────────

def _calc_indicators(bars: list[dict]) -> dict:
    if not bars:
        return {}
    closes  = [b['c'] for b in bars]
    volumes = [b['v'] for b in bars]
    highs   = [b['h'] for b in bars]
    lows    = [b['l'] for b in bars]
    cur     = closes[-1]
    prev    = closes[0]

    # RSI (14)
    rsi = None
    if len(closes) >= 15:
        gains, losses = [], []
        for i in range(1, len(closes)):
            d = closes[i] - closes[i-1]
            gains.append(max(d, 0))
            losses.append(max(-d, 0))
        p = 14
        ag = sum(gains[-p:]) / p
        al = sum(losses[-p:]) / p
        rsi = round(100 - (100 / (1 + ag / al)), 1) if al > 0 else 100.0

    # MACD signal (simplified)
    macd_signal = 'neutral'
    if len(closes) >= 15:
        def ema(data, span):
            k = 2 / (span + 1)
            e = data[0]
            for v in data[1:]:
                e = v * k + e * (1 - k)
            return e
        macd = ema(closes, 6) - ema(closes, 13)
        prev_macd = ema(closes[:-1], 6) - ema(closes[:-1], 13) if len(closes) > 2 else 0
        if macd > 0 and prev_macd <= 0:
            macd_signal = 'bullish_crossover'
        elif macd > 0:
            macd_signal = 'bullish'
        elif macd < 0:
            macd_signal = 'bearish'

    # Trend (5-min)
    trend = 'up' if cur > prev else 'down'
    change_pct = round((cur - prev) / prev * 100, 2) if prev > 0 else 0

    # Volume ratio
    avg_vol = sum(volumes[:-1]) / len(volumes[:-1]) if len(volumes) > 1 else 1
    vol_ratio = round(volumes[-1] / avg_vol, 2) if avg_vol > 0 else 0

    # Support / resistance
    resistance = round(max(highs), 2)
    support    = round(min(lows), 2)

    return {
        'current_price': round(cur, 2),
        'change_pct':    change_pct,
        'rsi':           rsi,
        'macd_signal':   macd_signal,
        'trend':         trend,
        'volume_ratio':  vol_ratio,
        'resistance':    resistance,
        'support':       support,
        'bars_count':    len(bars),
    }


# ── Claude API call ──────────────────────────────────────────────

def _call_claude(ticker: str, indicators: dict, scanner_score: int,
                 insider_summary: str, sp500_trend: str) -> dict:
    key = os.environ.get('ANTHROPIC_API_KEY', '') or ANTHROPIC_KEY
    price      = indicators.get('current_price', 0)
    change_pct = indicators.get('change_pct', 0)
    vol_ratio  = indicators.get('volume_ratio', 0)
    rsi        = indicators.get('rsi', 'N/A')
    macd       = indicators.get('macd_signal', 'N/A')
    trend      = indicators.get('trend', 'unknown')

    day_gain = indicators.get('day_gain_pct', 0)

    prompt = f"""You are an intraday trading analyst. Make precise, disciplined trade decisions.

Stock: {ticker}
Price: ${price:.2f} | Day gain so far: {day_gain:+.2f}% | 30-min change: {change_pct:+.2f}%
Volume: {vol_ratio:.1f}x average | Scanner score: {scanner_score}/100
RSI: {rsi} | MACD: {macd} | Trend: {trend}
Insider activity (last 30d): {insider_summary or 'none'}
S&P 500: {sp500_trend or 'neutral'}

Entry rules (ALL must be true to BUY):
1. Day gain < 3% — do NOT chase stocks already up big
2. RSI between 40–70 — momentum without being overbought
3. Volume > 1.5x average — confirms real interest
4. Trend is up AND MACD is bullish
5. Scanner score >= 75

Risk rules:
- Stop loss: -1.5% from entry (gives room for normal noise)
- Target 1: +2.5% (partial exit, lock in profit)
- Target 2: +4.0% (runner — only if strong momentum)
- Hold: 30–120 minutes. NEVER hold overnight.
- If ANY rule is violated → SKIP

Respond ONLY with valid JSON (no markdown):
{{
  "signal": "BUY" | "SKIP",
  "confidence": <float 0.0-1.0>,
  "entry": <float>,
  "stop_loss": <float>,
  "target_1": <float>,
  "target_2": <float>,
  "hold_minutes": <int 30-120>,
  "reasoning": "<1-2 sentence explanation of why BUY or SKIP>"
}}"""

    try:
        r = requests.post(
            'https://api.anthropic.com/v1/messages',
            headers={
                'x-api-key': key,
                'anthropic-version': '2023-06-01',
                'content-type': 'application/json',
            },
            json={
                'model':      'claude-haiku-4-5-20251001',
                'max_tokens': 300,
                'messages':   [{'role': 'user', 'content': prompt}],
            },
            timeout=20,
        )

        if not r.ok:
            logger.warning(f'Claude API error for {ticker}: {r.status_code}')
            return {}

        resp      = r.json()
        text      = resp['content'][0]['text'].strip()
        tokens    = resp.get('usage', {}).get('input_tokens', 0) + resp.get('usage', {}).get('output_tokens', 0)
        _record_call(tokens)

        # Strip markdown fences if present
        if text.startswith('```'):
            text = '\n'.join(text.split('\n')[1:]).rstrip('`').strip()

        result = json.loads(text)
        result['tokens_used'] = tokens
        # Normalise: if Claude returns old single 'target' field, map to target_1/target_2
        if 'target' in result and 'target_1' not in result:
            t = result.pop('target')
            result['target_1'] = t
            result['target_2'] = round(float(t) * (1 + TARGET_2_PCT - TARGET_1_PCT), 2)
        return result

    except Exception as e:
        logger.warning(f'_call_claude({ticker}): {e}')
        return {}


# ── Fallback: technical-only score ──────────────────────────────

def _technical_fallback(indicators: dict, scanner_score: int) -> dict:
    """Return a BUY/SKIP based purely on technicals when Claude is unavailable."""
    price     = indicators.get('current_price', 0)
    rsi       = indicators.get('rsi')
    macd      = indicators.get('macd_signal', '')
    trend     = indicators.get('trend', 'down')
    change    = indicators.get('change_pct', 0)
    day_gain  = indicators.get('day_gain_pct', 0)

    bullish = (
        scanner_score >= 75 and       # raised threshold
        trend == 'up' and
        change > 0.5 and
        day_gain < 3.0 and            # don't chase
        (rsi is None or 40 <= rsi <= 70) and
        'bullish' in macd
    )
    signal     = 'BUY' if bullish else 'SKIP'
    confidence = min(scanner_score / 100, 0.72)  # cap at 72% without AI

    stop = round(price * (1 - STOP_LOSS_PCT), 2)
    t1   = round(price * (1 + TARGET_1_PCT), 2)
    t2   = round(price * (1 + TARGET_2_PCT), 2)

    return {
        'signal':       signal,
        'confidence':   round(confidence, 2),
        'entry':        round(price, 2),
        'stop_loss':    stop,
        'target_1':     t1,
        'target_2':     t2,
        'hold_minutes': 60,
        'reasoning':    f'Technical-only fallback (AI disabled). Score {scanner_score}/100, trend {trend}, day gain {day_gain:.1f}%.',
        'source':       'technical_fallback',
    }


# ── Public API ───────────────────────────────────────────────────

def score_stock(
    ticker: str,
    scanner_score: int = 0,
    bars: list[dict] | None = None,
    insider_summary: str = '',
    sp500_trend: str = 'neutral',
    redis_get=None,
    redis_set=None,
) -> dict:
    """
    Score a single ticker for intraday trading.

    Returns dict with: signal, confidence, entry, stop_loss, target,
    hold_minutes, reasoning, source, scored_at.
    """
    ticker = ticker.upper().strip()

    # Skip low-score stocks
    if scanner_score < MIN_SCANNER_SCORE:
        return {
            'ticker':     ticker,
            'signal':     'SKIP',
            'confidence': 0,
            'reasoning':  f'Scanner score {scanner_score} below threshold {MIN_SCANNER_SCORE}',
            'source':     'threshold_filter',
            'scored_at':  datetime.utcnow().isoformat(),
        }

    # Redis cache check (5-minute TTL)
    cache_key = f'intraday:score:{ticker}'
    if redis_get:
        cached = redis_get(cache_key)
        if cached:
            return cached

    # Fetch bars if not provided
    if bars is None:
        bars = _fetch_bars(ticker, limit=30)

    indicators = _calc_indicators(bars)
    if not indicators:
        indicators = {'current_price': 0}

    # Try Claude, fall back to technicals
    allowed, reason = _can_call_claude()
    if allowed:
        result = _call_claude(ticker, indicators, scanner_score, insider_summary, sp500_trend)
        source = 'claude_ai'
    else:
        logger.info(f'Claude unavailable for {ticker}: {reason}')
        result = {}
        source = 'technical_fallback'

    if not result or 'signal' not in result:
        result = _technical_fallback(indicators, scanner_score)
        source = 'technical_fallback'
    else:
        result['source'] = source

    # Apply hard intraday price level rules
    price = indicators.get('current_price', 0)
    if price > 0:
        if not result.get('stop_loss') or result['stop_loss'] <= 0:
            result['stop_loss'] = round(price * (1 - STOP_LOSS_PCT), 2)
        if not result.get('target') or result['target'] <= 0:
            result['target'] = round(price * (1 + TARGET_1_PCT), 2)
        if not result.get('entry') or result['entry'] <= 0:
            result['entry'] = round(price, 2)
        if not result.get('hold_minutes'):
            result['hold_minutes'] = 60
        result['hold_minutes'] = min(int(result['hold_minutes']), MAX_HOLD_MINS)

    # Enrich
    result['ticker']        = ticker
    result['scanner_score'] = scanner_score
    result['indicators']    = indicators
    result['scored_at']     = datetime.utcnow().isoformat()
    result.setdefault('source', source)

    # Cache 5 min
    if redis_set:
        redis_set(cache_key, result, ttl_seconds=300)

    # Log to Supabase (async-safe — fire and forget)
    _log_score({
        'ticker':         ticker,
        'signal':         result.get('signal'),
        'confidence':     result.get('confidence'),
        'entry':          result.get('entry'),
        'stop_loss':      result.get('stop_loss'),
        'target':         result.get('target'),
        'hold_minutes':   result.get('hold_minutes'),
        'scanner_score':  scanner_score,
        'source':         result.get('source'),
        'reasoning':      result.get('reasoning', '')[:500],
        'scored_at':      result['scored_at'],
    })

    return result


def get_daily_token_usage() -> dict:
    """Return current day's token usage stats."""
    with _rate_lock:
        return {
            'tokens_today':   _tokens_today,
            'max_tokens_day': MAX_TOKENS_DAY,
            'pct_used':       round(_tokens_today / MAX_TOKENS_DAY * 100, 1),
            'calls_last_min': len([t for t in _call_times if time.time() - t < 60]),
            'date':           _tokens_date,
        }
