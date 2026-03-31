"""
WHAL-92 — Combined Signal Score
Merges insider conviction (from AI) with technical confirmation to produce
a single final_score that drives all paper and live trade decisions.

Formula:
  final_score = (INSIDER_WEIGHT * insider_confidence_0_1)
              + (TECHNICAL_WEIGHT * technical_score_0_1)

  insider_confidence_0_1 = confidence / 100          (Claude returns 0-100)
  technical_score_0_1    = (technical_score + 1) / 2  (WHAL-91 returns -1 to +1)

Weights are configurable via Railway env vars:
  INSIDER_WEIGHT      default 0.6
  TECHNICAL_WEIGHT    default 0.4
  MIN_SCORE_TO_TRADE  default 0.85
"""

import os
import logging

logger = logging.getLogger(__name__)


def _w_insider() -> float:
    return float(os.environ.get('INSIDER_WEIGHT', '0.6'))


def _w_technical() -> float:
    return float(os.environ.get('TECHNICAL_WEIGHT', '0.4'))


def _min_score() -> float:
    return float(os.environ.get('MIN_SCORE_TO_TRADE', '0.85'))


def combine_signals(insider_result: dict, technical_result: dict) -> dict:
    """
    Combine insider AI signal with technical indicator score.

    Args:
        insider_result:  dict from /api/ai/signal — must have 'confidence' (0-100)
                         and 'signal' (STRONG BUY / BUY / NEUTRAL / SELL / STRONG SELL)
        technical_result: dict from get_technical_score() — must have 'technical_score'
                          (-1.0 to +1.0) and 'technical_signal' (BULLISH/NEUTRAL/BEARISH)

    Returns dict with:
        insider_signal, insider_confidence (0-1), insider_confidence_pct (0-100),
        technical_signal, technical_score (-1 to +1), technical_score_0_1 (0-1),
        final_score (0-1), action, reasoning, weights used
    """
    w_ins  = _w_insider()
    w_tech = _w_technical()
    min_s  = _min_score()

    # ── Insider inputs ──────────────────────────────────────────
    insider_signal = (insider_result.get('signal') or 'NEUTRAL').upper()
    confidence_pct = float(insider_result.get('confidence') or 0)
    insider_conf   = confidence_pct / 100.0

    # ── Technical inputs ────────────────────────────────────────
    tech_score_raw   = float(technical_result.get('technical_score') or 0)   # -1 to +1
    tech_score_01    = (tech_score_raw + 1.0) / 2.0                           # 0 to 1
    technical_signal = (technical_result.get('technical_signal') or 'NEUTRAL').upper()

    # ── Combined score ──────────────────────────────────────────
    final_score = round(w_ins * insider_conf + w_tech * tech_score_01, 3)

    # ── Action decision ─────────────────────────────────────────
    # Only execute on BUY signals — never execute SELL/NEUTRAL regardless of score
    is_buy_signal = insider_signal in ('STRONG BUY', 'BUY')

    if is_buy_signal and final_score >= min_s:
        action = 'EXECUTE_TRADE'
    elif is_buy_signal and final_score >= 0.65:
        action = 'WATCH_ONLY'
    else:
        action = 'SKIP'

    # ── Human-readable reasoning ─────────────────────────────────
    insider_reasoning = insider_result.get('reasoning', '')
    tech_votes_bull   = technical_result.get('votes_bullish', 0)
    tech_votes_bear   = technical_result.get('votes_bearish', 0)
    rsi               = technical_result.get('rsi')
    macd_sig          = technical_result.get('macd_signal', '')
    vol_ratio         = technical_result.get('volume_ratio')

    tech_summary_parts = []
    if rsi is not None:
        tech_summary_parts.append(f'RSI {rsi:.0f}')
    if macd_sig:
        tech_summary_parts.append(f'MACD {macd_sig.replace("_", " ")}')
    if vol_ratio is not None:
        tech_summary_parts.append(f'volume {vol_ratio:.1f}× avg')
    tech_summary = ', '.join(tech_summary_parts) if tech_summary_parts else 'technicals checked'

    combined_reasoning = (
        f'{insider_reasoning} '
        f'Technical confirmation: {technical_signal} ({tech_votes_bull} bullish / {tech_votes_bear} bearish votes — {tech_summary}). '
        f'Combined score: {final_score:.2f} → {action.replace("_", " ")}.'
    ).strip()

    return {
        # Insider
        'insider_signal':          insider_signal,
        'insider_confidence':      round(insider_conf, 3),
        'insider_confidence_pct':  int(confidence_pct),

        # Technical
        'technical_signal':        technical_signal,
        'technical_score':         round(tech_score_raw, 3),
        'technical_score_0_1':     round(tech_score_01, 3),
        'technical_votes_bullish': tech_votes_bull,
        'technical_votes_bearish': tech_votes_bear,

        # Combined
        'final_score':             final_score,
        'action':                  action,
        'combined_reasoning':      combined_reasoning,

        # Config used
        'insider_weight':          w_ins,
        'technical_weight':        w_tech,
        'min_score_to_trade':      min_s,

        # Price levels from insider signal
        'suggested_entry':         insider_result.get('entry_price'),
        'dynamic_stop_loss':       technical_result.get('dynamic_stop_loss') or insider_result.get('stop_loss'),
        'suggested_target':        insider_result.get('target_price'),
        'hold_days':               insider_result.get('hold_days'),
    }
