"""
WHAL-131 — Algo Engine Loop: signal evaluation + trade decisions
WHAL-132 — Safety circuit breaker: max daily trades, max daily loss, hard kill

Architecture:
  - Runs as a background thread every EVAL_INTERVAL_SECS (90 s)
  - Reads top scanner picks from intraday_scanner._state (in-memory, fresh every 60 s)
  - Scores each candidate with intraday_ai_scorer.score_stock()
  - Applies circuit breaker BEFORE placing any order
  - Calls intraday_executor.place_intraday_order() for execution
  - Writes to BOTH intraday_trades (executor) AND algo_trades (new — for AlgoTradingScreen)
  - Updates algo_state Supabase row after every eval cycle

Kill switch:  INTRADAY_TRADING_ENABLED=true  (Railway env var, same as executor)
App kill:     algo_config.enabled = false     (Supabase, written by AlgoTradingScreen toggle)
Circuit:      trips on max_daily_trades OR max_daily_loss — auto-resets next market day
"""
from __future__ import annotations

import os
import json
import logging
import threading
import time
import uuid
from datetime import datetime, timezone, timedelta

import pytz
import requests

logger = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────────
EVAL_INTERVAL_SECS   = 90       # how often the engine wakes up
MAX_DAILY_LOSS_PCT   = 0.02     # 2% of portfolio — circuit trips if daily P&L < -2%
DEFAULT_MIN_SCORE    = 65       # fallback if algo_config not readable
DEFAULT_MIN_CONF     = 0.70     # fallback confidence threshold
DEFAULT_MAX_TRADES   = 3        # fallback max trades per day

_ET = pytz.timezone('America/New_York')

# ── Supabase ────────────────────────────────────────────────────────────────────
_SUPA_URL = os.environ.get('SUPABASE_URL', 'https://bedurjtazsfbnkisoeee.supabase.co')
_SUPA_KEY = os.environ.get('SUPABASE_KEY', '')
_SUPA_HDRS = {
    'apikey': _SUPA_KEY,
    'Authorization': f'Bearer {_SUPA_KEY}',
    'Content-Type': 'application/json',
    'Prefer': 'resolution=merge-duplicates,return=minimal',
}


def _sb_get(table: str, query: str = '') -> list:
    try:
        r = requests.get(
            f'{_SUPA_URL}/rest/v1/{table}?{query}',
            headers=_SUPA_HDRS, timeout=8,
        )
        if r.status_code == 200:
            return r.json() if isinstance(r.json(), list) else []
    except Exception as e:
        logger.warning(f'_sb_get {table}: {e}')
    return []


def _sb_upsert(table: str, rows: list):
    if not rows:
        return
    try:
        r = requests.post(
            f'{_SUPA_URL}/rest/v1/{table}',
            headers=_SUPA_HDRS, json=rows, timeout=10,
        )
        if r.status_code not in (200, 201, 204):
            logger.warning(f'_sb_upsert {table}: {r.status_code} {r.text[:200]}')
    except Exception as e:
        logger.warning(f'_sb_upsert {table}: {e}')


# ── State ────────────────────────────────────────────────────────────────────────
_engine_lock   = threading.Lock()
_circuit_open  = False          # True = no more trades today
_circuit_reason = ''
_current_date  = ''             # YYYY-MM-DD, reset resets circuit


# ── Config reader ────────────────────────────────────────────────────────────────
def _load_config() -> dict:
    """Read algo_config from Supabase. Falls back to defaults on error."""
    rows = _sb_get('algo_config', 'id=eq.1&limit=1')
    if rows:
        return rows[0]
    return {
        'enabled':            True,
        'aggression':         0.5,
        'min_score':          DEFAULT_MIN_SCORE,
        'min_confidence':     DEFAULT_MIN_CONF * 100,
        'max_trades_per_day': DEFAULT_MAX_TRADES,
    }


# ── State writer ─────────────────────────────────────────────────────────────────
def _update_state(patch: dict):
    """Upsert algo_state row (id=1) with patch fields."""
    row = {'id': 1, 'updated_at': datetime.now(timezone.utc).isoformat(), **patch}
    _sb_upsert('algo_state', [row])


# ── Trades-today counter ──────────────────────────────────────────────────────────
def _trades_today_count() -> int:
    """Count algo_trades placed today (from algo_trades table, not intraday_trades)."""
    today = datetime.now(_ET).strftime('%Y-%m-%d')
    rows  = _sb_get('algo_trades', f'trade_date=eq.{today}')
    return len(rows)


def _daily_pnl(equity: float) -> float:
    """
    Return today's total P&L from intraday_trades (closed trades).
    Returns fraction of equity (e.g. -0.015 = -1.5%).
    """
    today  = datetime.now(_ET).strftime('%Y-%m-%d')
    rows   = _sb_get('intraday_trades', f'date=eq.{today}&status=eq.closed')
    total  = sum(float(r.get('pnl') or 0) for r in rows)
    return total / equity if equity > 0 else 0.0


# ── Circuit breaker ───────────────────────────────────────────────────────────────
def _check_circuit(config: dict, equity: float) -> tuple[bool, str]:
    """
    WHAL-132: Returns (tripped: bool, reason: str).
    Checks:
      1. Engine disabled via app toggle
      2. max_trades_per_day exhausted
      3. daily P&L < -MAX_DAILY_LOSS_PCT of equity
    """
    global _circuit_open, _circuit_reason

    # App toggle
    if not config.get('enabled', True):
        return True, 'disabled_via_app'

    # Already tripped — persist until day reset
    if _circuit_open:
        return True, _circuit_reason

    # Max trades
    max_trades = int(config.get('max_trades_per_day') or DEFAULT_MAX_TRADES)
    done_today = _trades_today_count()
    if done_today >= max_trades:
        reason = f'max_trades_reached ({done_today}/{max_trades})'
        with _engine_lock:
            _circuit_open   = True
            _circuit_reason = reason
        logger.warning(f'Circuit tripped: {reason}')
        return True, reason

    # Max daily loss
    pnl_pct = _daily_pnl(equity)
    if pnl_pct < -MAX_DAILY_LOSS_PCT:
        reason = f'max_loss_reached ({pnl_pct:.1%})'
        with _engine_lock:
            _circuit_open   = True
            _circuit_reason = reason
        logger.warning(f'Circuit tripped: {reason}')
        return True, reason

    return False, ''


def reset_circuit():
    """Manually reset the circuit breaker (e.g. via Flask endpoint)."""
    global _circuit_open, _circuit_reason
    with _engine_lock:
        _circuit_open   = False
        _circuit_reason = ''
    logger.info('Circuit breaker manually reset')


# ── Alpaca account equity ─────────────────────────────────────────────────────────
def _get_equity() -> float:
    key    = os.environ.get('ALPACA_KEY', '')
    secret = os.environ.get('ALPACA_SECRET', '')
    if not key or not secret:
        return 100_000.0  # paper default
    try:
        r = requests.get(
            'https://paper-api.alpaca.markets/v2/account',
            headers={'APCA-API-KEY-ID': key, 'APCA-API-SECRET-KEY': secret},
            timeout=8,
        )
        if r.status_code == 200:
            return float(r.json().get('equity') or 100_000)
    except Exception as e:
        logger.warning(f'_get_equity: {e}')
    return 100_000.0


# ── Write to algo_trades ──────────────────────────────────────────────────────────
def _write_algo_trade(ticker: str, signal: dict, order_result: dict, config: dict):
    """Write a placed trade to algo_trades table (read by AlgoTradingScreen)."""
    today = datetime.now(_ET).strftime('%Y-%m-%d')
    row = {
        'id':          str(uuid.uuid4()),
        'ticker':      ticker,
        'side':        'buy' if signal.get('signal') == 'BUY' else 'sell',
        'qty':         order_result.get('qty', 1),
        'entry_price': signal.get('entry', 0),
        'stop_loss':   signal.get('stop_loss', 0),
        'target':      signal.get('target_2') or signal.get('target', 0),
        'confidence':  round(float(signal.get('confidence', 0)), 3),
        'score':       signal.get('scanner_score', 0),
        'status':      'open' if order_result.get('status') == 'placed' else order_result.get('status', 'error'),
        'pnl':         None,
        'pnl_pct':     None,
        'exit_reason': None,
        'entry_time':  datetime.now(timezone.utc).isoformat(),
        'exit_time':   None,
        'trade_date':  today,
        'created_at':  datetime.now(timezone.utc).isoformat(),
    }
    _sb_upsert('algo_trades', [row])
    return row['id']


# ── Main evaluation cycle ─────────────────────────────────────────────────────────
def _run_eval_cycle():
    """
    Single evaluation pass:
    1. Load config + check circuit
    2. Get scanner top picks
    3. Score each candidate with AI scorer
    4. Place orders that pass all gates
    5. Update algo_state
    """
    global _circuit_open, _circuit_reason, _current_date

    now_et = datetime.now(_ET)
    today  = now_et.strftime('%Y-%m-%d')

    # Reset circuit on new day
    if today != _current_date:
        _current_date = today
        with _engine_lock:
            _circuit_open   = False
            _circuit_reason = ''
        logger.info(f'Algo engine: new day {today} — circuit reset')

    # Market hours check
    if now_et.weekday() >= 5:
        return
    market_open  = now_et.replace(hour=9,  minute=30, second=0, microsecond=0)
    market_close = now_et.replace(hour=15, minute=45, second=0, microsecond=0)
    if not (market_open <= now_et <= market_close):
        return

    # INTRADAY_TRADING_ENABLED env gate
    if os.environ.get('INTRADAY_TRADING_ENABLED', 'false').lower() != 'true':
        _update_state({
            'running': False, 'regime': 'disabled',
            'last_scan': datetime.now(timezone.utc).isoformat(),
        })
        return

    config = _load_config()
    equity = _get_equity()

    # Circuit breaker check
    tripped, cb_reason = _check_circuit(config, equity)
    if tripped:
        _update_state({
            'running':     False,
            'last_scan':   datetime.now(timezone.utc).isoformat(),
            'regime':      f'circuit_open:{cb_reason}',
        })
        return

    # Get top scanner picks (from in-memory scanner state)
    try:
        from intraday_scanner import _state as _scanner_state
        top_picks = list(_scanner_state.get('top_picks', []))
        scan_count = int(_scanner_state.get('scan_count', 0))
    except Exception as e:
        logger.warning(f'Cannot read scanner state: {e}')
        top_picks  = []
        scan_count = 0

    # Apply aggression multiplier to score threshold
    aggression  = float(config.get('aggression', 0.5))
    base_score  = int(config.get('min_score', DEFAULT_MIN_SCORE))
    # aggression=1.0 → threshold drops 20% (more aggressive)
    # aggression=0.0 → threshold rises 20% (more conservative)
    adj_score   = int(base_score * (1.2 - aggression * 0.4))
    min_conf    = float(config.get('min_confidence', DEFAULT_MIN_CONF * 100)) / 100
    max_trades  = int(config.get('max_trades_per_day', DEFAULT_MAX_TRADES))
    done_today  = _trades_today_count()

    candidates = [p for p in top_picks
                  if p.get('score', 0) >= adj_score
                  and p.get('ticker') not in ('SPY', 'QQQ')]

    logger.info(
        f'Algo engine eval: {len(top_picks)} picks → {len(candidates)} above {adj_score} '
        f'(aggression={aggression:.1f}) | trades_today={done_today}/{max_trades}'
    )

    # Detect regime from SPY/QQQ score delta
    regime = _detect_regime(top_picks)

    new_trades = 0
    placed_tickers = []

    try:
        from intraday_ai_scorer import score_stock
        from intraday_executor  import place_intraday_order
    except Exception as e:
        logger.error(f'Algo engine import error: {e}')
        return

    for pick in candidates:
        if done_today + new_trades >= max_trades:
            logger.info('Algo engine: max_trades_per_day reached mid-cycle — stopping')
            break

        # Re-check circuit mid-cycle (live P&L may have changed)
        tripped, cb_reason = _check_circuit(config, equity)
        if tripped:
            logger.warning(f'Algo engine: circuit tripped mid-cycle: {cb_reason}')
            break

        ticker     = pick.get('ticker', '').upper()
        scan_score = pick.get('score', 0)

        try:
            scored = score_stock(
                ticker=ticker,
                scanner_score=scan_score,
                sp500_trend=regime,
            )
        except Exception as e:
            logger.warning(f'score_stock({ticker}): {e}')
            continue

        sig        = scored.get('signal', 'SKIP')
        confidence = float(scored.get('confidence', 0))

        if sig not in ('BUY', 'SELL'):
            logger.info(f'  {ticker}: SKIP (signal={sig})')
            continue
        if confidence < min_conf:
            logger.info(f'  {ticker}: SKIP (conf={confidence:.0%} < {min_conf:.0%})')
            continue

        # Add scanner_score for write
        scored['scanner_score'] = scan_score

        result = place_intraday_order(scored | {'ticker': ticker})
        status = result.get('status', 'error')

        if status == 'placed':
            trade_id = _write_algo_trade(ticker, scored, result, config)
            placed_tickers.append(ticker)
            new_trades += 1
            logger.info(
                f'  Algo trade placed: {sig} {ticker} '
                f'conf={confidence:.0%} score={scan_score} id={trade_id}'
            )
        else:
            logger.info(f'  {ticker}: order not placed ({status})')

    # Update algo_state
    trades_today_final = done_today + new_trades
    _update_state({
        'running':      True,
        'scan_count':   scan_count,
        'trades_today': trades_today_final,
        'regime':       regime,
        'last_scan':    datetime.now(timezone.utc).isoformat(),
    })

    if placed_tickers:
        logger.info(f'Algo engine: placed {new_trades} trade(s) this cycle: {placed_tickers}')


def _detect_regime(top_picks: list) -> str:
    """Simple regime: bullish / bearish / neutral based on SPY/QQQ momentum."""
    spy = next((p for p in top_picks if p.get('ticker') == 'SPY'), {})
    qqq = next((p for p in top_picks if p.get('ticker') == 'QQQ'), {})
    spy_mom = float(spy.get('momentum_pct', 0))
    qqq_mom = float(qqq.get('momentum_pct', 0))
    avg     = (spy_mom + qqq_mom) / 2
    if avg > 0.3:   return 'bullish'
    if avg < -0.3:  return 'bearish'
    return 'neutral'


# ── Engine loop ───────────────────────────────────────────────────────────────────
_engine_thread: threading.Thread | None = None
_engine_running = False


def _engine_loop():
    global _engine_running
    logger.info('Algo engine loop started')
    while _engine_running:
        try:
            _run_eval_cycle()
        except Exception as e:
            logger.error(f'Algo engine loop error: {e}')
        time.sleep(EVAL_INTERVAL_SECS)
    logger.info('Algo engine loop stopped')


def start():
    """Start the algo engine background thread. Call once at app startup."""
    global _engine_thread, _engine_running
    if _engine_thread and _engine_thread.is_alive():
        logger.info('Algo engine already running')
        return
    _engine_running = True
    _engine_thread  = threading.Thread(
        target=_engine_loop, daemon=True, name='algo-engine')
    _engine_thread.start()
    logger.info('Algo engine started')


def stop():
    """Stop the engine gracefully (completes current cycle first)."""
    global _engine_running
    _engine_running = False
    logger.info('Algo engine stop requested')


# ── Flask control routes ──────────────────────────────────────────────────────────
def register_routes(app):
    """Register /api/algo/* Flask endpoints."""
    from flask import jsonify, request

    @app.route('/api/algo/status')
    def algo_status():
        rows = _sb_get('algo_state', 'id=eq.1&limit=1')
        state = rows[0] if rows else {}
        running = _engine_running and (_engine_thread is not None and _engine_thread.is_alive())

        # Merge today's trade counts from Supabase for Flutter dashboard
        trades_today = 0
        pnl_today = 0.0
        try:
            from intraday_executor import _supa_get
            import pytz as _pytz
            _today = datetime.now(_pytz.timezone('America/New_York')).strftime('%Y-%m-%d')
            today_rows = _supa_get('intraday_trades', f'select=pnl,status&date=eq.{_today}&limit=100') or []
            trades_today = len(today_rows)
            pnl_today = round(sum(float(r.get('pnl') or 0) for r in today_rows if r.get('status') == 'closed'), 2)
        except Exception:
            pass

        try:
            from intraday_scanner import get_status as _scanner_status
            scanner = _scanner_status(lambda k, d=None: None)
        except Exception:
            scanner = {}

        return jsonify({
            # Flutter-compatible fields
            'running':         running,
            'circuit_breaker': _circuit_open,
            'circuit_reason':  _circuit_reason,
            'trades_today':    trades_today,
            'pnl_today':       pnl_today,
            'market_open':     scanner.get('market_open', False),
            'last_scan':       scanner.get('last_scan'),
            'scan_count':      scanner.get('scan_count', 0),
            'signals_today':   scanner.get('signals_today', 0),
            'mode':            'paper',
            # Legacy fields kept for backwards compatibility
            'engine_running':  running,
            'circuit_open':    _circuit_open,
            'algo_state':      state,
        })

    @app.route('/api/algo/config', methods=['GET', 'POST'])
    def algo_config_get():
        if request.method == 'POST':
            try:
                from intraday_scanner import update_config as _update_config
                body = request.get_json(force=True) or {}
                _update_config(body, lambda k, v, **kw: None)
                return jsonify({'status': 'updated'})
            except Exception as e:
                return jsonify({'error': str(e)}), 500
        # GET — return scanner config with fallback to algo_config table
        try:
            from intraday_scanner import get_config as _get_config
            return jsonify(_get_config(lambda k, d=None: None))
        except Exception:
            rows = _sb_get('algo_config', 'id=eq.1&limit=1')
            return jsonify(rows[0] if rows else {
                'min_confidence': 0.72,
                'max_trades_per_day': 3,
                'position_size_pct': 0.05,
                'stop_loss_pct': 0.08,
                'take_profit_pct': 0.15,
                'min_scanner_score': 60,
            })

    @app.route('/api/algo/circuit/reset', methods=['POST'])
    def algo_circuit_reset():
        reset_circuit()
        return jsonify({'ok': True, 'message': 'Circuit breaker reset'})

    @app.route('/api/algo/trades')
    def algo_trades_list():
        limit = request.args.get('limit', 50)
        today = request.args.get('today', 'false').lower() == 'true'
        query = f'order=created_at.desc&limit={limit}'
        if today:
            date = datetime.now(_ET).strftime('%Y-%m-%d')
            query += f'&trade_date=eq.{date}'
        rows = _sb_get('algo_trades', query)
        return jsonify(rows)

    @app.route('/api/algo/eval', methods=['POST'])
    def algo_eval_now():
        """Trigger an immediate eval cycle (for testing)."""
        threading.Thread(target=_run_eval_cycle, daemon=True).start()
        return jsonify({'ok': True, 'message': 'Eval cycle triggered'})
