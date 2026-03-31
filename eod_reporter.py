"""
WHAL-98 — End of Day Report
Generates a daily P&L summary from intraday_trades at 4:05 PM ET.

Kill switch: INTRADAY_TRADING_ENABLED (reuses the trading env var)

Report includes:
  - total_trades, winning_trades, losing_trades, win_rate
  - total_pnl, best_trade, worst_trade, avg_hold_mins
  - exit_reason breakdown
  - generated_at timestamp
"""

import os
import json
import logging
import threading
import time
from datetime import datetime, date

import pytz
import requests

logger = logging.getLogger(__name__)

_ET = pytz.timezone('America/New_York')

SUPABASE_URL = os.getenv('SUPABASE_URL', 'https://bedurjtazsfbnkisoeee.supabase.co')
SUPABASE_KEY = os.getenv('SUPABASE_KEY', '')
_SUPA_HEADERS = {
    'apikey': SUPABASE_KEY,
    'Authorization': f'Bearer {SUPABASE_KEY}',
    'Content-Type': 'application/json',
}

REPORT_TTL = 86400  # 24 hours


def _fetch_trades_for_date(trade_date: str) -> list:
    """Pull closed intraday_trades for the given date (YYYY-MM-DD) from Supabase."""
    try:
        url = (
            f'{SUPABASE_URL}/rest/v1/intraday_trades'
            f'?date=eq.{trade_date}'
            f'&status=eq.CLOSED'
            f'&select=ticker,pnl,pnl_pct,entry_price,exit_price,hold_minutes,exit_reason,entry_time,exit_time'
            f'&order=exit_time.desc'
            f'&limit=200'
        )
        r = requests.get(url, headers=_SUPA_HEADERS, timeout=15)
        if r.status_code == 200:
            return r.json() or []
    except Exception as e:
        logger.error(f'EOD: Supabase fetch failed: {e}')
    return []


def _build_report(trades: list, trade_date: str) -> dict:
    """Compute EOD summary from a list of closed trade rows."""
    if not trades:
        return {
            'date': trade_date,
            'total_trades': 0,
            'winning_trades': 0,
            'losing_trades': 0,
            'win_rate': 0.0,
            'total_pnl': 0.0,
            'avg_pnl_pct': 0.0,
            'avg_hold_mins': 0.0,
            'best_trade': None,
            'worst_trade': None,
            'exit_reasons': {},
            'trades': [],
            'generated_at': datetime.now(_ET).isoformat(),
        }

    winning = [t for t in trades if (t.get('pnl') or 0) > 0]
    losing  = [t for t in trades if (t.get('pnl') or 0) <= 0]
    pnls    = [(t.get('pnl') or 0) for t in trades]
    pct_s   = [(t.get('pnl_pct') or 0) for t in trades]
    holds   = [(t.get('hold_minutes') or 0) for t in trades]

    total_pnl   = round(sum(pnls), 2)
    avg_pnl_pct = round(sum(pct_s) / len(pct_s), 4) if pct_s else 0.0
    avg_hold    = round(sum(holds) / len(holds), 1) if holds else 0.0

    # Best / worst by pnl
    best  = max(trades, key=lambda t: (t.get('pnl') or 0))
    worst = min(trades, key=lambda t: (t.get('pnl') or 0))

    def _trade_summary(t: dict) -> dict:
        return {
            'ticker':      t.get('ticker', ''),
            'pnl':         round(t.get('pnl') or 0, 2),
            'pnl_pct':     round(t.get('pnl_pct') or 0, 4),
            'entry_price': t.get('entry_price'),
            'exit_price':  t.get('exit_price'),
            'hold_minutes': t.get('hold_minutes'),
            'exit_reason': t.get('exit_reason', ''),
        }

    # Exit reason breakdown
    reasons: dict[str, int] = {}
    for t in trades:
        r = t.get('exit_reason') or 'UNKNOWN'
        reasons[r] = reasons.get(r, 0) + 1

    return {
        'date':           trade_date,
        'total_trades':   len(trades),
        'winning_trades': len(winning),
        'losing_trades':  len(losing),
        'win_rate':       round(len(winning) / len(trades), 4) if trades else 0.0,
        'total_pnl':      total_pnl,
        'avg_pnl_pct':    avg_pnl_pct,
        'avg_hold_mins':  avg_hold,
        'best_trade':     _trade_summary(best),
        'worst_trade':    _trade_summary(worst),
        'exit_reasons':   reasons,
        'trades':         [_trade_summary(t) for t in trades],
        'generated_at':   datetime.now(_ET).isoformat(),
    }


def generate_report(trade_date: str, redis_set_fn, redis_get_fn) -> dict:
    """Generate (or regenerate) the EOD report for a given date and cache it."""
    logger.info(f'EOD: generating report for {trade_date}')
    trades = _fetch_trades_for_date(trade_date)
    report = _build_report(trades, trade_date)
    try:
        cache_key = f'eod:report:{trade_date}'
        redis_set_fn(cache_key, json.dumps(report), ex=REPORT_TTL)
    except Exception as e:
        logger.warning(f'EOD: cache write failed: {e}')
    logger.info(
        f'EOD report {trade_date}: {report["total_trades"]} trades, '
        f'P&L ${report["total_pnl"]:.2f}, win rate {report["win_rate"]*100:.0f}%'
    )
    return report


def get_report(trade_date: str, redis_get_fn) -> dict | None:
    """Return cached EOD report or None if not yet generated."""
    try:
        cache_key = f'eod:report:{trade_date}'
        raw = redis_get_fn(cache_key)
        if raw:
            return json.loads(raw)
    except Exception as e:
        logger.warning(f'EOD: cache read failed: {e}')
    return None


# ── Background thread ─────────────────────────────────────────────

_triggered_today: str | None = None   # tracks which date we already ran


def _monitor_loop(redis_set_fn, redis_get_fn):
    global _triggered_today
    logger.info('EOD reporter: background thread started')
    while True:
        try:
            now_et = datetime.now(_ET)
            # Trigger once per trading day at 16:05 ET (Mon–Fri)
            if (
                now_et.weekday() < 5             # Mon–Fri
                and now_et.hour == 16
                and now_et.minute >= 5
                and now_et.minute < 10            # 5-min window
            ):
                today_str = now_et.strftime('%Y-%m-%d')
                if _triggered_today != today_str:
                    _triggered_today = today_str
                    generate_report(today_str, redis_set_fn, redis_get_fn)
        except Exception as e:
            logger.error(f'EOD monitor error: {e}')
        time.sleep(60)


def start(redis_set_fn, redis_get_fn):
    t = threading.Thread(
        target=_monitor_loop,
        args=(redis_set_fn, redis_get_fn),
        daemon=True,
        name='eod-reporter',
    )
    t.start()
    logger.info('EOD reporter started')
