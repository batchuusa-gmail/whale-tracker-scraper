"""
Options Engine — main scanner entry point.
Runs Income + Growth scans and exposes results via Flask routes.
Can also be run standalone: python -m options_engine.scanner
"""
import logging
import os
import threading
import time
from datetime import datetime, timezone

import requests
import yaml

logger = logging.getLogger(__name__)

_CONFIG_PATH = os.path.join(os.path.dirname(__file__), 'config.yaml')

_SUPA_URL = os.environ.get('SUPABASE_URL', 'https://bedurjtazsfbnkisoeee.supabase.co')
_SUPA_KEY = os.environ.get('SUPABASE_KEY', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJlZHVyanRhenNmYm5raXNvZWVlIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzM2NDcxOTUsImV4cCI6MjA4OTIyMzE5NX0.MK4N9dxAIHlXkGP4rLJPq5tHh9UU8L75EB1b8Q7CVmg')
_SUPA_HDR  = {'apikey': _SUPA_KEY, 'Authorization': f'Bearer {_SUPA_KEY}', 'Content-Type': 'application/json'}
_SUPA_UPSERT_HDR = {**_SUPA_HDR, 'Prefer': 'resolution=merge-duplicates,return=representation'}
_PICKS_URL = f'{_SUPA_URL}/rest/v1/options_picks_cache'

_state = {
    'running':      False,
    'last_scan':    None,
    'scan_count':   0,
    'income_picks': [],
    'growth_picks': [],
    'error':        None,
}
_prev_income_tickers: set[str] = set()
_state_lock = threading.Lock()
_thread: threading.Thread | None = None


def _is_market_open() -> bool:
    """Returns True if US options market is currently open (Mon-Fri 9:30–16:00 ET)."""
    import pytz
    et = pytz.timezone('America/New_York')
    now = datetime.now(et)
    if now.weekday() >= 5:  # Sat/Sun
        return False
    t = now.hour * 60 + now.minute
    return 570 <= t <= 960  # 9:30=570, 16:00=960


def _save_picks_to_supabase(income: list, growth: list):
    try:
        payload = {
            'id': 1,
            'income_picks': income,
            'growth_picks': growth,
            'updated_at': datetime.now(timezone.utc).isoformat(),
        }
        requests.post(_PICKS_URL, headers=_SUPA_UPSERT_HDR, json=payload, timeout=10)
        logger.info(f'options_scanner: saved {len(income)} income + {len(growth)} growth picks to Supabase')
    except Exception as e:
        logger.warning(f'options_scanner: Supabase save failed: {e}')


def _load_picks_from_supabase() -> tuple[list, list]:
    try:
        r = requests.get(f'{_PICKS_URL}?id=eq.1', headers=_SUPA_HDR, timeout=8)
        if r.status_code == 200 and r.json():
            row = r.json()[0]
            return row.get('income_picks') or [], row.get('growth_picks') or []
    except Exception as e:
        logger.warning(f'options_scanner: Supabase load failed: {e}')
    return [], []


def _load_config() -> dict:
    try:
        path = os.environ.get('OPTIONS_CONFIG_PATH', _CONFIG_PATH)
        with open(path) as f:
            return yaml.safe_load(f) or {}
    except Exception as e:
        logger.warning(f'options scanner: config load failed: {e}')
        return {}


def _run_scan():
    global _prev_income_tickers

    if not _is_market_open():
        logger.info('options_scanner: market closed — skipping scan, using cached picks')
        with _state_lock:
            _state['scan_count'] += 1
        return

    from options_engine.engines.income_engine import scan_income
    from options_engine.engines.growth_engine import scan_growth

    config  = _load_config()
    tickers = config.get('tickers', [])

    income_results, growth_results = [], []

    if config.get('income', {}).get('enabled', True):
        income_results = scan_income(tickers, config.get('income', {}))

    if config.get('growth', {}).get('enabled', True):
        growth_results = scan_growth(tickers, config.get('growth', {}))

    income_dicts = [t.to_dict() for t in income_results[:10]]
    growth_dicts = [t.to_dict() for t in growth_results[:10]]

    # Only update in-memory + Supabase if we got results (don't wipe good picks with empty scan)
    if income_dicts or growth_dicts:
        with _state_lock:
            _state['income_picks'] = income_dicts
            _state['growth_picks'] = growth_dicts
        _save_picks_to_supabase(income_dicts, growth_dicts)
        _notify_new_picks(income_dicts, growth_dicts)
        _execute_picks(income_dicts, growth_dicts)

    with _state_lock:
        _state['last_scan']   = datetime.now(timezone.utc).isoformat()
        _state['scan_count'] += 1
        _state['error']       = None

    logger.info(
        f'options_scanner: scan #{_state["scan_count"]} done — '
        f'{len(income_results)} income, {len(growth_results)} growth candidates'
    )


def _notify_new_picks(income_dicts: list, growth_dicts: list):
    global _prev_income_tickers
    try:
        new_income = [p for p in income_dicts
                      if p['ticker'] not in _prev_income_tickers
                      and p.get('pop', 0) >= 0.70]

        if new_income:
            import os
            from supabase import create_client
            from firebase_push import send_to_all_devices

            supabase_url = os.environ.get('SUPABASE_URL', '')
            supabase_key = os.environ.get('SUPABASE_SERVICE_KEY', '')
            if supabase_url and supabase_key:
                sb = create_client(supabase_url, supabase_key)
                for pick in new_income:
                    pop_pct = int(pick['pop'] * 100)
                    title = f"Options Pick: {pick['ticker']}"
                    body  = (f"Short PUT ${pick['strike']:.0f} · exp {pick['expiry']} · "
                             f"${pick['premium']:.2f} cr · POP {pop_pct}%")
                    sent = send_to_all_devices(title, body, {'type': 'options_income', 'ticker': pick['ticker']}, supabase=sb)
                    logger.info(f'options_scanner: FCM push for {pick["ticker"]} → {sent} devices')

        _prev_income_tickers = {p['ticker'] for p in income_dicts}

    except Exception as e:
        logger.warning(f'options_scanner: notify error: {e}')


def _execute_picks(income_dicts: list, growth_dicts: list):
    """Execute picks via Alpaca paper options orders if OPTIONS_TRADING_ENABLED=true."""
    import os
    if os.environ.get('OPTIONS_TRADING_ENABLED', 'false').lower() != 'true':
        return
    try:
        from options_engine.executor import execute_income_pick, execute_growth_pick
        for pick in income_dicts:
            result = execute_income_pick(pick)
            if result['status'] not in ('disabled', 'skipped', 'ok'):
                logger.warning(f'options_scanner: income exec {pick["ticker"]}: {result}')
        for pick in growth_dicts:
            result = execute_growth_pick(pick)
            if result['status'] not in ('disabled', 'skipped', 'ok'):
                logger.warning(f'options_scanner: growth exec {pick["ticker"]}: {result}')
    except Exception as e:
        logger.error(f'options_scanner: _execute_picks error: {e}')


def _scan_loop():
    # Restore last good picks from Supabase immediately on startup
    income_cached, growth_cached = _load_picks_from_supabase()
    with _state_lock:
        _state['running']      = True
        if income_cached or growth_cached:
            _state['income_picks'] = income_cached
            _state['growth_picks'] = growth_cached
            logger.info(f'options_scanner: restored {len(income_cached)} income + {len(growth_cached)} growth picks from Supabase')

    while True:
        try:
            _run_scan()
        except Exception as e:
            logger.error(f'options_scanner: scan error: {e}')
            with _state_lock:
                _state['error'] = str(e)

        config   = _load_config()
        interval = config.get('scan_interval_seconds', 3600)
        time.sleep(interval)


def start():
    """Launch background scan thread. Call once at app startup."""
    global _thread
    if _thread and _thread.is_alive():
        return
    _thread = threading.Thread(target=_scan_loop, daemon=True, name='options-scanner')
    _thread.start()
    logger.info('options_scanner: background thread launched')


def get_status() -> dict:
    with _state_lock:
        return dict(_state)


def register_routes(app):
    """Register /api/options/* routes on Flask app."""
    from flask import jsonify, request

    @app.route('/api/options/status')
    def options_status():
        return jsonify(get_status())

    @app.route('/api/options/income')
    def options_income():
        capital = float(request.args.get('capital', 5000))
        with _state_lock:
            picks = list(_state['income_picks'])
        # Filter picks whose per-contract max_risk fits within the capital budget
        picks = [p for p in picks if (p.get('max_risk', 0) * 100) <= capital]
        return jsonify({'status': 'ok', 'count': len(picks), 'picks': picks, 'capital': capital})

    @app.route('/api/options/growth')
    def options_growth():
        with _state_lock:
            picks = list(_state['growth_picks'])
        return jsonify({'status': 'ok', 'count': len(picks), 'picks': picks})

    @app.route('/api/options/scan', methods=['POST'])
    def options_scan_now():
        """Trigger an immediate scan. Passes market-hours check unless force=true."""
        from flask import request as req
        force = req.args.get('force', '').lower() == 'true'
        def _forced_scan():
            # Temporarily bypass market hours check
            orig = globals().get('_is_market_open')
            try:
                import options_engine.scanner as _self
                _orig = _self._is_market_open
                _self._is_market_open = lambda: True
                _run_scan()
            finally:
                _self._is_market_open = _orig
        target = _forced_scan if force else _run_scan
        t = threading.Thread(target=target, daemon=True)
        t.start()
        return jsonify({'status': 'ok', 'message': 'scan triggered', 'market_open': _is_market_open(), 'forced': force})

    @app.route('/api/options/debug/<ticker>')
    def options_debug_ticker(ticker):
        """Debug income scan for a single ticker — shows per-step filter results."""
        from options_engine.data.stock_fetcher import fetch_stock_snapshot
        from options_engine.data.options_fetcher import fetch_options_chain
        from options_engine.analytics.greeks import enrich_chain_with_greeks
        from options_engine.analytics.pop import enrich_chain_with_pop
        from options_engine.analytics.contract_selector import select_income_contract
        config = _load_config()
        cfg = config.get('income', {})
        min_iv_rank    = int(cfg.get('min_iv_rank', 25))
        min_avg_volume = int(cfg.get('min_avg_volume', 300000))
        min_oi         = int(cfg.get('min_oi', 200))
        min_premium    = float(cfg.get('min_premium', 0.15))
        min_pop        = float(cfg.get('min_pop', 0.55))
        dte_min        = int(cfg.get('dte_min', 21))
        dte_max        = int(cfg.get('dte_max', 60))

        result = {'ticker': ticker.upper(), 'config': cfg}


        snap = fetch_stock_snapshot(ticker.upper())
        result['snapshot'] = snap
        if not snap:
            result['fail'] = 'snapshot_failed'
            return jsonify(result)
        result['price'] = snap.get('price')
        result['iv_rank'] = snap.get('iv_rank_proxy')
        result['avg_vol'] = snap.get('avg_volume_20d')
        result['above_ma50'] = snap.get('above_ma50')
        result['passes_stock_filter'] = (
            snap.get('price', 0) >= 15
            and snap.get('avg_volume_20d', 0) >= min_avg_volume
            and snap.get('iv_rank_proxy', 0) >= min_iv_rank
            and snap.get('above_ma50') is not False
        )
        if not result['passes_stock_filter']:
            result['fail'] = 'stock_filter'
            return jsonify(result)
        chain = fetch_options_chain(ticker.upper(), min_dte=dte_min, max_dte=dte_max)
        if not chain or chain['puts'].empty:
            result['fail'] = 'no_chain'
            return jsonify(result)
        puts = chain['puts'].copy()
        result['puts_total'] = len(puts)
        puts = puts[(puts['bid'] > 0) | (puts['ask'] > 0)]
        result['puts_after_bid_filter'] = len(puts)
        if puts.empty:
            result['fail'] = 'no_active_quotes'
            return jsonify(result)
        enrich_chain_with_greeks(puts, snap['price'])
        enrich_chain_with_pop(puts, mode='income')
        result['sample_deltas'] = puts[['strike', 'dte', 'impliedVolatility', 'delta', 'openInterest', 'bid', 'ask', 'mid', 'pop']].head(10).to_dict(orient='records') if 'delta' in puts.columns else 'no_delta'
        result['iv_stats'] = {'median': float(puts['impliedVolatility'].median()), 'max': float(puts['impliedVolatility'].max()), 'min': float(puts['impliedVolatility'].min())} if 'impliedVolatility' in puts.columns else {}
        contract = select_income_contract(puts)
        if contract is None:
            result['fail'] = 'no_contract_selected'
            return jsonify(result)
        result['selected_contract'] = {k: float(v) if hasattr(v, 'item') else v for k, v in contract.items() if k not in ('contractSymbol',)}
        result['premium'] = float(contract.get('mid') or 0)
        result['pop'] = float(contract.get('pop', 0))
        result['passes_premium'] = result['premium'] >= min_premium
        result['passes_pop'] = result['pop'] >= min_pop
        return jsonify(result)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    logger.info('options_scanner: running one-shot scan…')
    _run_scan()
    status = get_status()
    print(f'\nIncome picks: {len(status["income_picks"])}')
    for p in status['income_picks']:
        print(f'  {p["ticker"]} PUT {p["strike"]} exp={p["expiry"]} prem={p["premium"]} POP={p["pop"]:.0%}')
    print(f'\nGrowth picks: {len(status["growth_picks"])}')
    for p in status['growth_picks']:
        print(f'  {p["ticker"]} {p["direction"].upper()} {p["option_type"].upper()} {p["strike"]} exp={p["expiry"]}')
