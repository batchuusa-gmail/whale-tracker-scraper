#!/usr/bin/env python3
"""
WHAL-115: Daily Data Persistence Cron
Runs once per day (5 PM ET) via Railway Cron Service.
Scrapes: congress trades, options flow, market snapshots, short interest.
Saves everything to Supabase for fast Flask reads.
"""
import os, time, logging, requests
from datetime import date, datetime, timedelta, timezone
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger('cron')

SUPABASE_URL = os.getenv('SUPABASE_URL', 'https://bedurjtazsfbnkisoeee.supabase.co')
SUPABASE_KEY = os.getenv('SUPABASE_KEY', '')
ALPACA_KEY   = os.getenv('ALPACA_KEY', '')
ALPACA_SEC   = os.getenv('ALPACA_SECRET', '')

HEADERS = {
    'apikey': SUPABASE_KEY,
    'Authorization': f'Bearer {SUPABASE_KEY}',
    'Content-Type': 'application/json',
    'Prefer': 'resolution=merge-duplicates,return=minimal',
}

TODAY = date.today().isoformat()

# ─── Tickers to snapshot daily ────────────────────────────────────
SNAPSHOT_TICKERS = [
    'AAPL','MSFT','NVDA','GOOGL','AMZN','META','TSLA','JPM','V','WMT',
    'JNJ','XOM','BAC','MA','AVGO','LLY','MRK','CVX','PEP','COST',
    'AMD','NFLX','INTC','DIS','ADBE','QCOM','MU','TXN','GS','PLTR',
    'UBER','COIN','SNOW','HOOD','SOFI','RBLX','PYPL','CRM','NOW','CRWD',
    'ZS','DDOG','NET','PANW','RIVN','LCID','NIO','SHOP','SQ','ABNB',
]

SHORT_INTEREST_TICKERS = [
    'AAPL','MSFT','NVDA','TSLA','AMZN','META','GOOGL','AMD','PLTR','COIN',
    'HOOD','SOFI','RIVN','LCID','NIO','SNOW','CRWD','UBER','DASH','RBLX',
    'GME','AMC','BBBY','SPCE','NKLA','CLOV','WISH','CLNE','SNDL','MVIS',
]


# ─── Supabase helpers ─────────────────────────────────────────────

POLYGON_KEY = os.getenv('POLYGON_API_KEY', '')
QUIVER_KEY  = os.getenv('QUIVER_API_KEY', '')

ON_CONFLICT = {
    'congress_trades':      'member,ticker,date,type',
    'options_flow_daily':   'ticker,option_type,strike,expiry,trade_date',
    'market_snapshots':     'ticker,snapshot_date',
    'short_interest_daily': 'ticker,snapshot_date',
    # WHAL-119 new tables
    'insider_trades':       'ticker,owner_name,filing_date',
    'dark_pool_prints':     'ticker,execution_time',
    'options_flow':         'ticker,strike,expiry,call_put,scraped_at',
    'congressional_trades': 'politician,ticker,trade_date',
    'ticker_fundamentals':  'ticker',
    'reddit_sentiment':     'ticker',
}

def sb_upsert(table: str, rows: list) -> bool:
    if not rows:
        return True
    try:
        conflict_cols = ON_CONFLICT.get(table, '')
        url = f'{SUPABASE_URL}/rest/v1/{table}'
        if conflict_cols:
            url += f'?on_conflict={conflict_cols}'
        r = requests.post(url, headers=HEADERS, json=rows, timeout=30)
        if r.status_code in (200, 201, 204):
            log.info(f'  ✓ {table}: saved {len(rows)} rows')
            return True
        log.error(f'  ✗ {table}: {r.status_code} {r.text[:200]}')
        return False
    except Exception as e:
        log.error(f'  ✗ {table} exception: {e}')
        return False


# ─── 1. Congress Trades ───────────────────────────────────────────

def scrape_congress_trades() -> int:
    log.info('── Scraping congress trades …')
    try:
        r = requests.get(
            'https://api.quiverquant.com/beta/live/congresstrading',
            headers={'Accept': 'application/json'},
            timeout=20,
        )
        if r.status_code != 200:
            log.warning(f'QuiverQuant returned {r.status_code}')
            return 0
        rows = []
        for t in r.json():
            ticker = (t.get('Ticker') or '').strip().upper()
            if not ticker or ticker in ('N/A', '--', ''):
                continue
            if (t.get('TickerType') or '') not in ('ST', ''):
                continue
            tx_raw = (t.get('Transaction') or '').lower()
            if 'purchase' in tx_raw or 'buy' in tx_raw:
                kind = 'Buy'
            elif 'sale' in tx_raw or 'sell' in tx_raw:
                kind = 'Sell'
            else:
                continue
            trade_date = t.get('TransactionDate') or t.get('ReportDate') or TODAY
            rows.append({
                'member':  (t.get('Representative') or '')[:120],
                'party':   (t.get('Party') or '')[:1].upper(),
                'chamber': 'Senate' if (t.get('House') or '') == 'Senate' else 'House',
                'ticker':  ticker,
                'type':    kind,
                'amount':  (t.get('Range') or '')[:50],
                'date':    trade_date[:10],
                'company': (t.get('Description') or '')[:120],
            })
        # Deduplicate within the batch (QuiverQuant sometimes has duplicate entries)
        seen = set()
        deduped = []
        for row in rows:
            key = (row['member'], row['ticker'], row['date'], row['type'])
            if key not in seen:
                seen.add(key)
                deduped.append(row)
        sb_upsert('congress_trades', deduped)
        return len(deduped)
    except Exception as e:
        log.error(f'congress_trades error: {e}')
        return 0


# ─── 2. Market Snapshots ──────────────────────────────────────────

def scrape_market_snapshots() -> int:
    log.info('── Scraping market snapshots …')
    rows = []

    # Try Alpaca snapshots (fast, one call for all tickers)
    if ALPACA_KEY and ALPACA_SEC:
        try:
            hdrs = {'APCA-API-KEY-ID': ALPACA_KEY, 'APCA-API-SECRET-KEY': ALPACA_SEC}
            r = requests.get(
                'https://data.alpaca.markets/v2/stocks/snapshots',
                headers=hdrs,
                params={'symbols': ','.join(SNAPSHOT_TICKERS), 'feed': 'sip'},
                timeout=20,
            )
            if r.status_code == 200:
                MKTCAP = {
                    'AAPL':2.95e12,'MSFT':3.08e12,'NVDA':2.16e12,'GOOGL':2.14e12,
                    'AMZN':1.94e12,'META':1.32e12,'TSLA':7.90e11,'JPM':5.64e11,
                    'V':5.55e11,'WMT':5.48e11,'JNJ':3.80e11,'XOM':4.60e11,
                    'BAC':3.10e11,'MA':4.50e11,'AVGO':7.28e11,'LLY':7.52e11,
                    'MRK':2.60e11,'CVX':2.80e11,'PEP':2.10e11,'COST':3.40e11,
                    'AMD':2.89e11,'NFLX':2.71e11,'INTC':1.20e11,'DIS':2.00e11,
                    'ADBE':2.40e11,'QCOM':1.80e11,'MU':1.40e11,'TXN':1.70e11,
                    'GS':1.60e11,'PLTR':5.35e10,
                }
                for sym, snap in r.json().items():
                    last = float(snap.get('latestTrade', {}).get('p', 0) or 0)
                    prev = float(snap.get('prevDailyBar', {}).get('c', 0) or 0)
                    vol  = int(snap.get('dailyBar', {}).get('v', 0) or 0)
                    chg  = round((last - prev) / prev * 100, 2) if prev and last else 0
                    if last > 0:
                        rows.append({
                            'ticker':        sym,
                            'price':         round(last, 2),
                            'change_pct':    chg,
                            'volume':        vol,
                            'market_cap':    MKTCAP.get(sym),
                            'snapshot_date': TODAY,
                        })
                if rows:
                    log.info(f'  Alpaca: {len(rows)} symbols')
        except Exception as e:
            log.warning(f'Alpaca snapshot error: {e}')

    # Fallback: yfinance batch download
    if not rows:
        try:
            import yfinance as yf
            hist = yf.download(
                ' '.join(SNAPSHOT_TICKERS),
                period='2d', group_by='ticker',
                auto_adjust=True, progress=False, threads=True, timeout=30,
            )
            for sym in SNAPSHOT_TICKERS:
                try:
                    closes = hist[sym]['Close'].dropna() if len(SNAPSHOT_TICKERS) > 1 else hist['Close'].dropna()
                    vols   = hist[sym]['Volume'].dropna() if len(SNAPSHOT_TICKERS) > 1 else hist['Volume'].dropna()
                    if len(closes) >= 2:
                        prev  = float(closes.iloc[-2])
                        price = float(closes.iloc[-1])
                        vol   = int(vols.iloc[-1]) if len(vols) >= 1 else 0
                        chg   = round((price - prev) / prev * 100, 2) if prev else 0
                        rows.append({
                            'ticker':        sym,
                            'price':         round(price, 2),
                            'change_pct':    chg,
                            'volume':        vol,
                            'market_cap':    None,
                            'snapshot_date': TODAY,
                        })
                except Exception:
                    pass
            log.info(f'  yfinance: {len(rows)} symbols')
        except Exception as e:
            log.error(f'yfinance snapshot error: {e}')

    sb_upsert('market_snapshots', rows)
    return len(rows)


# ─── 3. Short Interest ────────────────────────────────────────────

def scrape_short_interest() -> int:
    log.info('── Scraping short interest …')
    rows = []
    try:
        import yfinance as yf
        for ticker in SHORT_INTEREST_TICKERS:
            try:
                info = yf.Ticker(ticker).info
                short_pct = float(info.get('shortPercentOfFloat') or 0) * 100
                rows.append({
                    'ticker':        ticker,
                    'short_ratio':   round(float(info.get('shortRatio') or 0), 2),
                    'short_percent': round(short_pct, 2),
                    'shares_short':  int(info.get('sharesShort') or 0),
                    'float_shares':  int(info.get('floatShares') or 0),
                    'snapshot_date': TODAY,
                })
                time.sleep(0.3)
            except Exception as ex:
                log.debug(f'  short interest {ticker}: {ex}')
        log.info(f'  collected {len(rows)} tickers')
    except Exception as e:
        log.error(f'short_interest error: {e}')

    sb_upsert('short_interest_daily', rows)
    return len(rows)


# ─── 4. Options Flow ─────────────────────────────────────────────

def scrape_options_flow() -> int:
    log.info('── Scraping options flow …')
    rows = []
    try:
        import yfinance as yf
        # Focus on highest-volume option tickers
        OPTION_TICKERS = [
            'SPY','QQQ','NVDA','AAPL','TSLA','META','AMZN','MSFT',
            'AMD','PLTR','COIN','GOOGL','JPM','GS','BAC',
        ]
        for ticker in OPTION_TICKERS:
            try:
                t = yf.Ticker(ticker)
                exps = t.options
                if not exps:
                    continue
                for exp in exps[:2]:
                    try:
                        chain = t.option_chain(exp)
                        avg_call_vol = chain.calls['volume'].mean() if len(chain.calls) > 0 else 1
                        avg_put_vol  = chain.puts['volume'].mean() if len(chain.puts) > 0 else 1

                        for _, row in chain.calls.iterrows():
                            vol = int(row.get('volume') or 0)
                            if vol < 100:
                                continue
                            rows.append({
                                'ticker':        ticker,
                                'option_type':   'CALL',
                                'strike':        float(row.get('strike', 0)),
                                'expiry':        exp,
                                'volume':        vol,
                                'open_interest': int(row.get('openInterest') or 0),
                                'premium':       round(float(row.get('lastPrice') or 0) * vol * 100, 2),
                                'unusual':       bool(avg_call_vol > 0 and vol > avg_call_vol * 3),
                                'trade_date':    TODAY,
                            })
                        for _, row in chain.puts.iterrows():
                            vol = int(row.get('volume') or 0)
                            if vol < 100:
                                continue
                            rows.append({
                                'ticker':        ticker,
                                'option_type':   'PUT',
                                'strike':        float(row.get('strike', 0)),
                                'expiry':        exp,
                                'volume':        vol,
                                'open_interest': int(row.get('openInterest') or 0),
                                'premium':       round(float(row.get('lastPrice') or 0) * vol * 100, 2),
                                'unusual':       bool(avg_put_vol > 0 and vol > avg_put_vol * 3),
                                'trade_date':    TODAY,
                            })
                    except Exception as e:
                        log.debug(f'  options chain {ticker} {exp}: {e}')
                time.sleep(0.5)
            except Exception as e:
                log.debug(f'  options {ticker}: {e}')
        # Sort by premium descending, keep top 200
        rows.sort(key=lambda x: x.get('premium', 0), reverse=True)
        rows = rows[:200]
        log.info(f'  collected {len(rows)} option contracts')
    except Exception as e:
        log.error(f'options_flow error: {e}')

    sb_upsert('options_flow_daily', rows)
    return len(rows)


# ─── WHAL-118: New scrapers ───────────────────────────────────────

WATCHLIST = [
    'AAPL','MSFT','NVDA','TSLA','AMZN','META','GOOGL','JPM','BAC',
    'SPY','QQQ','AMD','PLTR','COIN','GS','V','NFLX','AVGO','LLY','MRK',
]


def scrape_insider_trades() -> int:
    """SEC EDGAR Form 4 filings → insider_trades table."""
    log.info('── Scraping insider trades (SEC EDGAR) …')
    rows = []
    try:
        import xml.etree.ElementTree as ET
        cutoff = (date.today() - timedelta(days=7)).isoformat()
        url = (
            'https://efts.sec.gov/LATEST/search-index?q=%22form+4%22'
            f'&dateRange=custom&startdt={cutoff}&enddt={date.today().isoformat()}'
            '&from=0&size=40&forms=4'
        )
        r = requests.get(url,
            headers={'User-Agent': 'WhaleTracker research@whaletracker.app'},
            timeout=20)
        if r.status_code != 200:
            log.warning(f'EDGAR returned {r.status_code}')
            return 0

        hits = r.json().get('hits', {}).get('hits', [])
        for hit in hits:
            try:
                src = hit.get('_source', {})
                accession = (src.get('accession_no') or '').replace('-', '')
                cik = str(src.get('entity_id', '')).zfill(10)
                filed_at = src.get('file_date', '')
                if not accession or not cik or not filed_at:
                    continue

                # Fetch Form 4 XML for transaction details
                xml_url = f'https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession}/{accession}-index.htm'
                xml_r = requests.get(xml_url,
                    headers={'User-Agent': 'WhaleTracker research@whaletracker.app'},
                    timeout=10)
                if xml_r.status_code != 200:
                    continue

                # Find the .xml form4 file link
                import re
                xml_file = re.search(r'href="([^"]+\.xml)"', xml_r.text, re.IGNORECASE)
                if not xml_file:
                    continue
                form_url = f'https://www.sec.gov{xml_file.group(1)}'
                form_r = requests.get(form_url,
                    headers={'User-Agent': 'WhaleTracker research@whaletracker.app'},
                    timeout=10)
                if form_r.status_code != 200:
                    continue

                root = ET.fromstring(form_r.text)
                ns = ''
                issuer = root.find(f'{ns}issuer')
                owner  = root.find(f'{ns}reportingOwner')
                ticker = ''
                if issuer is not None:
                    t = issuer.find(f'{ns}issuerTradingSymbol')
                    if t is not None:
                        ticker = (t.text or '').strip().upper()
                if not ticker:
                    continue

                owner_name = ''
                owner_type = ''
                if owner is not None:
                    rpt = owner.find(f'{ns}reportingOwnerIdentification')
                    if rpt is not None:
                        n = rpt.find(f'{ns}rptOwnerName')
                        if n is not None:
                            owner_name = (n.text or '').strip()
                    rel = owner.find(f'{ns}reportingOwnerRelationship')
                    if rel is not None:
                        for tag in ['isOfficer', 'isDirector', 'isTenPercentOwner']:
                            e = rel.find(f'{ns}{tag}')
                            if e is not None and (e.text or '').strip() == '1':
                                owner_type = tag.replace('is', '')
                                break

                for tx in root.findall(f'.//{ns}nonDerivativeTransaction'):
                    try:
                        def _txt(tag):
                            e = tx.find(f'.//{ns}{tag}')
                            return (e.text or '').strip() if e is not None else ''
                        tx_code = _txt('transactionCode')
                        tx_type = 'Buy' if tx_code in ('P',) else 'Sell' if tx_code in ('S', 'D') else tx_code
                        shares = float(_txt('transactionShares') or 0)
                        price  = float(_txt('transactionPricePerShare') or 0)
                        value  = round(shares * price, 2)
                        if value < 1000:
                            continue
                        rows.append({
                            'ticker':           ticker,
                            'owner_name':       owner_name[:120],
                            'owner_type':       owner_type[:40],
                            'transaction_type': tx_type,
                            'shares':           shares,
                            'price':            price,
                            'value':            value,
                            'filing_date':      filed_at[:10],
                        })
                    except Exception:
                        pass
            except Exception as ex:
                log.debug(f'EDGAR hit error: {ex}')

        log.info(f'  collected {len(rows)} insider trades')
    except Exception as e:
        log.error(f'insider_trades error: {e}')

    sb_upsert('insider_trades', rows)
    return len(rows)


def scrape_dark_pool_prints() -> int:
    """Polygon.io last trades → dark_pool_prints table (large block trades as proxy)."""
    log.info('── Scraping dark pool prints (Polygon.io) …')
    rows = []
    if not POLYGON_KEY:
        log.warning('  POLYGON_API_KEY not set — skipping dark_pool_prints')
        return 0
    try:
        from datetime import datetime as dt
        today = date.today().isoformat()
        for ticker in WATCHLIST:
            try:
                # Polygon v2 ticks endpoint: last 50 trades for today
                r = requests.get(
                    f'https://api.polygon.io/v3/trades/{ticker}',
                    params={'order': 'desc', 'limit': 50, 'apiKey': POLYGON_KEY},
                    timeout=10,
                )
                if r.status_code != 200:
                    continue
                results = r.json().get('results', [])
                for t in results:
                    size  = float(t.get('size', 0))
                    price = float(t.get('price', 0))
                    val   = round(size * price, 2)
                    # Only capture large block trades ($500k+)
                    if val < 500_000:
                        continue
                    ts = t.get('sip_timestamp') or t.get('participant_timestamp')
                    if ts:
                        exec_time = dt.fromtimestamp(ts / 1e9, tz=timezone.utc).isoformat()
                    else:
                        exec_time = f'{today}T16:00:00+00:00'
                    rows.append({
                        'ticker':         ticker,
                        'price':          price,
                        'size':           size,
                        'value':          val,
                        'execution_time': exec_time,
                    })
                time.sleep(0.2)
            except Exception as ex:
                log.debug(f'dark pool {ticker}: {ex}')
        log.info(f'  collected {len(rows)} dark pool prints')
    except Exception as e:
        log.error(f'dark_pool_prints error: {e}')

    sb_upsert('dark_pool_prints', rows)
    return len(rows)


def scrape_options_flow_new() -> int:
    """Polygon.io options trades → options_flow table."""
    log.info('── Scraping options flow (Polygon.io) …')
    rows = []
    if not POLYGON_KEY:
        log.warning('  POLYGON_API_KEY not set — using yfinance fallback')
        # yfinance fallback
        try:
            import yfinance as yf
            now_ts = datetime.now(timezone.utc).isoformat()
            for ticker in WATCHLIST[:10]:
                try:
                    t = yf.Ticker(ticker)
                    exps = t.options
                    if not exps:
                        continue
                    for exp in exps[:1]:
                        chain = t.option_chain(exp)
                        avg_cv = chain.calls['volume'].mean() if len(chain.calls) else 1
                        avg_pv = chain.puts['volume'].mean()  if len(chain.puts)  else 1
                        for _, row in chain.calls.iterrows():
                            vol  = int(row.get('volume') or 0)
                            prem = round(float(row.get('lastPrice') or 0) * vol * 100, 2)
                            if prem < 100_000:
                                continue
                            rows.append({
                                'ticker': ticker, 'call_put': 'CALL',
                                'strike': float(row.get('strike', 0)), 'expiry': exp,
                                'premium': prem, 'volume': vol,
                                'open_interest': int(row.get('openInterest') or 0),
                                'sentiment': 'bullish' if avg_cv > 0 and vol > avg_cv * 2 else 'neutral',
                                'scraped_at': now_ts,
                            })
                        for _, row in chain.puts.iterrows():
                            vol  = int(row.get('volume') or 0)
                            prem = round(float(row.get('lastPrice') or 0) * vol * 100, 2)
                            if prem < 100_000:
                                continue
                            rows.append({
                                'ticker': ticker, 'call_put': 'PUT',
                                'strike': float(row.get('strike', 0)), 'expiry': exp,
                                'premium': prem, 'volume': vol,
                                'open_interest': int(row.get('openInterest') or 0),
                                'sentiment': 'bearish' if avg_pv > 0 and vol > avg_pv * 2 else 'neutral',
                                'scraped_at': now_ts,
                            })
                    time.sleep(0.5)
                except Exception as ex:
                    log.debug(f'options yf {ticker}: {ex}')
        except Exception as e:
            log.error(f'options_flow yfinance error: {e}')
        rows.sort(key=lambda x: x.get('premium', 0), reverse=True)
        rows = rows[:100]
        sb_upsert('options_flow', rows)
        return len(rows)

    # Polygon options snapshots
    try:
        now_ts = datetime.now(timezone.utc).isoformat()
        for ticker in WATCHLIST[:10]:
            try:
                r = requests.get(
                    f'https://api.polygon.io/v3/snapshot/options/{ticker}',
                    params={'apiKey': POLYGON_KEY, 'limit': 20,
                            'sort': 'open_interest', 'order': 'desc'},
                    timeout=10,
                )
                if r.status_code != 200:
                    continue
                for result in r.json().get('results', []):
                    det = result.get('details', {})
                    greeks = result.get('greeks', {})
                    day   = result.get('day', {})
                    vol   = int(day.get('volume', 0))
                    prem  = round(float(result.get('last_quote', {}).get('ask', 0)) * vol * 100, 2)
                    if prem < 100_000:
                        continue
                    expiry = det.get('expiration_date', '')
                    rows.append({
                        'ticker':         ticker,
                        'strike':         float(det.get('strike_price', 0)),
                        'expiry':         expiry[:10] if expiry else None,
                        'call_put':       det.get('contract_type', '').upper(),
                        'premium':        prem,
                        'volume':         vol,
                        'open_interest':  int(result.get('open_interest', 0)),
                        'sentiment':      'bullish' if det.get('contract_type', '').upper() == 'CALL' else 'bearish',
                        'scraped_at':     now_ts,
                    })
                time.sleep(0.2)
            except Exception as ex:
                log.debug(f'options polygon {ticker}: {ex}')
        log.info(f'  collected {len(rows)} options flow rows')
    except Exception as e:
        log.error(f'options_flow_new error: {e}')

    rows.sort(key=lambda x: x.get('premium', 0), reverse=True)
    rows = rows[:100]
    sb_upsert('options_flow', rows)
    return len(rows)


def scrape_congressional_trades() -> int:
    """Quiver Quant congressional trades → congressional_trades table."""
    log.info('── Scraping congressional trades (Quiver Quant) …')
    rows = []
    try:
        headers = {'Accept': 'application/json'}
        if QUIVER_KEY:
            headers['Authorization'] = f'Token {QUIVER_KEY}'
        r = requests.get(
            'https://api.quiverquant.com/beta/live/congresstrading',
            headers=headers,
            timeout=20,
        )
        if r.status_code != 200:
            log.warning(f'QuiverQuant returned {r.status_code}')
            return 0
        seen = set()
        for t in r.json():
            ticker = (t.get('Ticker') or '').strip().upper()
            if not ticker or ticker in ('N/A', '--', ''):
                continue
            if (t.get('TickerType') or '') not in ('ST', ''):
                continue
            tx_raw = (t.get('Transaction') or '').lower()
            if 'purchase' in tx_raw or 'buy' in tx_raw:
                tx_type = 'Buy'
            elif 'sale' in tx_raw or 'sell' in tx_raw:
                tx_type = 'Sell'
            else:
                continue
            politician   = (t.get('Representative') or '')[:120]
            trade_date   = (t.get('TransactionDate') or t.get('ReportDate') or TODAY)[:10]
            disc_date    = (t.get('ReportDate') or trade_date)[:10]
            key = (politician, ticker, trade_date)
            if key in seen:
                continue
            seen.add(key)
            rows.append({
                'politician':       politician,
                'ticker':           ticker,
                'transaction_type': tx_type,
                'amount_range':     (t.get('Range') or '')[:50],
                'trade_date':       trade_date,
                'disclosure_date':  disc_date,
            })
        log.info(f'  collected {len(rows)} congressional trades')
    except Exception as e:
        log.error(f'congressional_trades error: {e}')

    sb_upsert('congressional_trades', rows)
    return len(rows)


def scrape_ticker_fundamentals() -> int:
    """yfinance fundamentals → ticker_fundamentals table."""
    log.info('── Scraping ticker fundamentals (yfinance) …')
    rows = []
    try:
        import yfinance as yf
        for ticker in WATCHLIST:
            try:
                info = yf.Ticker(ticker).info
                pe    = info.get('trailingPE') or info.get('forwardPE')
                mcap  = info.get('marketCap')
                sector = info.get('sector', '')
                # earnings date
                earn_date = None
                try:
                    ed = yf.Ticker(ticker).earnings_dates
                    if ed is not None and not ed.empty:
                        import pytz
                        now_tz = datetime.now(timezone.utc)
                        future = ed[ed.index >= now_tz]
                        if not future.empty:
                            earn_date = future.index[-1].strftime('%Y-%m-%d')
                except Exception:
                    pass
                rows.append({
                    'ticker':        ticker,
                    'pe_ratio':      round(float(pe), 2) if pe else None,
                    'market_cap':    int(mcap) if mcap else None,
                    'earnings_date': earn_date,
                    'sector':        sector[:80] if sector else None,
                    'updated_at':    datetime.now(timezone.utc).isoformat(),
                })
                time.sleep(0.4)
            except Exception as ex:
                log.debug(f'fundamentals {ticker}: {ex}')
        log.info(f'  collected {len(rows)} ticker fundamentals')
    except Exception as e:
        log.error(f'ticker_fundamentals error: {e}')

    sb_upsert('ticker_fundamentals', rows)
    return len(rows)


REDDIT_TICKERS = [
    'AAPL','MSFT','NVDA','TSLA','AMZN','META','AMD','PLTR','COIN','GME',
    'AMC','RIVN','HOOD','SOFI','SPY','QQQ','NFLX','GOOGL','SHOP','SNOW',
]

REDDIT_SUBS = ['wallstreetbets', 'stocks', 'investing', 'stockmarket', 'options']

def scrape_reddit_sentiment() -> int:
    """Reddit WallStreetBets/stocks sentiment via public JSON API — no auth needed."""
    log.info('── Scraping Reddit sentiment …')
    rows = []
    now_ts = datetime.now(timezone.utc).isoformat()
    headers = {'User-Agent': 'WhaleTracker/1.0 (whale tracker app)'}

    for ticker in REDDIT_TICKERS:
        counts = {'bullish': 0, 'bearish': 0, 'neutral': 0}
        total_mentions = 0
        top_posts = []
        bull_kw = {'buy','bull','moon','calls','long','squeeze','pump','rocket','ath','dip','hold'}
        bear_kw = {'sell','bear','puts','short','crash','dump','drop','fall','puts','red','rip'}

        for sub in REDDIT_SUBS[:3]:  # limit to 3 subs per ticker to avoid rate limits
            try:
                url = f'https://www.reddit.com/r/{sub}/search.json'
                r = requests.get(url, params={'q': ticker, 'sort': 'new', 'limit': 15, 'restrict_sr': '1'},
                                 headers=headers, timeout=8)
                if r.status_code == 429:
                    log.warning(f'Reddit rate limit on r/{sub}')
                    time.sleep(5)
                    continue
                if r.status_code != 200:
                    continue
                data = r.json().get('data', {}).get('children', [])
                for child in data:
                    post = child.get('data', {})
                    title = (post.get('title') or '').lower()
                    text  = (post.get('selftext') or '').lower()
                    body  = title + ' ' + text
                    if ticker.lower() not in body and ticker.lower() not in title:
                        continue
                    total_mentions += 1
                    words = set(body.split())
                    if words & bull_kw:
                        counts['bullish'] += 1
                    elif words & bear_kw:
                        counts['bearish'] += 1
                    else:
                        counts['neutral'] += 1
                    if len(top_posts) < 5:
                        top_posts.append({
                            'title':  post.get('title', '')[:200],
                            'score':  post.get('score', 0),
                            'url':    f"https://reddit.com{post.get('permalink', '')}",
                            'sub':    sub,
                        })
                time.sleep(1.5)  # respect Reddit rate limits
            except Exception as ex:
                log.debug(f'Reddit r/{sub} {ticker}: {ex}')

        if total_mentions == 0:
            continue
        total = counts['bullish'] + counts['bearish'] + counts['neutral']
        sentiment_score = round((counts['bullish'] - counts['bearish']) / total, 3) if total else 0
        rows.append({
            'ticker':          ticker,
            'scraped_at':      now_ts,
            'total_mentions':  total_mentions,
            'bullish_count':   counts['bullish'],
            'bearish_count':   counts['bearish'],
            'neutral_count':   counts['neutral'],
            'sentiment_score': sentiment_score,
            'top_posts':       top_posts,
        })
        log.info(f'  Reddit {ticker}: {total_mentions} mentions, score={sentiment_score:.2f}')

    log.info(f'  Reddit sentiment: {len(rows)} tickers scraped')
    sb_upsert('reddit_sentiment', rows)
    return len(rows)


# ─── WHAL-155: Morning AI Briefing ───────────────────────────────

RAILWAY_URL = os.getenv('RAILWAY_URL', 'https://whale-tracker-scraper-production.up.railway.app')

def send_morning_briefing():
    """WHAL-155: Trigger morning briefing generation + FCM push at 9:30 AM ET."""
    log.info('── Sending morning briefing …')
    try:
        r = requests.get(f'{RAILWAY_URL}/api/morning-briefing', timeout=30)
        if r.status_code == 200:
            data = r.json()
            log.info(f'Morning briefing sent — regime={data.get("regime")} picks={len(data.get("top_picks", []))}')
            return 1
        log.warning(f'Morning briefing HTTP {r.status_code}')
        return 0
    except Exception as e:
        log.error(f'Morning briefing error: {e}')
        return 0


# ─── Main ─────────────────────────────────────────────────────────

def run():
    start = datetime.now(timezone.utc)
    log.info(f'===== Whale Tracker Daily Cron — {TODAY} =====')

    # WHAL-155: Morning briefing runs separately at 9:30 AM ET via Railway cron
    # This daily cron runs at 5 PM ET for data scraping
    counts = {
        # Existing scrapers (WHAL-115)
        'congress_trades':    scrape_congress_trades(),
        'market_snapshots':   scrape_market_snapshots(),
        'short_interest':     scrape_short_interest(),
        'options_flow_daily': scrape_options_flow(),
        # New scrapers (WHAL-118)
        'insider_trades':        scrape_insider_trades(),
        'dark_pool_prints':      scrape_dark_pool_prints(),
        'options_flow':          scrape_options_flow_new(),
        'congressional_trades':  scrape_congressional_trades(),
        'ticker_fundamentals':   scrape_ticker_fundamentals(),
        # Reddit sentiment (WHAL-119)
        'reddit_sentiment':      scrape_reddit_sentiment(),
    }

    elapsed = (datetime.now(timezone.utc) - start).seconds
    log.info(f'===== Done in {elapsed}s — {counts} =====')


def run_morning():
    """Entrypoint for 9:30 AM ET Railway cron job (WHAL-155)."""
    log.info(f'===== Morning Briefing Cron — {TODAY} =====')
    send_morning_briefing()


if __name__ == '__main__':
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == 'morning':
        run_morning()
    else:
        run()
