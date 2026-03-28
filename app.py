import os
import json
import time
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from flask import Flask, jsonify, request
from supabase import create_client, Client
import yfinance as yf
from apscheduler.schedulers.background import BackgroundScheduler
import threading
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)

# ── Supabase ──────────────────────────────────────────────────
SUPABASE_URL = os.environ.get('SUPABASE_URL', '')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY', '')
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ── Major tickers to track historically ──────────────────────
MAJOR_TICKERS = [
    # Big Tech
    'AAPL', 'MSFT', 'GOOGL', 'GOOG', 'AMZN', 'META', 'NVDA', 'TSLA',
    'NFLX', 'ORCL', 'CRM', 'ADBE', 'INTC', 'AMD', 'QCOM', 'AVGO',
    'TXN', 'MU', 'AMAT', 'LRCX', 'KLAC', 'SNOW', 'PLTR', 'COIN',
    # Finance
    'JPM', 'BAC', 'WFC', 'GS', 'MS', 'BLK', 'V', 'MA', 'PYPL', 'AXP',
    'C', 'USB', 'PNC', 'TFC', 'SCHW', 'BX', 'KKR', 'APO',
    # Healthcare
    'JNJ', 'PFE', 'MRK', 'ABBV', 'LLY', 'BMY', 'AMGN', 'GILD',
    'MRNA', 'BNTX', 'REGN', 'VRTX', 'ILMN', 'ISRG',
    # Energy
    'XOM', 'CVX', 'COP', 'SLB', 'EOG', 'PXD', 'OXY', 'VLO', 'MPC',
    # Consumer
    'WMT', 'COST', 'TGT', 'HD', 'LOW', 'NKE', 'SBUX', 'MCD',
    'KO', 'PEP', 'PM', 'MO', 'CL', 'PG', 'UL',
    # EV & Mobility
    'RIVN', 'LCID', 'F', 'GM', 'UBER', 'LYFT',
    # Communication
    'DIS', 'CMCSA', 'T', 'VZ', 'TMUS', 'CHTR',
    # Other majors
    'BRK-B', 'UNH', 'LMT', 'RTX', 'BA', 'CAT', 'DE', 'HON',
    'ABNB', 'BKNG', 'EXPE', 'RBLX', 'HOOD', 'SOFI',
    'SHOP', 'SQ', 'TWLO', 'ZM', 'DOCU', 'OKTA',
    'SPCE', 'ASTS', 'RKLB', 'LUNR',
]

SEC_HEADERS = {
    'User-Agent': 'WhaleTracker contact@whaletracker.app',
    'Accept-Encoding': 'gzip, deflate',
}

# ── Helper: get stock price ───────────────────────────────────
def get_stock_price(ticker):
    try:
        stock = yf.Ticker(ticker)
        info = stock.fast_info
        price = float(info.last_price or 0)
        prev = float(info.previous_close or 0)
        change_pct = ((price - prev) / prev * 100) if prev else 0
        return round(price, 2), round(change_pct, 2)
    except:
        return 0.0, 0.0

# ── Parse SEC Form 4 XML ──────────────────────────────────────
def parse_form4_xml(xml_url):
    """Parse a Form 4 XML file and return list of transactions"""
    try:
        r = requests.get(xml_url, headers=SEC_HEADERS, timeout=15)
        if r.status_code != 200:
            return []
        root = ET.fromstring(r.content)

        def ft(el, *tags):
            for tag in tags:
                found = el.find(f'.//{tag}')
                if found is not None and found.text:
                    return found.text.strip()
            return ''

        issuer = root.find('.//issuer')
        ticker = ft(issuer, 'issuerTradingSymbol') if issuer else ''
        company = ft(issuer, 'issuerName') if issuer else ''
        if not ticker:
            return []

        owner = root.find('.//reportingOwner')
        owner_name = ft(owner, 'rptOwnerName') if owner else ''
        officer_title = ft(owner, 'officerTitle') if owner else ''
        is_director = ft(owner, 'isDirector') if owner else '0'
        is_officer = ft(owner, 'isOfficer') if owner else '0'
        is_ten_pct = ft(owner, 'isTenPercentOwner') if owner else '0'

        if officer_title:
            owner_type = officer_title
        elif is_director == '1':
            owner_type = 'Director'
        elif is_ten_pct == '1':
            owner_type = '10% Owner'
        elif is_officer == '1':
            owner_type = 'Officer'
        else:
            owner_type = 'Insider'

        transactions = []
        for tx in root.findall('.//nonDerivativeTransaction'):
            try:
                code = ft(tx, 'transactionCode')
                if code not in ['P', 'S']:
                    continue
                shares = float(ft(tx, 'transactionShares') or ft(tx, 'value') or '0')
                price = float(ft(tx, 'transactionPricePerShare') or '0')
                date = ft(tx, 'transactionDate')
                shares_after = float(ft(tx, 'sharesOwnedFollowingTransaction') or '0')
                value = shares * price
                if shares <= 0:
                    continue
                transactions.append({
                    'ticker': ticker.upper().strip(),
                    'company_name': company,
                    'owner_name': owner_name,
                    'owner_type': owner_type,
                    'transaction_type': 'Buy' if code == 'P' else 'Sell',
                    'shares': shares,
                    'price': price,
                    'value': value,
                    'transaction_date': date,
                    'filed_at': datetime.now().strftime('%Y-%m-%d'),
                    'filing_url': xml_url,
                    'shares_owned_after': shares_after,
                    'stock_price': 0.0,
                    'stock_change_pct': 0.0,
                })
            except Exception as e:
                logger.error(f'TX parse error: {e}')
        return transactions
    except Exception as e:
        logger.error(f'XML parse error {xml_url}: {e}')
        return []

# ── Get filings index for a CIK ──────────────────────────────
def get_cik_for_ticker(ticker):
    """Get SEC CIK number for a ticker"""
    try:
        url = f'https://efts.sec.gov/LATEST/search-index?q=%22{ticker}%22&dateRange=custom&startdt=2020-01-01&forms=4'
        r = requests.get(url, headers=SEC_HEADERS, timeout=10)
        if r.status_code == 200:
            data = r.json()
            hits = data.get('hits', {}).get('hits', [])
            if hits:
                return hits[0].get('_source', {}).get('period_of_report', '')
    except:
        pass

    # Try company search
    try:
        url = f'https://www.sec.gov/cgi-bin/browse-edgar?company=&CIK={ticker}&type=4&dateb=&owner=include&count=1&search_text=&action=getcompany&output=atom'
        r = requests.get(url, headers=SEC_HEADERS, timeout=10)
        if r.status_code == 200:
            root = ET.fromstring(r.content)
            entries = root.findall('.//{http://www.w3.org/2005/Atom}entry')
            if entries:
                link = entries[0].find('{http://www.w3.org/2005/Atom}link')
                if link is not None:
                    href = link.get('href', '')
                    # Extract CIK from URL
                    import re
                    match = re.search(r'CIK=(\d+)', href)
                    if match:
                        return match.group(1)
    except:
        pass
    return None

def get_historical_filings_for_ticker(ticker, years_back=3):
    """Pull all Form 4 filings for a specific ticker going back N years"""
    logger.info(f'Pulling historical filings for {ticker}...')
    all_filings = []

    try:
        # Use SEC EDGAR full-text search
        url = f'https://efts.sec.gov/LATEST/search-index?q=%22{ticker}%22&forms=4&dateRange=custom&startdt={(datetime.now() - timedelta(days=365*years_back)).strftime("%Y-%m-%d")}&enddt={datetime.now().strftime("%Y-%m-%d")}'
        r = requests.get(url, headers=SEC_HEADERS, timeout=15)

        if r.status_code == 200:
            data = r.json()
            hits = data.get('hits', {}).get('hits', [])
            logger.info(f'{ticker}: found {len(hits)} filings in search')

            for hit in hits[:50]:  # Max 50 per ticker
                try:
                    source = hit.get('_source', {})
                    file_date = source.get('file_date', '')
                    entity_id = source.get('entity_id', '')
                    accession = hit.get('_id', '').replace('-', '')

                    if entity_id and accession:
                        # Build XML URL
                        xml_url = f'https://www.sec.gov/Archives/edgar/data/{entity_id}/{accession}/{accession}-index.htm'
                        # Try to find the actual XML
                        idx_r = requests.get(xml_url, headers=SEC_HEADERS, timeout=10)
                        if idx_r.status_code == 200:
                            import re
                            xml_files = re.findall(r'href="(/Archives/edgar/data/[^"]+\.xml)"', idx_r.text)
                            for xf in xml_files:
                                filings = parse_form4_xml('https://www.sec.gov' + xf)
                                if filings:
                                    # Update filed_at with actual date
                                    for f in filings:
                                        f['filed_at'] = file_date or f['filed_at']
                                    all_filings.extend(filings)
                                    break
                    time.sleep(0.1)
                except Exception as e:
                    logger.error(f'Hit parse error: {e}')
                    continue

    except Exception as e:
        logger.error(f'Historical fetch error for {ticker}: {e}')

    # Fallback: use EDGAR RSS for ticker
    if not all_filings:
        try:
            url = f'https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&company=&CIK={ticker}&type=4&dateb=&owner=include&count=40&search_text=&output=atom'
            r = requests.get(url, headers=SEC_HEADERS, timeout=15)
            if r.status_code == 200:
                root = ET.fromstring(r.content)
                entries = root.findall('.//{http://www.w3.org/2005/Atom}entry')
                logger.info(f'{ticker} RSS: found {len(entries)} entries')

                for entry in entries[:20]:
                    try:
                        link = entry.find('{http://www.w3.org/2005/Atom}link')
                        if link is None:
                            continue
                        filing_index_url = link.get('href', '')
                        if not filing_index_url:
                            continue

                        # Get the index page and find XML
                        idx_r = requests.get(filing_index_url, headers=SEC_HEADERS, timeout=10)
                        if idx_r.status_code != 200:
                            continue

                        import re
                        xml_files = re.findall(r'href="(/Archives/edgar/data/[^"]+\.xml)"', idx_r.text)
                        for xf in xml_files:
                            filings = parse_form4_xml('https://www.sec.gov' + xf)
                            if filings:
                                all_filings.extend(filings)
                                break
                        time.sleep(0.1)
                    except Exception as e:
                        logger.error(f'RSS entry error: {e}')
                        continue
        except Exception as e:
            logger.error(f'RSS fallback error for {ticker}: {e}')

    logger.info(f'{ticker}: total {len(all_filings)} transactions found')
    return all_filings

def scrape_recent_feed(max_entries=500):
    """Scrape recent SEC EDGAR Form 4 feed"""
    all_filings = []
    seen_urls = set()
    import re

    feeds = [
        f'https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&dateb=&owner=include&count=100&start={i*100}&output=atom'
        for i in range(5)
    ]

    for feed_url in feeds:
        if len(all_filings) >= max_entries:
            break
        try:
            r = requests.get(feed_url, headers=SEC_HEADERS, timeout=15)
            if r.status_code != 200:
                continue
            root = ET.fromstring(r.content)
            entries = root.findall('.//{http://www.w3.org/2005/Atom}entry')
            logger.info(f'Feed {feed_url[-20:]}: {len(entries)} entries')

            for entry in entries:
                if len(all_filings) >= max_entries:
                    break
                link = entry.find('{http://www.w3.org/2005/Atom}link')
                if link is None:
                    continue
                url = link.get('href', '')
                if not url or url in seen_urls:
                    continue
                seen_urls.add(url)

                idx_r = requests.get(url, headers=SEC_HEADERS, timeout=10)
                if idx_r.status_code != 200:
                    continue

                xml_files = re.findall(r'href="(/Archives/edgar/data/[^"]+\.xml)"', idx_r.text)
                for xf in xml_files:
                    filings = parse_form4_xml('https://www.sec.gov' + xf)
                    if filings:
                        all_filings.extend(filings)
                        break
                time.sleep(0.1)
        except Exception as e:
            logger.error(f'Feed error: {e}')

    logger.info(f'Recent feed total: {len(all_filings)}')
    return all_filings

def store_filings(filings):
    """Deduplicate and store filings in Supabase"""
    if not filings:
        return 0

    # Get stock prices in batch
    price_cache = {}
    stored = 0

    for f in filings:
        try:
            ticker = f['ticker']
            if ticker not in price_cache:
                price, chg = get_stock_price(ticker)
                price_cache[ticker] = (price, chg)
                time.sleep(0.05)

            f['stock_price'] = price_cache[ticker][0]
            f['stock_change_pct'] = price_cache[ticker][1]

            supabase.table('filings').upsert(
                f,
                on_conflict='ticker,owner_name,transaction_date,transaction_type,shares'
            ).execute()
            stored += 1
        except Exception as e:
            logger.error(f'Store error: {e}')

    logger.info(f'Stored {stored}/{len(filings)} filings')
    return stored

def pull_historical_major_tickers():
    """Pull 3 years of history for all major tickers"""
    logger.info('=== PULLING HISTORICAL DATA FOR MAJOR TICKERS ===')
    all_filings = []

    for i, ticker in enumerate(MAJOR_TICKERS):
        logger.info(f'[{i+1}/{len(MAJOR_TICKERS)}] Pulling history for {ticker}')
        filings = get_historical_filings_for_ticker(ticker, years_back=3)
        all_filings.extend(filings)

        # Store in batches of 50
        if len(all_filings) >= 50:
            store_filings(all_filings)
            all_filings = []
        time.sleep(0.2)

    # Store remaining
    if all_filings:
        store_filings(all_filings)

    logger.info('=== HISTORICAL PULL COMPLETE ===')

def full_refresh():
    """Pull recent filings + major ticker history"""
    logger.info('=== FULL REFRESH STARTED ===')

    # 1. Pull recent feed (last 500 filings)
    recent = scrape_recent_feed(500)
    store_filings(recent)

    # 2. Pull major ticker history in background
    threading.Thread(target=pull_historical_major_tickers, daemon=True).start()

    logger.info('=== REFRESH INITIATED ===')

# ── API Routes ─────────────────────────────────────────────────
@app.route('/api/health')
def health():
    key = os.environ.get('ANTHROPIC_API_KEY', '')
    return jsonify({
        'status': 'ok',
        'version': '2026-03-28-app',
        'timestamp': datetime.now().isoformat(),
        'anthropic_configured': bool(key),
    })

@app.route('/api/signal/<ticker>')
def ai_signal(ticker):
    key = os.environ.get('ANTHROPIC_API_KEY', '')
    if not key:
        return jsonify({'error': 'AI signals not configured'}), 503
    try:
        import anthropic as _anthropic
        ticker = ticker.upper()
        # Fetch recent filings for this ticker from Supabase
        result = supabase.table('filings').select('*').eq('ticker', ticker).order('filed_at', desc=True).limit(5).execute()
        filings = result.data or []
        if not filings:
            return jsonify({'error': 'No filings found for this ticker'}), 404

        lines = []
        for f in filings[:3]:
            name = f.get('owner_name', 'Unknown')
            ttype = f.get('transaction_type', '')
            val = float(f.get('value', 0) or 0)
            shares = int(f.get('shares', 0) or 0)
            fp = float(f.get('price', 0) or 0)
            date = f.get('transaction_date', f.get('filed_at', ''))
            lines.append(f"- {name} {ttype} ${val:,.0f} of {shares:,.0f} shares at ${fp:.2f} on {date}")
        filing_summary = '\n'.join(lines)

        client = _anthropic.Anthropic(api_key=key)
        msg = client.messages.create(
            model='claude-haiku-4-5-20251001',
            max_tokens=400,
            messages=[{'role': 'user', 'content': f"""Analyze insider trading for {ticker}:
{filing_summary}

Reply ONLY with valid JSON (no markdown):
{{"signal":"BULLISH|BEARISH|NEUTRAL","confidence":0-100,"summary":"2 sentence max","key_factors":["factor1","factor2","factor3"]}}"""}]
        )
        import json as _json
        text = msg.content[0].text.strip()
        if text.startswith('```'):
            text = text.split('```')[1]
            if text.startswith('json'):
                text = text[4:]
        data = _json.loads(text)
        return jsonify(data)
    except Exception as e:
        logger.error(f'AI signal error: {e}')
        return jsonify({'error': str(e)}), 500

@app.route('/api/summary')
def summary():
    try:
        result = supabase.table('filings').select('*').order('filed_at', desc=True).limit(1000).execute()
        filings = result.data or []
        buys = [f for f in filings if f.get('transaction_type') == 'Buy']
        sells = [f for f in filings if f.get('transaction_type') == 'Sell']
        return jsonify({
            'total_filings': len(filings),
            'total_buys': len(buys),
            'total_sells': len(sells),
            'total_value': sum(f.get('value', 0) for f in filings),
            'buy_value': sum(f.get('value', 0) for f in buys),
            'sell_value': sum(f.get('value', 0) for f in sells),
            'sentiment': 'Bullish' if len(buys) > len(sells) else 'Bearish',
            'last_updated': filings[0].get('filed_at', '') if filings else '',
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/filings')
def get_filings():
    try:
        limit = min(int(request.args.get('limit', 100)), 500)
        tx_type = request.args.get('type', '')
        ticker = request.args.get('ticker', '').upper()
        query = supabase.table('filings').select('*').order('filed_at', desc=True)
        if tx_type:
            query = query.eq('transaction_type', tx_type)
        if ticker:
            query = query.eq('ticker', ticker)
        result = query.limit(limit).execute()
        return jsonify(result.data or [])
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/filings/refresh')
def refresh():
    threading.Thread(target=full_refresh, daemon=True).start()
    return jsonify({'status': 'started', 'message': 'Pulling recent filings + 3 years history for 70+ major tickers'})

@app.route('/api/filings/history-pull')
def history_pull():
    """Trigger historical pull for major tickers only"""
    threading.Thread(target=pull_historical_major_tickers, daemon=True).start()
    return jsonify({'status': 'started', 'message': f'Pulling 3yr history for {len(MAJOR_TICKERS)} major tickers'})

@app.route('/api/company/<ticker>')
def get_company(ticker):
    try:
        result = supabase.table('filings').select('*')\
            .eq('ticker', ticker.upper())\
            .order('transaction_date', desc=True)\
            .limit(50).execute()
        return jsonify(result.data or [])
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/top-stocks')
def top_stocks():
    try:
        result = supabase.table('filings').select('ticker,company_name,transaction_type,value')\
            .order('value', desc=True).limit(500).execute()
        ticker_data = {}
        for f in (result.data or []):
            t = f['ticker']
            if t not in ticker_data:
                ticker_data[t] = {'ticker': t, 'company_name': f['company_name'],
                    'buy_value': 0, 'sell_value': 0, 'buy_count': 0, 'sell_count': 0}
            if f['transaction_type'] == 'Buy':
                ticker_data[t]['buy_value'] += f.get('value', 0)
                ticker_data[t]['buy_count'] += 1
            else:
                ticker_data[t]['sell_value'] += f.get('value', 0)
                ticker_data[t]['sell_count'] += 1
        stocks = sorted(ticker_data.values(),
            key=lambda x: x['buy_value'] + x['sell_value'], reverse=True)[:20]
        return jsonify(stocks)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/history')
def history():
    try:
        days = int(request.args.get('days', 30))
        since = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        result = supabase.table('filings').select('*')\
            .gte('transaction_date', since)\
            .order('transaction_date', desc=True)\
            .limit(500).execute()
        return jsonify(result.data or [])
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/chart/<ticker>')
def chart(ticker):
    try:
        period = request.args.get('period', '1mo')
        interval = request.args.get('interval', '1d')
        stock = yf.Ticker(ticker)
        hist = stock.history(period=period, interval=interval)
        if hist.empty:
            return jsonify({'error': 'No data', 'points': []}), 404
        points = [{'o': round(float(r['Open']),2), 'h': round(float(r['High']),2),
            'l': round(float(r['Low']),2), 'c': round(float(r['Close']),2),
            'v': int(r['Volume']), 't': ts.isoformat()}
            for ts, r in hist.iterrows()]
        current = round(float(hist['Close'].iloc[-1]), 2)
        prev_close = round(float(hist['Close'].iloc[-2]), 2) if len(hist) > 1 else current
        return jsonify({'ticker': ticker.upper(), 'period': period,
            'interval': interval, 'current': current, 'prev_close': prev_close, 'points': points})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/search')
def search():
    try:
        q = request.args.get('q', '').upper()
        if not q:
            return jsonify([])
        result = supabase.table('filings').select('ticker,company_name')\
            .or_(f'ticker.ilike.%{q}%,company_name.ilike.%{q}%').limit(20).execute()
        seen = set()
        stocks = []
        for f in (result.data or []):
            if f['ticker'] not in seen:
                seen.add(f['ticker'])
                stocks.append({'ticker': f['ticker'], 'company_name': f['company_name']})
        return jsonify(stocks)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ── Auto refresh every 6 hours ────────────────────────────────
scheduler = BackgroundScheduler()
scheduler.add_job(full_refresh, 'interval', hours=6)
scheduler.start()

if __name__ == '__main__':
    # Pull history on startup
    threading.Thread(target=full_refresh, daemon=True).start()
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)
