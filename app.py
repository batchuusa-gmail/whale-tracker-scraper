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

app = Flask(__name__)

# ── Supabase ─────────────────────────────────────────────────
SUPABASE_URL = os.environ.get('SUPABASE_URL', '')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY', '')
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ── S&P 500 + Major tickers to always track ──────────────────
MAJOR_TICKERS = [
    # Big Tech
    'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META', 'NVDA', 'TSLA', 'NFLX',
    'ORCL', 'CRM', 'ADBE', 'INTC', 'AMD', 'QCOM', 'AVGO', 'TXN',
    # Finance
    'JPM', 'BAC', 'WFC', 'GS', 'MS', 'BLK', 'V', 'MA', 'PYPL', 'AXP',
    # Healthcare
    'JNJ', 'PFE', 'MRK', 'ABBV', 'LLY', 'BMY', 'AMGN', 'GILD',
    # Energy
    'XOM', 'CVX', 'COP', 'SLB', 'EOG', 'PXD',
    # Consumer
    'WMT', 'COST', 'TGT', 'HD', 'LOW', 'NKE', 'SBUX', 'MCD', 'KO', 'PEP',
    # EV & Future Tech
    'RIVN', 'LCID', 'F', 'GM', 'PLTR', 'SNOW', 'COIN', 'HOOD', 'RBLX',
    # Biotech
    'MRNA', 'BNTX', 'REGN', 'VRTX', 'ILMN',
    # Other majors
    'BRK-B', 'UNH', 'LMT', 'RTX', 'BA', 'CAT', 'DE', 'MMM', 'GE', 'HON',
    'DIS', 'CMCSA', 'T', 'VZ', 'TMUS', 'UBER', 'LYFT', 'ABNB', 'BKNG',
]

# ── SEC EDGAR RSS feeds ───────────────────────────────────────
SEC_FEEDS = [
    'https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&dateb=&owner=include&count=100&search_text=&output=atom',
    'https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&dateb=&owner=include&count=100&start=100&output=atom',
    'https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&dateb=&owner=include&count=100&start=200&output=atom',
    'https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&dateb=&owner=include&count=100&start=300&output=atom',
    'https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&dateb=&owner=include&count=100&start=400&output=atom',
]

SEC_HEADERS = {
    'User-Agent': 'WhaleTracker contact@whaletracker.app',
    'Accept-Encoding': 'gzip, deflate',
}

def get_stock_price(ticker):
    """Get current stock price from Yahoo Finance"""
    try:
        stock = yf.Ticker(ticker)
        info = stock.fast_info
        price = info.last_price
        prev_close = info.previous_close
        if price and prev_close:
            change_pct = ((price - prev_close) / prev_close) * 100
            return round(float(price), 2), round(float(change_pct), 2)
    except Exception as e:
        print(f'Price error for {ticker}: {e}')
    return 0.0, 0.0

def parse_filing_xml(filing_url):
    """Parse SEC Form 4 XML to extract trade details"""
    try:
        # Get the filing index page
        r = requests.get(filing_url, headers=SEC_HEADERS, timeout=10)
        if r.status_code != 200:
            return []

        # Find the XML file in the filing
        xml_url = None
        for line in r.text.split('\n'):
            if '.xml' in line.lower() and 'form4' in line.lower():
                import re
                urls = re.findall(r'href="([^"]+\.xml)"', line, re.IGNORECASE)
                if urls:
                    xml_url = 'https://www.sec.gov' + urls[0]
                    break

        if not xml_url:
            # Try to find any XML file
            import re
            urls = re.findall(r'href="(/Archives/edgar/data/[^"]+\.xml)"', r.text)
            if urls:
                xml_url = 'https://www.sec.gov' + urls[0]

        if not xml_url:
            return []

        # Parse the XML
        r2 = requests.get(xml_url, headers=SEC_HEADERS, timeout=10)
        if r2.status_code != 200:
            return []

        root = ET.fromstring(r2.content)
        ns = {'': ''}

        def find_text(element, *paths):
            for path in paths:
                el = element.find(path)
                if el is not None and el.text:
                    return el.text.strip()
            return ''

        # Extract issuer info
        issuer = root.find('.//issuer')
        ticker = find_text(issuer, 'issuerTradingSymbol') if issuer else ''
        company = find_text(issuer, 'issuerName') if issuer else ''

        # Extract owner info
        owner = root.find('.//reportingOwner')
        owner_name = ''
        owner_type = ''
        if owner:
            owner_name = find_text(owner, './/rptOwnerName')
            is_officer = find_text(owner, './/isOfficer')
            is_director = find_text(owner, './/isDirector')
            is_ten_pct = find_text(owner, './/isTenPercentOwner')
            officer_title = find_text(owner, './/officerTitle')
            if officer_title:
                owner_type = officer_title
            elif is_director == '1':
                owner_type = 'Director'
            elif is_ten_pct == '1':
                owner_type = '10% Owner'
            else:
                owner_type = 'Officer'

        if not ticker or not company:
            return []

        # Extract transactions
        filings = []
        for tx in root.findall('.//nonDerivativeTransaction'):
            try:
                tx_code = find_text(tx, './/transactionCode')
                if tx_code not in ['P', 'S']:
                    continue

                shares_str = find_text(tx, './/transactionShares/value')
                price_str = find_text(tx, './/transactionPricePerShare/value')
                date_str = find_text(tx, './/transactionDate/value')
                shares_after_str = find_text(tx, './/sharesOwnedFollowingTransaction/value')

                shares = float(shares_str) if shares_str else 0
                price = float(price_str) if price_str else 0
                shares_after = float(shares_after_str) if shares_after_str else 0
                value = shares * price

                if shares <= 0:
                    continue

                is_buy = tx_code == 'P'

                filings.append({
                    'ticker': ticker.upper(),
                    'company_name': company,
                    'owner_name': owner_name,
                    'owner_type': owner_type,
                    'transaction_type': 'Buy' if is_buy else 'Sell',
                    'shares': shares,
                    'price': price,
                    'value': value,
                    'transaction_date': date_str,
                    'filed_at': datetime.now().strftime('%Y-%m-%d'),
                    'filing_url': xml_url,
                    'shares_owned_after': shares_after,
                    'stock_price': 0.0,
                    'stock_change_pct': 0.0,
                })
            except Exception as e:
                print(f'TX parse error: {e}')
                continue

        return filings

    except Exception as e:
        print(f'Filing parse error: {e}')
        return []

def scrape_sec_filings(max_filings=500):
    """Scrape SEC EDGAR for latest Form 4 filings"""
    print(f'Starting SEC scrape - target: {max_filings} filings')
    all_filings = []
    seen_urls = set()

    for feed_url in SEC_FEEDS:
        if len(all_filings) >= max_filings:
            break
        try:
            print(f'Fetching feed: {feed_url}')
            r = requests.get(feed_url, headers=SEC_HEADERS, timeout=15)
            if r.status_code != 200:
                continue

            root = ET.fromstring(r.content)
            entries = root.findall('.//{http://www.w3.org/2005/Atom}entry')
            print(f'Found {len(entries)} entries in feed')

            for entry in entries:
                if len(all_filings) >= max_filings:
                    break

                link_el = entry.find('{http://www.w3.org/2005/Atom}link')
                if link_el is None:
                    continue

                filing_url = link_el.get('href', '')
                if not filing_url or filing_url in seen_urls:
                    continue

                seen_urls.add(filing_url)
                filings = parse_filing_xml(filing_url)
                if filings:
                    all_filings.extend(filings)
                    print(f'Parsed {len(filings)} transactions from {filing_url}')

                time.sleep(0.15)  # SEC rate limit

        except Exception as e:
            print(f'Feed error: {e}')
            continue

    print(f'Total filings scraped: {len(all_filings)}')
    return all_filings

def scrape_major_tickers():
    """Specifically scrape filings for major tickers"""
    print('Scraping major tickers...')
    all_filings = []

    for ticker in MAJOR_TICKERS:
        try:
            url = f'https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&company=&CIK={ticker}&type=4&dateb=&owner=include&count=10&search_text=&output=atom'
            r = requests.get(url, headers=SEC_HEADERS, timeout=10)
            if r.status_code != 200:
                continue

            root = ET.fromstring(r.content)
            entries = root.findall('.//{http://www.w3.org/2005/Atom}entry')

            for entry in entries[:5]:  # Last 5 filings per ticker
                link_el = entry.find('{http://www.w3.org/2005/Atom}link')
                if link_el is None:
                    continue
                filing_url = link_el.get('href', '')
                if not filing_url:
                    continue
                filings = parse_filing_xml(filing_url)
                all_filings.extend(filings)
                time.sleep(0.1)

        except Exception as e:
            print(f'Major ticker error {ticker}: {e}')
            continue

    print(f'Major ticker filings: {len(all_filings)}')
    return all_filings

def store_filings(filings):
    """Store filings in Supabase, update stock prices for significant ones"""
    if not filings:
        return 0

    stored = 0
    # Get stock prices for high value filings
    price_cache = {}

    for f in filings:
        try:
            ticker = f['ticker']

            # Get stock price (cache to avoid repeated calls)
            if ticker not in price_cache:
                price, change_pct = get_stock_price(ticker)
                price_cache[ticker] = (price, change_pct)
                time.sleep(0.05)

            f['stock_price'] = price_cache[ticker][0]
            f['stock_change_pct'] = price_cache[ticker][1]

            # Upsert to Supabase (avoid duplicates)
            result = supabase.table('filings').upsert(f, on_conflict='ticker,owner_name,transaction_date,shares').execute()
            stored += 1

        except Exception as e:
            print(f'Store error: {e}')
            continue

    print(f'Stored {stored} filings in Supabase')
    return stored

def full_refresh():
    """Full refresh - scrape everything and store in DB"""
    print('=== FULL REFRESH STARTED ===')
    start = time.time()

    # Scrape general filings
    filings = scrape_sec_filings(max_filings=500)

    # Also scrape major tickers specifically  
    major_filings = scrape_major_tickers()
    filings.extend(major_filings)

    # Deduplicate
    seen = set()
    unique_filings = []
    for f in filings:
        key = f"{f['ticker']}_{f['owner_name']}_{f['transaction_date']}_{f['shares']}"
        if key not in seen:
            seen.add(key)
            unique_filings.append(f)

    print(f'Unique filings: {len(unique_filings)}')
    stored = store_filings(unique_filings)
    elapsed = round(time.time() - start, 1)
    print(f'=== FULL REFRESH DONE: {stored} stored in {elapsed}s ===')
    return stored

# ── API Routes ────────────────────────────────────────────────

@app.route('/api/health')
def health():
    return jsonify({'status': 'ok', 'timestamp': datetime.now().isoformat()})

@app.route('/api/summary')
def summary():
    try:
        result = supabase.table('filings').select('*').order('filed_at', desc=True).limit(500).execute()
        filings = result.data or []

        buys = [f for f in filings if f.get('transaction_type') == 'Buy']
        sells = [f for f in filings if f.get('transaction_type') == 'Sell']
        total_value = sum(f.get('value', 0) for f in filings)

        return jsonify({
            'total_filings': len(filings),
            'total_buys': len(buys),
            'total_sells': len(sells),
            'total_value': total_value,
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

@app.route('/api/filings/refresh', methods=['GET', 'POST'])
def refresh_filings():
    try:
        import threading
        thread = threading.Thread(target=full_refresh)
        thread.daemon = True
        thread.start()
        return jsonify({'status': 'refresh started', 'message': 'Scraping 500+ filings in background'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

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
            .order('value', desc=True).limit(200).execute()

        # Aggregate by ticker
        ticker_data = {}
        for f in (result.data or []):
            t = f['ticker']
            if t not in ticker_data:
                ticker_data[t] = {
                    'ticker': t,
                    'company_name': f['company_name'],
                    'buy_value': 0,
                    'sell_value': 0,
                    'buy_count': 0,
                    'sell_count': 0,
                }
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

        points = []
        for ts, row in hist.iterrows():
            points.append({
                'o': round(float(row['Open']), 2),
                'h': round(float(row['High']), 2),
                'l': round(float(row['Low']), 2),
                'c': round(float(row['Close']), 2),
                'v': int(row['Volume']),
                't': ts.isoformat(),
            })

        current = round(float(hist['Close'].iloc[-1]), 2)
        prev_close = round(float(hist['Close'].iloc[-2]), 2) if len(hist) > 1 else current

        return jsonify({
            'ticker': ticker.upper(),
            'period': period,
            'interval': interval,
            'current': current,
            'prev_close': prev_close,
            'points': points,
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/search')
def search():
    try:
        q = request.args.get('q', '').upper()
        if not q:
            return jsonify([])
        result = supabase.table('filings').select('ticker,company_name')\
            .or_(f'ticker.ilike.%{q}%,company_name.ilike.%{q}%')\
            .limit(20).execute()
        # Deduplicate
        seen = set()
        stocks = []
        for f in (result.data or []):
            if f['ticker'] not in seen:
                seen.add(f['ticker'])
                stocks.append({'ticker': f['ticker'], 'company_name': f['company_name']})
        return jsonify(stocks)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ── Scheduler - auto refresh every 6 hours ───────────────────
scheduler = BackgroundScheduler()
scheduler.add_job(full_refresh, 'interval', hours=6, id='full_refresh')
scheduler.start()

if __name__ == '__main__':
    # Run initial scrape on startup
    import threading
    t = threading.Thread(target=full_refresh)
    t.daemon = True
    t.start()

    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)
