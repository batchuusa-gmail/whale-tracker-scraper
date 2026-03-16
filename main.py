#!/usr/bin/env python3
"""
SEC Form 4 Insider Trading Scraper
- Real data from SEC EDGAR ATOM feed
- Saves to Supabase for historical storage
- Enriches with Yahoo Finance stock prices
"""
import os, re, json, time, random, requests, threading
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from flask import Flask, jsonify, request
from flask_cors import CORS
import schedule
import logging
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
load_dotenv()

SEC_BASE_URL = "https://www.sec.gov"
USER_AGENT = "WhaleTracker batchuusa@gmail.com"
SEC_HEADERS = {'User-Agent': USER_AGENT, 'Accept-Encoding': 'gzip, deflate', 'Accept': '*/*'}
SEC_DELAY = 0.6

SUPABASE_URL = os.getenv('SUPABASE_URL', 'https://bedurjtazsfbnkisoeee.supabase.co')
SUPABASE_KEY = os.getenv('SUPABASE_KEY', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJlZHVyanRhenNmYm5raXNvZWVlIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzM2NDcxOTUsImV4cCI6MjA4OTIyMzE5NX0.MK4N9dxAIHlXkGP4rLJPq5tHh9UU8L75EB1b8Q7CVmg')
SUPABASE_HEADERS = {
    'apikey': SUPABASE_KEY,
    'Authorization': f'Bearer {SUPABASE_KEY}',
    'Content-Type': 'application/json',
    'Prefer': 'resolution=ignore-duplicates,return=minimal',
}

_filings_cache = []
_last_updated = None


# ─── Supabase ────────────────────────────────────────────────────

class SupabaseClient:
    def __init__(self):
        self.base = f"{SUPABASE_URL}/rest/v1"

    def upsert(self, table, rows):
        if not rows:
            return
        try:
            resp = requests.post(
                f"{self.base}/{table}",
                headers=SUPABASE_HEADERS,
                json=rows,
                timeout=15,
            )
            if resp.status_code in (200, 201, 204):
                logger.info(f"Saved {len(rows)} rows to Supabase {table}")
            else:
                logger.error(f"Supabase upsert error: {resp.status_code} {resp.text[:200]}")
        except Exception as e:
            logger.error(f"Supabase upsert exception: {e}")

    def get_recent(self, table, limit=100):
        try:
            resp = requests.get(
                f"{self.base}/{table}?order=filed_at.desc&limit={limit}",
                headers=SUPABASE_HEADERS,
                timeout=15,
            )
            if resp.status_code == 200:
                return resp.json()
        except Exception as e:
            logger.error(f"Supabase get error: {e}")
        return []

    def get_summary(self):
        try:
            resp = requests.get(
                f"{self.base}/filings?select=transaction_type,value",
                headers=SUPABASE_HEADERS,
                timeout=15,
            )
            if resp.status_code == 200:
                return resp.json()
        except Exception as e:
            logger.error(f"Supabase summary error: {e}")
        return []

    def get_by_ticker(self, ticker, limit=50):
        try:
            resp = requests.get(
                f"{self.base}/filings?ticker=eq.{ticker}&order=filed_at.desc&limit={limit}",
                headers=SUPABASE_HEADERS,
                timeout=15,
            )
            if resp.status_code == 200:
                return resp.json()
        except Exception as e:
            logger.error(f"Supabase ticker error: {e}")
        return []


# ─── Yahoo Finance ───────────────────────────────────────────────

class YahooFinance:
    HEADERS = {'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X)'}

    @staticmethod
    def get_quote(ticker):
        try:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval=1d&range=2d"
            resp = requests.get(url, headers=YahooFinance.HEADERS, timeout=8)
            if resp.status_code == 200:
                data = resp.json()
                result = data.get('chart', {}).get('result', [{}])[0]
                meta = result.get('meta', {})
                price = meta.get('regularMarketPrice', 0)
                prev = meta.get('chartPreviousClose', meta.get('previousClose', price))
                change_pct = ((price - prev) / prev * 100) if prev else 0
                return {
                    'stock_price': round(float(price), 2),
                    'stock_change_pct': round(float(change_pct), 2),
                }
        except Exception as e:
            logger.error(f"Yahoo Finance error for {ticker}: {e}")
        return {'stock_price': 0.0, 'stock_change_pct': 0.0}

    @staticmethod
    def get_quotes_batch(tickers):
        results = {}
        for ticker in tickers:
            if ticker:
                results[ticker] = YahooFinance.get_quote(ticker)
                time.sleep(0.2)
        return results


# ─── SEC Scraper ─────────────────────────────────────────────────

class SECScraper:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(SEC_HEADERS)
        self.db = SupabaseClient()

    def rate_limit(self):
        time.sleep(SEC_DELAY + random.uniform(0, 0.3))

    def fetch_atom_feed(self, limit=50):
        results = []
        try:
            url = f"{SEC_BASE_URL}/cgi-bin/browse-edgar?action=getcurrent&type=4&dateb=&owner=include&count={limit}&search_text=&output=atom"
            self.rate_limit()
            resp = self.session.get(url, timeout=20)
            resp.raise_for_status()
            root = ET.fromstring(resp.content)
            ns = {'atom': 'http://www.w3.org/2005/Atom'}
            entries = root.findall('atom:entry', ns)
            logger.info(f"ATOM feed: {len(entries)} entries")
            for entry in entries:
                try:
                    title = entry.findtext('atom:title', '', ns)
                    updated = entry.findtext('atom:updated', '', ns)
                    link_el = entry.find('atom:link', ns)
                    filing_url = link_el.get('href', '') if link_el is not None else ''
                    ticker_match = re.search(r'\(([A-Z]{1,5})\)', title)
                    company_match = re.search(r'4\s*-\s*(.+?)\s*\(', title)
                    ticker = ticker_match.group(1) if ticker_match else ''
                    company = company_match.group(1).strip() if company_match else title
                    results.append({
                        'ticker': ticker,
                        'company_name': company,
                        'filing_url': filing_url,
                        'filed_at': updated[:10] if updated else '',
                        'owner_name': '',
                        'owner_type': '',
                        'transaction_type': '',
                        'shares': 0,
                        'price': 0.0,
                        'value': 0.0,
                        'transaction_date': None,
                        'shares_owned_after': 0,
                        'stock_price': 0.0,
                        'stock_change_pct': 0.0,
                    })
                except Exception as e:
                    logger.error(f"Entry parse error: {e}")
        except Exception as e:
            logger.error(f"ATOM feed error: {e}")
        return results

    def enrich_from_xml(self, filing):
        try:
            filing_url = filing.get('filing_url', '')
            if not filing_url:
                return filing
            self.rate_limit()
            resp = self.session.get(filing_url, timeout=15)
            if resp.status_code != 200:
                return filing
            soup = BeautifulSoup(resp.content, 'html.parser')
            xml_link = None
            for a in soup.find_all('a', href=True):
                h = a['href']
                if h.endswith('.xml') and '/Archives/' in h:
                    xml_link = f"{SEC_BASE_URL}{h}" if h.startswith('/') else h
                    break
            if xml_link:
                self.rate_limit()
                xr = self.session.get(xml_link, timeout=15)
                if xr.status_code == 200:
                    self._parse_xml_into(filing, xr.text)
        except Exception as e:
            logger.error(f"Enrich error: {e}")
        return filing

    def _parse_xml_into(self, filing, xml_text):
        try:
            try:
                root = ET.fromstring(xml_text)
            except ET.ParseError:
                from lxml import etree as lxml_et
                parser = lxml_et.XMLParser(recover=True)
                lxml_root = lxml_et.fromstring(xml_text.encode(), parser)
                xml_text = lxml_et.tostring(lxml_root).decode()
                root = ET.fromstring(xml_text)

            def ft(path):
                el = root.find(path)
                return el.text.strip() if el is not None and el.text else ''

            filing['owner_name'] = ft('.//rptOwnerName')
            rel = root.find('.//reportingOwnerRelationship')
            if rel is not None:
                if rel.findtext('isOfficer') == '1':
                    filing['owner_type'] = rel.findtext('officerTitle') or 'Officer'
                elif rel.findtext('isDirector') == '1':
                    filing['owner_type'] = 'Director'
                elif rel.findtext('isTenPercentOwner') == '1':
                    filing['owner_type'] = '10% Owner'

            if not filing.get('ticker'):
                filing['ticker'] = ft('.//issuerTradingSymbol').upper()
            if not filing.get('company_name'):
                filing['company_name'] = ft('.//issuerName')

            txs = root.findall('.//nonDerivativeTransaction')
            if txs:
                tx = txs[0]
                code = tx.findtext('.//transactionCode') or ''
                shares_el = tx.find('.//transactionShares/value')
                price_el = tx.find('.//transactionPricePerShare/value')
                date_el = tx.find('.//transactionDate/value')
                owned_el = tx.find('.//sharesOwnedFollowingTransaction/value')
                shares = float(shares_el.text) if shares_el is not None and shares_el.text else 0
                price = float(price_el.text) if price_el is not None and price_el.text else 0
                filing['transaction_type'] = 'Buy' if code in ('P', 'A') else 'Sell' if code in ('S', 'D', 'F') else code
                filing['shares'] = int(shares)
                filing['price'] = round(price, 2)
                filing['value'] = round(shares * price, 2)
                tx_date = date_el.text if date_el is not None else None
                filing['transaction_date'] = tx_date
                filing['shares_owned_after'] = int(float(owned_el.text)) if owned_el is not None and owned_el.text else 0
        except Exception as e:
            logger.error(f"XML parse error: {e}")

    def run(self):
        global _filings_cache, _last_updated
        logger.info("=" * 50)
        logger.info("Fetching Form 4 filings from SEC EDGAR...")

        filings = self.fetch_atom_feed(limit=60)
        enriched = []
        for i, f in enumerate(filings):
            try:
                enriched.append(self.enrich_from_xml(f))
                if i % 5 == 0:
                    logger.info(f"Enriched {i+1}/{len(filings)}")
            except Exception as e:
                logger.error(f"Error enriching {i}: {e}")
                enriched.append(f)

        # Get unique tickers and fetch stock prices
        tickers = list(set(f['ticker'] for f in enriched if f.get('ticker')))
        logger.info(f"Fetching Yahoo Finance prices for {len(tickers)} tickers...")
        quotes = YahooFinance.get_quotes_batch(tickers[:30])  # limit to 30

        # Add stock prices to filings
        for f in enriched:
            ticker = f.get('ticker', '')
            if ticker in quotes:
                f['stock_price'] = quotes[ticker]['stock_price']
                f['stock_change_pct'] = quotes[ticker]['stock_change_pct']

        # Save to Supabase
        rows_to_save = []
        for f in enriched:
            if f.get('filing_url'):
                rows_to_save.append({
                    'ticker': f.get('ticker', ''),
                    'company_name': f.get('company_name', ''),
                    'owner_name': f.get('owner_name', ''),
                    'owner_type': f.get('owner_type', ''),
                    'transaction_type': f.get('transaction_type', ''),
                    'shares': f.get('shares', 0),
                    'price': f.get('price', 0),
                    'value': f.get('value', 0),
                    'transaction_date': f.get('transaction_date'),
                    'filed_at': f.get('filed_at') or None,
                    'filing_url': f.get('filing_url', ''),
                    'shares_owned_after': f.get('shares_owned_after', 0),
                    'stock_price': f.get('stock_price', 0),
                    'stock_change_pct': f.get('stock_change_pct', 0),
                })

        if rows_to_save:
            self.db.upsert('filings', rows_to_save)

        _filings_cache = enriched
        _last_updated = datetime.now().isoformat()
        logger.info(f"Done — cached {len(_filings_cache)} filings, saved to Supabase")
        return {'filings_found': len(_filings_cache), 'timestamp': _last_updated}


# ─── Flask API ────────────────────────────────────────────────────

def create_app():
    app = Flask(__name__)
    CORS(app)
    scraper = SECScraper()
    db = SupabaseClient()

    @app.route('/api/health')
    def health():
        return jsonify({
            'status': 'ok',
            'timestamp': datetime.now().isoformat(),
            'filings_cached': len(_filings_cache),
            'last_updated': _last_updated,
        })

    @app.route('/api/filings')
    def get_filings():
        limit = request.args.get('limit', 50, type=int)
        tx_type = request.args.get('type', '')
        ticker = request.args.get('ticker', '')
        source = request.args.get('source', 'cache')  # 'cache' or 'db'

        if source == 'db':
            results = db.get_recent('filings', limit=limit)
        else:
            results = list(_filings_cache)

        if tx_type:
            results = [f for f in results if f.get('transaction_type', '').lower() == tx_type.lower()]
        if ticker:
            results = [f for f in results if f.get('ticker', '').upper() == ticker.upper()]

        return jsonify({'filings': results[:limit], 'total': len(results), 'last_updated': _last_updated})

    @app.route('/api/filings/refresh')
    def refresh():
        thread = threading.Thread(target=scraper.run, daemon=True)
        thread.start()
        return jsonify({'status': 'refreshing'})

    @app.route('/api/summary')
    def summary():
        # Use Supabase for accurate historical summary
        all_rows = db.get_summary()
        buys = [r for r in all_rows if r.get('transaction_type') == 'Buy']
        sells = [r for r in all_rows if r.get('transaction_type') == 'Sell']
        total_value = sum(float(r.get('value', 0) or 0) for r in all_rows)

        # Top buys/sells from cache for speed
        cache_buys = sorted([f for f in _filings_cache if f.get('transaction_type') == 'Buy'],
                            key=lambda x: x.get('value', 0), reverse=True)[:5]
        cache_sells = sorted([f for f in _filings_cache if f.get('transaction_type') == 'Sell'],
                             key=lambda x: x.get('value', 0), reverse=True)[:5]

        return jsonify({
            'total_filings': len(all_rows) or len(_filings_cache),
            'buys': len(buys) or len([f for f in _filings_cache if f.get('transaction_type') == 'Buy']),
            'sells': len(sells) or len([f for f in _filings_cache if f.get('transaction_type') == 'Sell']),
            'total_value': total_value,
            'top_buys': cache_buys,
            'top_sells': cache_sells,
            'last_updated': _last_updated,
        })

    @app.route('/api/company/<ticker>')
    def company(ticker):
        # Check Supabase first for historical data
        results = db.get_by_ticker(ticker.upper(), limit=50)
        if not results:
            results = [f for f in _filings_cache if f.get('ticker', '').upper() == ticker.upper()]
        return jsonify({'ticker': ticker.upper(), 'filings': results, 'count': len(results)})

    @app.route('/api/top-stocks')
    def top_stocks():
        counts = {}
        for f in _filings_cache:
            t = f.get('ticker', '')
            if t:
                counts[t] = counts.get(t, 0) + 1
        sorted_t = sorted(counts.items(), key=lambda x: x[1], reverse=True)
        return jsonify({'stocks': [{'ticker': t, 'count': c} for t, c in sorted_t[:30]]})

    @app.route('/api/history')
    def history():
        """Get historical filings from Supabase"""
        limit = request.args.get('limit', 100, type=int)
        ticker = request.args.get('ticker', '')
        if ticker:
            results = db.get_by_ticker(ticker.upper(), limit=limit)
        else:
            results = db.get_recent('filings', limit=limit)
        return jsonify({'filings': results, 'total': len(results)})

    def startup():
        time.sleep(3)
        scraper.run()

    threading.Thread(target=startup, daemon=True).start()
    return app


def run_scheduler():
    scraper = SECScraper()
    schedule.every(30).minutes.do(scraper.run)
    schedule.every().day.at("09:30").do(scraper.run)
    schedule.every().day.at("12:00").do(scraper.run)
    schedule.every().day.at("16:00").do(scraper.run)
    logger.info("Scheduler started")
    scraper.run()
    while True:
        schedule.run_pending()
        time.sleep(60)


# For gunicorn
app = create_app()

if __name__ == '__main__':
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == 'api':
        port = int(os.getenv('PORT', 5000))
        app.run(host='0.0.0.0', port=port, debug=False)
    else:
        run_scheduler()
