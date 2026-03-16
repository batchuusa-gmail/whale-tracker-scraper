#!/usr/bin/env python3
"""
SEC Form 4 Insider Trading Scraper - Real Data Only
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

_filings_cache = []
_last_updated = None


class SECScraper:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(SEC_HEADERS)

    def rate_limit(self):
        time.sleep(SEC_DELAY + random.uniform(0, 0.3))

    def fetch_atom_feed(self, limit=50):
        """Fetch real-time Form 4 filings from EDGAR ATOM feed"""
        results = []
        try:
            url = f"{SEC_BASE_URL}/cgi-bin/browse-edgar?action=getcurrent&type=4&dateb=&owner=include&count={limit}&search_text=&output=atom"
            self.rate_limit()
            resp = self.session.get(url, timeout=20)
            resp.raise_for_status()
            root = ET.fromstring(resp.content)
            ns = {'atom': 'http://www.w3.org/2005/Atom'}
            entries = root.findall('atom:entry', ns)
            logger.info(f"ATOM feed returned {len(entries)} entries")
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
                        'transaction_date': '',
                        'shares_owned_after': 0,
                        'form_type': '4',
                    })
                except Exception as e:
                    logger.error(f"Entry parse error: {e}")
        except Exception as e:
            logger.error(f"ATOM feed error: {e}")
        return results

    def enrich_from_xml(self, filing):
        """Parse the Form 4 XML for transaction details"""
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
            for row in soup.select('table.tableFile tr'):
                cells = row.find_all('td')
                if len(cells) >= 3:
                    desc = cells[1].get_text(strip=True).lower()
                    a = cells[2].find('a') if len(cells) > 2 else None
                    if not a:
                        a = cells[1].find('a')
                    if a and a.get('href', '').endswith('.xml'):
                        xml_link = f"{SEC_BASE_URL}{a['href']}"
                        break
            if not xml_link:
                for a in soup.find_all('a', href=True):
                    h = a['href']
                    if h.endswith('.xml') and ('/Archives/' in h or h.startswith('/')):
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
                filing['transaction_date'] = date_el.text if date_el is not None else filing.get('filed_at', '')
                filing['shares_owned_after'] = int(float(owned_el.text)) if owned_el is not None and owned_el.text else 0
        except Exception as e:
            logger.error(f"XML parse error: {e}")

    def run(self):
        global _filings_cache, _last_updated
        logger.info("Fetching Form 4 filings from SEC EDGAR...")
        filings = self.fetch_atom_feed(limit=60)
        enriched = []
        for i, f in enumerate(filings):
            try:
                enriched.append(self.enrich_from_xml(f))
                if i % 5 == 0:
                    logger.info(f"Enriched {i+1}/{len(filings)}")
            except Exception as e:
                logger.error(f"Error enriching filing {i}: {e}")
                enriched.append(f)
        _filings_cache = enriched
        _last_updated = datetime.now().isoformat()
        logger.info(f"Done — cached {len(_filings_cache)} filings")
        return {'filings_found': len(_filings_cache), 'timestamp': _last_updated}


def create_app():
    app = Flask(__name__)
    CORS(app)
    scraper = SECScraper()

    @app.route('/api/health')
    def health():
        return jsonify({'status': 'ok', 'timestamp': datetime.now().isoformat(),
                        'filings_cached': len(_filings_cache), 'last_updated': _last_updated})

    @app.route('/api/filings')
    def get_filings():
        limit = request.args.get('limit', 50, type=int)
        tx_type = request.args.get('type', '')
        ticker = request.args.get('ticker', '')
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
        return jsonify({'status': 'refreshing', 'message': 'Fetch started in background'})

    @app.route('/api/summary')
    def summary():
        buys = [f for f in _filings_cache if f.get('transaction_type') == 'Buy']
        sells = [f for f in _filings_cache if f.get('transaction_type') == 'Sell']
        total_value = sum(f.get('value', 0) for f in _filings_cache if f.get('value'))
        top_buys = sorted(buys, key=lambda x: x.get('value', 0), reverse=True)[:5]
        top_sells = sorted(sells, key=lambda x: x.get('value', 0), reverse=True)[:5]
        return jsonify({'total_filings': len(_filings_cache), 'buys': len(buys), 'sells': len(sells),
                        'total_value': total_value, 'top_buys': top_buys, 'top_sells': top_sells,
                        'last_updated': _last_updated})

    @app.route('/api/company/<ticker>')
    def company(ticker):
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

    def startup():
        time.sleep(3)
        scraper.run()

    threading.Thread(target=startup, daemon=True).start()
    return app


def run_scheduler():
    scraper = SECScraper()
    schedule.every(30).minutes.do(scraper.run)
    schedule.every().day.at("09:30").do(scraper.run)
    schedule.every().day.at("16:00").do(scraper.run)
    logger.info("Scheduler started")
    scraper.run()
    while True:
        schedule.run_pending()
        time.sleep(60)


if __name__ == '__main__':
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == 'api':
        app = create_app()
        port = int(os.getenv('PORT', 5000))
        logger.info(f"Starting API on port {port}")
        app.run(host='0.0.0.0', port=port, debug=False)
    else:
        run_scheduler()


# Module-level app for gunicorn: gunicorn --bind 0.0.0.0:$PORT main:app
app = create_app()
