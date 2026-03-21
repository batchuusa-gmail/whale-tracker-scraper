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
    'Prefer': 'resolution=merge-duplicates,return=minimal',
}

_filings_cache = []
_last_updated = None

# ─── Historical Pull Globals ──────────────────────────────────────

MAJOR_TICKERS = [
    # Big Tech
    'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META', 'NVDA', 'TSLA', 'NFLX',
    'ORCL', 'CRM', 'ADBE', 'INTC', 'AMD', 'QCOM', 'AVGO', 'TXN',
    'MU', 'AMAT', 'LRCX', 'KLAC', 'SNOW', 'PLTR', 'COIN', 'UBER',
    'ABNB', 'DASH', 'RBLX', 'HOOD', 'SOFI', 'RIVN',
    # Finance
    'JPM', 'BAC', 'WFC', 'GS', 'MS', 'C', 'BLK', 'SCHW', 'V', 'MA',
    'PYPL', 'AXP', 'USB', 'PNC', 'TFC',
    # Healthcare / Biotech
    'JNJ', 'PFE', 'MRNA', 'LLY', 'ABBV', 'MRK', 'BMY', 'AMGN',
    'GILD', 'REGN', 'VRTX', 'BIIB', 'ISRG',
    # Energy
    'XOM', 'CVX', 'COP', 'OXY', 'SLB', 'PSX', 'VLO',
    # Consumer / Retail
    'WMT', 'COST', 'HD', 'TGT', 'NKE', 'MCD', 'SBUX', 'DIS', 'CMCSA',
    # Industrial / Defense
    'BA', 'CAT', 'GE', 'HON', 'RTX', 'LMT', 'NOC', 'MMM',
    # EV / Auto
    'F', 'GM',
]

_history_pull_status = {
    'running': False,
    'current_ticker': '',
    'tickers_done': 0,
    'tickers_total': 0,
    'filings_saved': 0,
    'started_at': None,
    'completed_at': None,
    'error': None,
}


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

    def fetch_atom_feed(self, limit=100):
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

            # Step 1: fetch the index page
            self.rate_limit()
            resp = self.session.get(filing_url, timeout=15)
            if resp.status_code != 200:
                return filing

            soup = BeautifulSoup(resp.content, 'html.parser')

            # Step 2: find the XML document link from the filing index table
            # SEC index pages have a table with document type, description, and link
            xml_url = None

            # Look for rows in the document table
            for row in soup.find_all('tr'):
                cells = row.find_all('td')
                if len(cells) >= 3:
                    # Check if this row contains an XML file for Form 4
                    row_text = row.get_text().lower()
                    for cell in cells:
                        a = cell.find('a', href=True)
                        if a:
                            href = a['href']
                            # Find .xml that is NOT the stylesheet (xsl)
                            if (href.endswith('.xml') and
                                'xsl' not in href.lower() and
                                'R9999' not in href):
                                xml_url = f"{SEC_BASE_URL}{href}" if href.startswith('/') else href
                                break
                    if xml_url:
                        break

            # Fallback: find any .xml link that looks like form 4 data
            if not xml_url:
                for a in soup.find_all('a', href=True):
                    href = a['href']
                    if (href.endswith('.xml') and
                        '/Archives/edgar/' in href and
                        'xsl' not in href.lower()):
                        xml_url = f"{SEC_BASE_URL}{href}" if href.startswith('/') else href
                        break

            # Step 3: fetch the actual XML
            if xml_url:
                self.rate_limit()
                xr = self.session.get(xml_url, timeout=15)
                if xr.status_code == 200:
                    # Verify it's actual Form 4 XML not HTML
                    if '<ownershipDocument' in xr.text or '<XML>' in xr.text:
                        xml_text = xr.text
                        # Handle wrapped XML (inside <XML> tags)
                        if '<XML>' in xml_text:
                            start = xml_text.find('<XML>') + 5
                            end = xml_text.find('</XML>')
                            if end > start:
                                xml_text = xml_text[start:end].strip()
                        self._parse_xml_into(filing, xml_text)
                    else:
                        logger.warning(f"Got HTML instead of XML from {xml_url}")
            else:
                logger.warning(f"No XML link found in index: {filing_url}")

        except Exception as e:
            logger.error(f"Enrich error: {e}")
        return filing

    def _parse_xml_into(self, filing, xml_text):
        try:
            # Try to parse XML, use lxml recovery if needed
            root = None
            try:
                root = ET.fromstring(xml_text)
            except ET.ParseError:
                try:
                    from lxml import etree as lxml_et
                    parser = lxml_et.XMLParser(recover=True)
                    lxml_root = lxml_et.fromstring(xml_text.encode(), parser)
                    xml_text = lxml_et.tostring(lxml_root).decode()
                    root = ET.fromstring(xml_text)
                except Exception as ex:
                    logger.error(f"XML recovery failed: {ex}")
                    return

            if root is None:
                return

            def ft(path):
                # Try with and without namespace
                el = root.find(path)
                if el is None:
                    # Try finding by tag name only
                    tag = path.split('/')[-1]
                    for elem in root.iter():
                        if elem.tag.lower().endswith(tag.lower()):
                            return elem.text.strip() if elem.text else ''
                return el.text.strip() if el is not None and el.text else ''

            # Always extract from XML — don't rely on ATOM title
            ticker = ft('.//issuerTradingSymbol') or ft('issuerTradingSymbol')
            issuer_name = ft('.//issuerName') or ft('issuerName')
            owner_name = ft('.//rptOwnerName') or ft('rptOwnerName')

            if ticker:
                filing['ticker'] = ticker.upper().strip()
            if issuer_name:
                filing['company_name'] = issuer_name.strip()
            if owner_name:
                filing['owner_name'] = owner_name.strip()

            logger.info(f"Parsed: ticker={filing.get('ticker')} owner={filing.get('owner_name')} company={filing.get('company_name')}")

            # Owner relationship
            rel = root.find('.//reportingOwnerRelationship')
            if rel is None:
                for elem in root.iter():
                    if 'relationship' in elem.tag.lower():
                        rel = elem
                        break

            if rel is not None:
                is_officer = rel.findtext('isOfficer') or rel.findtext('.//isOfficer') or ''
                is_director = rel.findtext('isDirector') or rel.findtext('.//isDirector') or ''
                is_ten_pct = rel.findtext('isTenPercentOwner') or rel.findtext('.//isTenPercentOwner') or ''
                officer_title = rel.findtext('officerTitle') or rel.findtext('.//officerTitle') or ''
                if is_officer.strip() == '1':
                    filing['owner_type'] = officer_title or 'Officer'
                elif is_director.strip() == '1':
                    filing['owner_type'] = 'Director'
                elif is_ten_pct.strip() == '1':
                    filing['owner_type'] = '10% Owner'

            # Find all elements and build a flat dict for easy access
            all_elements = {}
            for elem in root.iter():
                tag = elem.tag.lower().strip()
                if elem.text and elem.text.strip():
                    all_elements[tag] = elem.text.strip()

            # Extract ticker - try multiple variations
            ticker_keys = ['issuertradingsymbol', 'tradingsymbol', 'ticker']
            for key in ticker_keys:
                if key in all_elements and all_elements[key]:
                    filing['ticker'] = all_elements[key].upper().strip()
                    break

            # Extract issuer name
            name_keys = ['issuername', 'companyname', 'entityname']
            for key in name_keys:
                if key in all_elements and all_elements[key]:
                    filing['company_name'] = all_elements[key].strip()
                    break

            # Extract owner name
            owner_keys = ['rptownername', 'ownername', 'reportingownername']
            for key in owner_keys:
                if key in all_elements and all_elements[key]:
                    filing['owner_name'] = all_elements[key].strip()
                    break

            # Owner type
            if all_elements.get('isofficer') == '1':
                filing['owner_type'] = all_elements.get('officertitle', 'Officer')
            elif all_elements.get('isdirector') == '1':
                filing['owner_type'] = 'Director'
            elif all_elements.get('istenpercentowner') == '1':
                filing['owner_type'] = '10% Owner'

            logger.info(f"Parsed: ticker={filing.get('ticker')} owner={filing.get('owner_name')} company={filing.get('company_name')}")

            # Find transactions - check both nonDerivative and derivative
            txs = root.findall('.//nonDerivativeTransaction')
            if not txs:
                txs = root.findall('.//derivativeTransaction')
            if not txs:
                # Try case-insensitive search
                txs = [e for e in root.iter() if 'transaction' in e.tag.lower()
                       and e.tag.lower() not in ('transactioncode', 'transactiondate',
                       'transactionshares', 'transactionpricepershare', 'transactiontimeliness')]

            if txs:
                tx = txs[0]
                tx_elements = {}
                for elem in tx.iter():
                    tag = elem.tag.lower().strip()
                    if elem.text and elem.text.strip():
                        tx_elements[tag] = elem.text.strip()
                    # Also check value sub-elements
                    val = elem.find('value')
                    if val is not None and val.text and val.text.strip():
                        tx_elements[tag + '_value'] = val.text.strip()

                code = tx_elements.get('transactioncode', '')
                shares = 0
                price = 0
                tx_date = None
                owned = 0

                try: shares = float(tx_elements.get('transactionshares_value') or tx_elements.get('transactionshares', 0))
                except: pass
                try: price = float(tx_elements.get('transactionpricepershare_value') or tx_elements.get('transactionpricepershare', 0))
                except: pass
                try: tx_date = tx_elements.get('transactiondate_value') or tx_elements.get('transactiondate')
                except: pass
                try: owned = float(tx_elements.get('sharesownedfollowingtransaction_value') or tx_elements.get('sharesownedfollowingtransaction', 0))
                except: pass

                # For derivatives, use underlying shares if available
                if shares == 0:
                    try: shares = float(tx_elements.get('underlyingsecuritiestransacted_value', 0))
                    except: pass

                filing['transaction_type'] = 'Buy' if code in ('P', 'A', 'M') else 'Sell' if code in ('S', 'D', 'F', 'X') else code
                filing['shares'] = int(shares)
                filing['price'] = round(price, 2)
                filing['value'] = round(shares * price, 2)
                filing['transaction_date'] = tx_date
                filing['shares_owned_after'] = int(owned)
                logger.info(f"Transaction: code={code} type={filing['transaction_type']} shares={filing['shares']} price={filing['price']} value={filing['value']}")
            else:
                logger.warning(f"No transactions found for {filing.get('ticker', '?')}")

        except Exception as e:
            logger.error(f"XML parse error: {e}", exc_info=True)

    def run(self):
        global _filings_cache, _last_updated
        logger.info("=" * 50)
        logger.info("Fetching Form 4 filings from SEC EDGAR...")

        filings = self.fetch_atom_feed(limit=100)
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

        # Add stock prices to filings + calculate value for RSU/option exercises
        for f in enriched:
            ticker = f.get('ticker', '')
            if ticker in quotes:
                f['stock_price'] = quotes[ticker]['stock_price']
                f['stock_change_pct'] = quotes[ticker]['stock_change_pct']
                # Fix RSU/option exercises with no price (code M, A, F)
                # Use current stock price to estimate value
                if f.get('value', 0) == 0 and f.get('shares', 0) > 0:
                    stock_price = quotes[ticker]['stock_price']
                    if stock_price > 0:
                        f['price'] = stock_price
                        f['value'] = round(f['shares'] * stock_price, 2)
                        logger.info(f"Calculated RSU value for {ticker}: {f['shares']} shares × ${stock_price} = ${f['value']}")

        # Deduplicate by ticker + owner + transaction_type + date
        seen = set()
        deduped = []
        for f in enriched:
            ticker = f.get('ticker', '')
            owner = f.get('owner_name', '')
            tx_type = f.get('transaction_type', '')
            date = f.get('transaction_date', f.get('filed_at', ''))
            shares = f.get('shares', 0)
            # Use ticker+owner+shares as key to avoid true duplicates
            # but keep different insiders from same company
            key = f"{ticker}_{owner}_{shares}_{tx_type}"
            if key not in seen:
                seen.add(key)
                deduped.append(f)

        logger.info(f"Deduplicated: {len(enriched)} -> {len(deduped)} filings")

        # Save to Supabase
        rows_to_save = []
        for f in deduped:
            if f.get('filing_url') and f.get('ticker'):
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

        _filings_cache = deduped
        _last_updated = datetime.now().isoformat()
        logger.info(f"Done — cached {len(_filings_cache)} filings, saved to Supabase")
        return {'filings_found': len(_filings_cache), 'timestamp': _last_updated}

    # ── Historical pull helpers ──────────────────────────────────

    def fetch_ticker_atom_pages(self, ticker, days_back=1095):
        """Fetch Form 4 ATOM feed pages for a single ticker, up to days_back days back."""
        cutoff = datetime.now() - timedelta(days=days_back)
        results = []
        start = 0
        count = 40

        while True:
            url = (
                f"{SEC_BASE_URL}/cgi-bin/browse-edgar"
                f"?action=getcompany&CIK={ticker}&type=4&dateb=&owner=include"
                f"&count={count}&start={start}&search_text=&output=atom"
            )
            self.rate_limit()
            try:
                resp = self.session.get(url, timeout=20)
                resp.raise_for_status()
                root = ET.fromstring(resp.content)
                ns = {'atom': 'http://www.w3.org/2005/Atom'}
                entries = root.findall('atom:entry', ns)
                if not entries:
                    break

                stop = False
                for entry in entries:
                    try:
                        title = entry.findtext('atom:title', '', ns)
                        updated = entry.findtext('atom:updated', '', ns)
                        link_el = entry.find('atom:link', ns)
                        filing_url = link_el.get('href', '') if link_el is not None else ''

                        if updated:
                            entry_date = datetime.fromisoformat(updated[:10])
                            if entry_date < cutoff:
                                stop = True
                                break

                        ticker_match = re.search(r'\(([A-Z]{1,5})\)', title)
                        company_match = re.search(r'4\s*-\s*(.+?)\s*\(', title)
                        entry_ticker = ticker_match.group(1) if ticker_match else ticker.upper()
                        company = company_match.group(1).strip() if company_match else title

                        results.append({
                            'ticker': entry_ticker,
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
                        logger.error(f"Historical entry parse error ({ticker}): {e}")

                logger.info(f"{ticker}: fetched {len(results)} filings so far (page start={start})")

                if stop or len(entries) < count:
                    break
                start += count

            except Exception as e:
                logger.error(f"ATOM page error for {ticker} start={start}: {e}")
                break

        return results

    def run_historical_pull(self, tickers=None):
        """Pull 3 years of Form 4 data for each ticker and upsert to Supabase."""
        global _history_pull_status

        if tickers is None:
            tickers = MAJOR_TICKERS

        _history_pull_status.update({
            'running': True,
            'current_ticker': '',
            'tickers_done': 0,
            'tickers_total': len(tickers),
            'filings_saved': 0,
            'started_at': datetime.now().isoformat(),
            'completed_at': None,
            'error': None,
        })

        total_saved = 0
        try:
            for i, ticker in enumerate(tickers):
                _history_pull_status['current_ticker'] = ticker
                logger.info(f"[history] {ticker} ({i+1}/{len(tickers)})")
                try:
                    filings = self.fetch_ticker_atom_pages(ticker, days_back=1095)
                    logger.info(f"[history] {ticker}: enriching {len(filings)} filings")

                    enriched = []
                    for j, f in enumerate(filings):
                        try:
                            enriched.append(self.enrich_from_xml(f))
                        except Exception as e:
                            logger.error(f"[history] enrich error {ticker}[{j}]: {e}")
                            enriched.append(f)

                    # One price fetch per ticker
                    quote = YahooFinance.get_quote(ticker)
                    for f in enriched:
                        if not f.get('stock_price'):
                            f['stock_price'] = quote.get('stock_price', 0.0)
                            f['stock_change_pct'] = quote.get('stock_change_pct', 0.0)
                        if f.get('value', 0) == 0 and f.get('shares', 0) > 0 and quote.get('stock_price', 0) > 0:
                            f['price'] = quote['stock_price']
                            f['value'] = round(f['shares'] * quote['stock_price'], 2)

                    rows = [
                        {
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
                        }
                        for f in enriched
                        if f.get('filing_url') and f.get('ticker')
                    ]

                    # Upsert in batches of 20 to stay within Supabase limits
                    for k in range(0, len(rows), 20):
                        self.db.upsert('filings', rows[k:k + 20])
                    total_saved += len(rows)
                    _history_pull_status['filings_saved'] = total_saved
                    logger.info(f"[history] {ticker}: saved {len(rows)} rows (total={total_saved})")

                except Exception as e:
                    logger.error(f"[history] error processing {ticker}: {e}")

                _history_pull_status['tickers_done'] = i + 1

        except Exception as e:
            _history_pull_status['error'] = str(e)
            logger.error(f"[history] fatal error: {e}")
        finally:
            _history_pull_status['running'] = False
            _history_pull_status['completed_at'] = datetime.now().isoformat()
            logger.info(f"[history] complete — {total_saved} total filings saved")


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

    @app.route('/api/chart/<ticker>')
    def chart(ticker):
        """Get Yahoo Finance chart data for a ticker"""
        try:
            period = request.args.get('period', '1mo')
            interval = request.args.get('interval', '1d')
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval={interval}&range={period}"
            headers = {'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X)'}
            resp = requests.get(url, headers=headers, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                result = data.get('chart', {}).get('result', [{}])[0]
                timestamps = result.get('timestamp', [])
                closes = result.get('indicators', {}).get('quote', [{}])[0].get('close', [])
                meta = result.get('meta', {})
                points = [{'t': t, 'c': round(c, 2)} for t, c in zip(timestamps, closes) if c is not None]
                return jsonify({
                    'ticker': ticker.upper(),
                    'points': points,
                    'current': meta.get('regularMarketPrice', 0),
                    'prev_close': meta.get('chartPreviousClose', 0),
                })
        except Exception as e:
            logger.error(f"Chart error for {ticker}: {e}")
        return jsonify({'ticker': ticker.upper(), 'points': [], 'current': 0, 'prev_close': 0})

    @app.route('/api/filings/history-pull', methods=['POST'])
    def history_pull():
        """Trigger a 3-year historical pull for MAJOR_TICKERS (or a custom list).
        POST body (optional JSON): {"tickers": ["AAPL", "TSLA"]}
        """
        if _history_pull_status.get('running'):
            return jsonify({'status': 'already_running', 'progress': _history_pull_status}), 409
        tickers = None
        if request.is_json and request.json:
            tickers = request.json.get('tickers') or None
        thread = threading.Thread(
            target=scraper.run_historical_pull,
            args=(tickers,),
            daemon=True,
        )
        thread.start()
        return jsonify({
            'status': 'started',
            'tickers': tickers or MAJOR_TICKERS,
            'ticker_count': len(tickers or MAJOR_TICKERS),
            'message': 'Historical pull running in background. Poll /api/filings/history-status for progress.',
        })

    @app.route('/api/filings/history-status')
    def history_status():
        """Return current progress of the background historical pull."""
        return jsonify(_history_pull_status)

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
