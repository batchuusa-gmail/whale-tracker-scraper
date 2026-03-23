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
    # Technology — Mega Cap
    'AAPL', 'MSFT', 'NVDA', 'GOOGL', 'GOOG', 'AMZN', 'META', 'TSLA',
    'AVGO', 'ORCL', 'CRM', 'ADBE', 'AMD', 'QCOM', 'TXN', 'INTC',
    'AMAT', 'LRCX', 'KLAC', 'MU', 'MRVL', 'SNPS', 'CDNS', 'NFLX',
    # Technology — Software / Cloud
    'NOW', 'PANW', 'FTNT', 'CRWD', 'ZS', 'DDOG', 'NET', 'OKTA',
    'TEAM', 'HUBS', 'PAYC', 'ANSS', 'GDDY', 'IT', 'ACN', 'CTSH',
    'EPAM', 'IBM', 'HPQ', 'HPE', 'DELL', 'STX', 'WDC', 'NTAP',
    'CSCO', 'ANET', 'JNPR', 'KEYS', 'TER', 'CDW', 'APP', 'TWLO',
    'ZM', 'DOCU', 'SHOP', 'PCTY', 'CDAY', 'WEX', 'TOST', 'FOUR',
    'GPN', 'FIS', 'FISV', 'JKHY', 'FIVN', 'NICE', 'VEEV',
    # Technology — Fintech / Crypto / New Tech
    'COIN', 'HOOD', 'SOFI', 'PLTR', 'SNOW', 'UBER', 'LYFT',
    'DASH', 'RBLX', 'NIO', 'XPEV', 'LI',
    # Finance — Banks
    'JPM', 'BAC', 'WFC', 'GS', 'MS', 'C', 'USB', 'PNC', 'TFC',
    'COF', 'DFS', 'SYF', 'ALLY', 'FITB', 'HBAN', 'RF', 'CFG',
    'KEY', 'MTB', 'ZION', 'CMA', 'FNB', 'BOKF', 'EWBC', 'OZK',
    'NYCB', 'WAL', 'TCBI', 'CBSH', 'UMBF', 'WTFC', 'IBCP', 'SBCF',
    # Finance — Asset Mgmt / Insurance / Exchanges
    'BLK', 'SCHW', 'V', 'MA', 'PYPL', 'AXP', 'ICE', 'CME', 'CBOE',
    'NDAQ', 'SPGI', 'MCO', 'MSCI', 'FDS', 'BR', 'BEN', 'IVZ',
    'TROW', 'BX', 'KKR', 'APO', 'ARES', 'CG', 'STT', 'BK', 'NTRS',
    'MET', 'PRU', 'AFL', 'ALL', 'CB', 'TRV', 'HIG', 'AIG', 'PGR',
    'LNC', 'UNM', 'GL', 'RGA', 'EQH', 'VOYA', 'FG', 'CRBG',
    # Healthcare — Pharma / Biotech
    'JNJ', 'PFE', 'MRNA', 'LLY', 'ABBV', 'MRK', 'BMY', 'AMGN',
    'GILD', 'REGN', 'VRTX', 'BIIB', 'ISRG', 'ILMN', 'PACB',
    'BMRN', 'ALNY', 'INCY', 'JAZZ', 'HALO', 'IONS', 'RARE',
    'AKRO', 'ARWR', 'BEAM', 'EDIT', 'NTLA', 'CRSP', 'SGEN',
    'EXAS', 'HOLX', 'DXCM', 'PODD', 'ALGN', 'EW',
    # Healthcare — Equipment / Devices
    'ABT', 'TMO', 'DHR', 'A', 'WAT', 'BIO', 'IDXX', 'IQV', 'CRL',
    'CTLT', 'BSX', 'MDT', 'SYK', 'ZBH', 'BAX', 'BDX', 'RMD',
    'TNDM', 'SWAV', 'NVCR', 'INSP', 'NVST', 'LMAT',
    # Healthcare — Managed Care / Services
    'UNH', 'CVS', 'CI', 'HUM', 'MOH', 'CNC', 'ELV', 'HCA', 'THC',
    'UHS', 'ENSG', 'HQY', 'ACCD', 'DOCS', 'OSCR', 'WBA', 'MCK',
    'ABC', 'CAH',
    # Energy — Oil & Gas
    'XOM', 'CVX', 'COP', 'OXY', 'SLB', 'PSX', 'VLO', 'MPC',
    'HES', 'DVN', 'FANG', 'EOG', 'PXD', 'APA', 'HAL', 'BKR',
    'NOV', 'CTRA', 'EQT', 'AR', 'RRC', 'CNX', 'CHK', 'MRO',
    'NFG', 'SM', 'MTDR', 'CIVI', 'VTLE', 'PR', 'GPOR',
    # Energy — Renewables / Utilities
    'NEE', 'DUK', 'SO', 'D', 'EXC', 'ED', 'XEL', 'PCG', 'PPL',
    'FE', 'ETR', 'WEC', 'CMS', 'AEE', 'NI', 'LNT', 'EVRG',
    'PNW', 'ATO', 'NWE', 'OTTR', 'PNM', 'POR', 'SRE', 'AES',
    'ENPH', 'SEDG', 'FSLR', 'RUN', 'ARRY', 'NOVA', 'SPWR',
    # Consumer Discretionary
    'WMT', 'COST', 'HD', 'TGT', 'NKE', 'MCD', 'SBUX', 'DIS',
    'CMCSA', 'LOW', 'TJX', 'ROST', 'DG', 'DLTR', 'ULTA', 'BBY',
    'KSS', 'M', 'JWN', 'GPS', 'LULU', 'DECK', 'SKX', 'CROX',
    'ONON', 'BIRK', 'W', 'CHWY', 'ETSY', 'PTON',
    # Consumer Staples
    'KO', 'PEP', 'MDLZ', 'HSY', 'CPB', 'GIS', 'K', 'SJM', 'HRL',
    'MKC', 'CAG', 'TSN', 'PG', 'CL', 'KMB', 'CHD', 'ENR', 'EL',
    'COTY', 'KVUE', 'SPB', 'CLX', 'KHC', 'PM', 'MO', 'BTI',
    # Travel / Hospitality / Entertainment
    'RCL', 'CCL', 'NCLH', 'MAR', 'HLT', 'WH', 'H', 'IHG', 'WYN',
    'ABNB', 'BKNG', 'EXPE', 'TRIP', 'PCLN', 'UAL', 'DAL', 'AAL',
    'LUV', 'ALK', 'HA', 'JBLU', 'SAVE',
    # Auto / EV
    'F', 'GM', 'RIVN', 'LCID', 'FSR', 'FFIE', 'GOEV',
    'NKLA', 'PTRA', 'WKC', 'MGA', 'BWA', 'TEN', 'ALSN', 'GT',
    # Industrial / Machinery
    'BA', 'CAT', 'GE', 'HON', 'RTX', 'MMM', 'EMR', 'ETN', 'PH',
    'ROK', 'IR', 'AME', 'ROP', 'VRSK', 'XYL', 'GNRC', 'AOS',
    'CMI', 'PCAR', 'DE', 'AGCO', 'CNH', 'URI', 'FAST', 'GWW',
    'MSC', 'KMPR', 'EPAC', 'GHM', 'ACCO',
    # Defense
    'LMT', 'NOC', 'GD', 'LHX', 'HII', 'AXON', 'TDG', 'HEI',
    'CW', 'KTOS', 'LDOS', 'SAIC', 'CACI', 'BWXT', 'DRS', 'OSK',
    'MOOG', 'TDY', 'FLIR', 'VSE', 'DWT',
    # Materials / Mining / Chemicals
    'LIN', 'APD', 'DD', 'DOW', 'EMN', 'PPG', 'SHW', 'RPM',
    'FCX', 'NEM', 'GOLD', 'AEM', 'WPM', 'KGC', 'PAAS',
    'AA', 'CENX', 'KALU', 'ARNC', 'HBM', 'TECK', 'RIO', 'BHP',
    'CLF', 'STLD', 'NUE', 'RS', 'CMC', 'ZEUS', 'MTX',
    # Real Estate
    'PLD', 'AMT', 'CCI', 'EQIX', 'DLR', 'SBAC', 'SPG', 'O',
    'VICI', 'CBRE', 'JLL', 'PSA', 'EXR', 'CUBE', 'LSI',
    'MAA', 'UDR', 'CPT', 'NVR', 'TOL', 'PHM', 'DHI', 'LEN',
    'KBH', 'MDC', 'TMHC', 'TPH', 'BZH', 'SKY', 'CVCO',
    # Communication Services
    'T', 'VZ', 'TMUS', 'CHTR', 'WBD',
    'PARA', 'FOX', 'FOXA', 'NYT', 'NWS', 'NWSA', 'LUMN',
    'IRDM', 'VSAT', 'GSAT', 'SHEN', 'CCOI', 'BAND', 'OOMA',
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

        if source == 'db' or not _filings_cache:
            # Fall back to Supabase when cache is empty (e.g. fresh deploy)
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
        cutoff_7d = (datetime.now() - timedelta(days=7)).date().isoformat()
        today_str = datetime.now().date().isoformat()

        # Pull recent filings from Supabase or cache
        source_rows = db.get_recent('filings', limit=500) if not _filings_cache else list(_filings_cache)

        # 7-day filtered rows
        def _in_7d(f):
            tx = f.get('transaction_date') or f.get('filed_at') or ''
            if tx:
                tx = str(tx)[:10]
            return tx >= cutoff_7d

        rows_7d = [f for f in source_rows if _in_7d(f)]
        buys_7d = [f for f in rows_7d if f.get('transaction_type') == 'Buy']
        sells_7d = [f for f in rows_7d if f.get('transaction_type') == 'Sell']
        total_value = sum(float(f.get('value', 0) or 0) for f in rows_7d)

        # Top buys/sells — deduplicated by ticker, sorted by value desc
        def _top_deduped(items, limit=10):
            sorted_all = sorted(items, key=lambda x: float(x.get('value', 0) or 0), reverse=True)
            seen, result = set(), []
            for f in sorted_all:
                t = f.get('ticker', '')
                if t and t not in seen:
                    seen.add(t)
                    result.append(f)
                if len(result) >= limit:
                    break
            return result

        top_buys = _top_deduped(buys_7d, 10)
        top_sells = _top_deduped(sells_7d, 10)

        # Today's filings
        def _is_today(f):
            filed = f.get('filed_at') or f.get('transaction_date') or ''
            return str(filed)[:10] == today_str

        today_filings = sorted(
            [f for f in source_rows if _is_today(f)],
            key=lambda x: float(x.get('value', 0) or 0), reverse=True
        )[:20]

        return jsonify({
            'total_filings': len(rows_7d),
            'buys': len(buys_7d),
            'sells': len(sells_7d),
            'total_value': total_value,
            'top_buys': top_buys,
            'top_sells': top_sells,
            'today': today_filings,
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

    @app.route('/api/tickers')
    def get_tickers():
        """Return the current MAJOR_TICKERS list."""
        return jsonify({'tickers': MAJOR_TICKERS, 'count': len(MAJOR_TICKERS)})

    @app.route('/api/tickers', methods=['POST'])
    def add_ticker():
        """Add a ticker to MAJOR_TICKERS for this session."""
        global MAJOR_TICKERS
        data = request.get_json() or {}
        ticker = data.get('ticker', '').upper().strip()
        if not ticker or len(ticker) > 10:
            return jsonify({'error': 'Invalid ticker'}), 400
        if ticker not in MAJOR_TICKERS:
            MAJOR_TICKERS.append(ticker)
            logger.info(f"Added ticker {ticker} to MAJOR_TICKERS")
        return jsonify({'tickers': MAJOR_TICKERS, 'count': len(MAJOR_TICKERS)})

    @app.route('/api/tickers/<ticker>', methods=['DELETE'])
    def remove_ticker(ticker):
        """Remove a ticker from MAJOR_TICKERS for this session."""
        global MAJOR_TICKERS
        t = ticker.upper().strip()
        if t in MAJOR_TICKERS:
            MAJOR_TICKERS.remove(t)
            logger.info(f"Removed ticker {t} from MAJOR_TICKERS")
        return jsonify({'tickers': MAJOR_TICKERS, 'count': len(MAJOR_TICKERS)})

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

    @app.route('/api/options/flow')
    def options_flow():
        _mock_flow = [
            {'ticker':'SPY','type':'PUT','strike':490.0,'expiry':'2026-04-17','volume':84200,'openInterest':31000,'premium':28140000.0,'unusual':True},
            {'ticker':'NVDA','type':'CALL','strike':950.0,'expiry':'2026-04-17','volume':52300,'openInterest':18500,'premium':18305000.0,'unusual':True},
            {'ticker':'AAPL','type':'CALL','strike':210.0,'expiry':'2026-03-28','volume':38100,'openInterest':9200,'premium':9144000.0,'unusual':True},
            {'ticker':'TSLA','type':'PUT','strike':175.0,'expiry':'2026-04-17','volume':29800,'openInterest':14600,'premium':7152000.0,'unusual':False},
            {'ticker':'QQQ','type':'PUT','strike':450.0,'expiry':'2026-04-17','volume':26500,'openInterest':22000,'premium':6360000.0,'unusual':False},
            {'ticker':'META','type':'CALL','strike':560.0,'expiry':'2026-05-16','volume':18900,'openInterest':6100,'premium':5481000.0,'unusual':True},
            {'ticker':'MSFT','type':'CALL','strike':430.0,'expiry':'2026-05-16','volume':14200,'openInterest':7800,'premium':4970000.0,'unusual':False},
            {'ticker':'AMD','type':'CALL','strike':165.0,'expiry':'2026-03-28','volume':33600,'openInterest':8200,'premium':4704000.0,'unusual':True},
            {'ticker':'AMZN','type':'CALL','strike':195.0,'expiry':'2026-04-17','volume':12800,'openInterest':5300,'premium':3840000.0,'unusual':False},
            {'ticker':'GS','type':'CALL','strike':540.0,'expiry':'2026-04-17','volume':7400,'openInterest':1900,'premium':3182000.0,'unusual':True},
            {'ticker':'JPM','type':'PUT','strike':220.0,'expiry':'2026-04-17','volume':9200,'openInterest':11400,'premium':2208000.0,'unusual':False},
            {'ticker':'GOOGL','type':'CALL','strike':175.0,'expiry':'2026-05-16','volume':8100,'openInterest':4200,'premium':2025000.0,'unusual':False},
            {'ticker':'PLTR','type':'CALL','strike':35.0,'expiry':'2026-03-28','volume':41000,'openInterest':9600,'premium':1640000.0,'unusual':True},
            {'ticker':'BAC','type':'PUT','strike':38.0,'expiry':'2026-04-17','volume':22000,'openInterest':8800,'premium':1320000.0,'unusual':False},
            {'ticker':'XOM','type':'CALL','strike':115.0,'expiry':'2026-04-17','volume':6800,'openInterest':3100,'premium':952000.0,'unusual':False},
        ]
        try:
            import yfinance as yf
            rows = db.get_recent('filings', limit=50)
            tickers = list(set([f['ticker'] for f in rows if f.get('ticker')]))[:15]
            flow = []
            for ticker in tickers:
                try:
                    t = yf.Ticker(ticker)
                    exps = t.options
                    if not exps:
                        continue
                    for exp in exps[:2]:
                        try:
                            chain = t.option_chain(exp)
                            avg_call_vol = chain.calls['volume'].mean() if len(chain.calls) > 0 else 1
                            avg_put_vol = chain.puts['volume'].mean() if len(chain.puts) > 0 else 1
                            for _, row in chain.calls.iterrows():
                                vol = int(row.get('volume') or 0)
                                if vol > 100:
                                    flow.append({
                                        'ticker': ticker, 'type': 'CALL',
                                        'strike': float(row.get('strike', 0)), 'expiry': exp,
                                        'volume': vol,
                                        'openInterest': int(row.get('openInterest') or 0),
                                        'premium': round(float(row.get('lastPrice') or 0) * vol * 100, 2),
                                        'unusual': avg_call_vol > 0 and vol > avg_call_vol * 3,
                                    })
                            for _, row in chain.puts.iterrows():
                                vol = int(row.get('volume') or 0)
                                if vol > 100:
                                    flow.append({
                                        'ticker': ticker, 'type': 'PUT',
                                        'strike': float(row.get('strike', 0)), 'expiry': exp,
                                        'volume': vol,
                                        'openInterest': int(row.get('openInterest') or 0),
                                        'premium': round(float(row.get('lastPrice') or 0) * vol * 100, 2),
                                        'unusual': avg_put_vol > 0 and vol > avg_put_vol * 3,
                                    })
                        except Exception as e:
                            logger.error(f"Options chain error {ticker} {exp}: {e}")
                except Exception as e:
                    logger.error(f"Options ticker error {ticker}: {e}")
            if flow:
                flow.sort(key=lambda x: x['premium'], reverse=True)
                return jsonify(flow[:50])
            # yfinance blocked on this IP — return curated mock data
            logger.info("Options: yfinance returned no data, serving mock flow")
            return jsonify(_mock_flow)
        except Exception as e:
            logger.error(f"Options flow error: {e}")
            return jsonify(_mock_flow)

    @app.route('/api/shorts/<ticker>')
    def short_interest(ticker):
        try:
            import yfinance as yf
            t = yf.Ticker(ticker.upper())
            info = t.info
            short_pct = info.get('shortPercentOfFloat', 0) or 0
            return jsonify({
                'ticker': ticker.upper(),
                'short_ratio': round(float(info.get('shortRatio', 0) or 0), 2),
                'short_percent': round(float(short_pct) * 100, 2),
                'shares_short': int(info.get('sharesShort', 0) or 0),
                'float_shares': int(info.get('floatShares', 0) or 0),
            })
        except Exception as e:
            logger.error(f"Short interest error {ticker}: {e}")
            return jsonify({'ticker': ticker.upper(), 'error': str(e), 'short_percent': 0, 'short_ratio': 0, 'shares_short': 0, 'float_shares': 0})

    @app.route('/api/sectors')
    def sectors():
        SECTORS = {
            'AAPL':'Technology','MSFT':'Technology','GOOGL':'Technology','NVDA':'Technology',
            'META':'Technology','AMZN':'Consumer','WMT':'Consumer','TGT':'Consumer',
            'NFLX':'Technology','CRM':'Technology','ORCL':'Technology','ADBE':'Technology',
            'JPM':'Finance','BAC':'Finance','GS':'Finance','MS':'Finance','V':'Finance',
            'MA':'Finance','BLK':'Finance','SCHW':'Finance',
            'JNJ':'Healthcare','PFE':'Healthcare','ABBV':'Healthcare','MRK':'Healthcare',
            'LLY':'Healthcare','AMGN':'Healthcare','UNH':'Healthcare',
            'XOM':'Energy','CVX':'Energy','COP':'Energy','SLB':'Energy',
            'LMT':'Industrial','BA':'Industrial','CAT':'Industrial','GE':'Industrial',
            'HON':'Industrial','RTX':'Industrial',
        }
        try:
            rows = db.get_recent('filings', limit=500)
            sector_data = {}
            for f in rows:
                sector = SECTORS.get(f.get('ticker', ''), 'Other')
                if sector not in sector_data:
                    sector_data[sector] = {
                        'sector': sector,
                        'buy_value': 0.0, 'sell_value': 0.0,
                        'buy_count': 0, 'sell_count': 0,
                    }
                val = float(f.get('value') or 0)
                if f.get('transaction_type') == 'Buy':
                    sector_data[sector]['buy_value'] += val
                    sector_data[sector]['buy_count'] += 1
                else:
                    sector_data[sector]['sell_value'] += val
                    sector_data[sector]['sell_count'] += 1
            for s in sector_data.values():
                s['net_value'] = s['buy_value'] - s['sell_value']
            result = sorted(sector_data.values(), key=lambda x: abs(x['net_value']), reverse=True)
            return jsonify(result)
        except Exception as e:
            logger.error(f"Sectors error: {e}")
            return jsonify([])

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

# ── Startup: load cache from Supabase + run scheduler in background ──
def _startup():
    global _filings_cache, _last_updated
    try:
        db = SupabaseClient()
        # Load recent filings from Supabase into cache on startup
        rows = db.get_recent('filings', limit=200)
        if rows:
            _filings_cache = rows
            _last_updated = datetime.now().isoformat()
            logger.info(f"Startup: loaded {len(rows)} filings from Supabase into cache")
        else:
            logger.info("Startup: no rows in Supabase yet, cache empty")
    except Exception as e:
        logger.error(f"Startup cache load failed: {e}")

    # Run scraper immediately then schedule it
    _scraper = SECScraper()
    try:
        _scraper.run()
    except Exception as e:
        logger.error(f"Startup scraper run failed: {e}")

    schedule.every(30).minutes.do(_scraper.run)
    schedule.every().day.at("09:30").do(_scraper.run)
    schedule.every().day.at("12:00").do(_scraper.run)
    schedule.every().day.at("16:00").do(_scraper.run)

    while True:
        schedule.run_pending()
        time.sleep(60)

_startup_thread = threading.Thread(target=_startup, daemon=True)
_startup_thread.start()


# ─── Sentiment (from sentiment_backend.py) ───────────────────────

## ── ADD THIS TO YOUR main.py ────────────────────────────────
## No Reddit API credentials needed - uses public JSON endpoints
## Just add these functions and routes to main.py

import re as _re

REDDIT_HEADERS = {
    'User-Agent': 'Mozilla/5.0 WhaleTracker/1.0 (contact@whaletracker.app)',
}

BULLISH_WORDS = [
    'buy', 'bull', 'bullish', 'moon', 'rocket', 'calls', 'long', 'up',
    'gain', 'green', 'pump', 'squeeze', 'breakout', 'undervalued', 'strong',
    'beat', 'upgrade', 'outperform', 'ath', 'hold', 'hodl', 'diamond hands',
    'to the moon', 'buying', 'loading', 'accumulate', 'dip', 'cheap',
]

BEARISH_WORDS = [
    'sell', 'bear', 'bearish', 'puts', 'short', 'down', 'crash', 'dump',
    'loss', 'red', 'overvalued', 'miss', 'downgrade', 'underperform',
    'bubble', 'correction', 'bag', 'rekt', 'bankrupt', 'falling', 'weak',
    'overbought', 'shorting', 'drop', 'tank',
]

IGNORE_WORDS = {
    'I', 'A', 'THE', 'FOR', 'AND', 'OR', 'IN', 'IS', 'IT', 'TO', 'BE',
    'AS', 'AT', 'SO', 'WE', 'BY', 'DO', 'IF', 'ON', 'UP', 'DD', 'PE',
    'EPS', 'CEO', 'IPO', 'ETF', 'SEC', 'IMO', 'WSB', 'RH', 'US', 'EU',
    'UK', 'AM', 'PM', 'TV', 'AI', 'AR', 'VR', 'ER', 'PR', 'HR', 'IT',
    'UI', 'UX', 'YOY', 'QOQ', 'ATH', 'DCA', 'ROI', 'PNL', 'YTD', 'LOL',
    'OMG', 'WTF', 'IMO', 'IMHO', 'TLDR', 'ETA', 'FYI', 'AMA', 'EOD',
}

SUBREDDITS = ['wallstreetbets', 'stocks', 'investing', 'stockmarket', 'options']

def analyze_sentiment_text(text):
    text_lower = text.lower()
    bull = sum(1 for w in BULLISH_WORDS if w in text_lower)
    bear = sum(1 for w in BEARISH_WORDS if w in text_lower)
    total = bull + bear
    if total == 0:
        return 0.0
    return round((bull - bear) / total, 2)

def extract_tickers(text):
    dollar_tickers = set(_re.findall(r'\$([A-Z]{1,5})\b', text.upper()))
    return {t for t in dollar_tickers if t not in IGNORE_WORDS and len(t) >= 2}

def fetch_subreddit_posts(subreddit, sort='hot', limit=100, time_filter='day'):
    try:
        url = f'https://www.reddit.com/r/{subreddit}/{sort}.json'
        params = {'limit': limit, 't': time_filter}
        r = requests.get(url, headers=REDDIT_HEADERS, params=params, timeout=15)
        if r.status_code == 200:
            posts = r.json().get('data', {}).get('children', [])
            return [p['data'] for p in posts]
        logger.warning(f'Reddit {subreddit} returned {r.status_code}')
    except Exception as e:
        logger.error(f'Reddit fetch error {subreddit}: {e}')
    return []

def get_trending_sentiment(period='24h'):
    sort = 'new' if period == '24h' else 'hot'
    time_filter = 'day' if period == '24h' else 'week' if period == '7d' else 'month'
    ticker_data = {}

    for subreddit in SUBREDDITS:
        posts = fetch_subreddit_posts(subreddit, sort=sort, limit=100, time_filter=time_filter)
        logger.info(f'r/{subreddit}: {len(posts)} posts')
        for post in posts:
            title = post.get('title', '')
            body = post.get('selftext', '')
            text = f'{title} {body}'
            upvotes = post.get('ups', 0)
            comments = post.get('num_comments', 0)
            tickers = extract_tickers(text)
            if not tickers:
                continue
            score = analyze_sentiment_text(text)
            for ticker in tickers:
                if ticker not in ticker_data:
                    ticker_data[ticker] = {
                        'ticker': ticker, 'mentions': 0, 'scores': [],
                        'subreddits': set(), 'top_post': '', 'top_upvotes': 0,
                        'total_upvotes': 0, 'total_comments': 0,
                    }
                ticker_data[ticker]['mentions'] += 1
                ticker_data[ticker]['scores'].append(score)
                ticker_data[ticker]['subreddits'].add(subreddit)
                ticker_data[ticker]['total_upvotes'] += upvotes
                ticker_data[ticker]['total_comments'] += comments
                if upvotes > ticker_data[ticker]['top_upvotes']:
                    ticker_data[ticker]['top_upvotes'] = upvotes
                    ticker_data[ticker]['top_post'] = title[:150]
        time.sleep(1)

    results = []
    for ticker, data in ticker_data.items():
        if data['mentions'] < 3:
            continue
        scores = data['scores']
        avg_score = sum(scores) / len(scores) if scores else 0
        bullish = len([s for s in scores if s > 0.2])
        bearish = len([s for s in scores if s < -0.2])
        neutral = len(scores) - bullish - bearish
        total = len(scores)

        insider_buy = False
        company_name = ''
        try:
            result = supabase.table('filings').select('transaction_type')\
                .eq('ticker', ticker).eq('transaction_type', 'Buy')\
                .gte('transaction_date', (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d'))\
                .limit(1).execute()
            insider_buy = len(result.data or []) > 0
            r2 = supabase.table('filings').select('company_name')\
                .eq('ticker', ticker).limit(1).execute()
            if r2.data:
                company_name = r2.data[0].get('company_name', '')
        except:
            pass

        results.append({
            'ticker': ticker, 'company_name': company_name,
            'mentions': data['mentions'],
            'sentiment_score': round(avg_score, 2),
            'bullish_pct': round(bullish / total * 100) if total else 0,
            'bearish_pct': round(bearish / total * 100) if total else 0,
            'neutral_pct': round(neutral / total * 100) if total else 0,
            'subreddits': list(data['subreddits']),
            'top_post': data['top_post'],
            'total_upvotes': data['total_upvotes'],
            'total_comments': data['total_comments'],
            'insider_buy': insider_buy,
        })

    results.sort(key=lambda x: (x['mentions'], x['total_upvotes']), reverse=True)
    return results[:30]

def get_ticker_sentiment(ticker, period='24h'):
    sort = 'new' if period == '24h' else 'hot'
    time_filter = 'day' if period == '24h' else 'week' if period == '7d' else 'month'
    all_posts = []

    for subreddit in SUBREDDITS:
        posts = fetch_subreddit_posts(subreddit, sort=sort, limit=100, time_filter=time_filter)
        for post in posts:
            title = post.get('title', '')
            body = post.get('selftext', '')
            text = f'{title} {body}'
            if f'${ticker}' not in text.upper() and not _re.search(rf'\b{ticker}\b', text.upper()):
                continue
            score = analyze_sentiment_text(text)
            all_posts.append({
                'subreddit': subreddit, 'title': title[:200],
                'sentiment_score': score,
                'upvotes': post.get('ups', 0),
                'comments': post.get('num_comments', 0),
                'url': f"https://reddit.com{post.get('permalink', '')}",
            })
        time.sleep(0.5)

    if not all_posts:
        return {'ticker': ticker, 'mentions': 0, 'posts': [], 'sentiment_score': 0}

    scores = [p['sentiment_score'] for p in all_posts]
    avg = sum(scores) / len(scores)
    bullish = len([s for s in scores if s > 0.2])
    bearish = len([s for s in scores if s < -0.2])

    return {
        'ticker': ticker.upper(), 'mentions': len(all_posts),
        'sentiment_score': round(avg, 2),
        'bullish_pct': round(bullish / len(scores) * 100),
        'bearish_pct': round(bearish / len(scores) * 100),
        'neutral_pct': round((len(scores) - bullish - bearish) / len(scores) * 100),
        'posts': sorted(all_posts, key=lambda x: x['upvotes'], reverse=True)[:10],
    }

@app.route('/api/sentiment/trending')
def sentiment_trending():
    try:
        period = request.args.get('period', '24h')
        data = get_trending_sentiment(period)
        return jsonify(data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/sentiment/<ticker>')
def sentiment_ticker(ticker):
    try:
        period = request.args.get('period', '24h')
        data = get_ticker_sentiment(ticker.upper(), period)
        return jsonify(data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/congress')
def api_congress():
    """Return recent congressional stock trades from Supabase or fallback mock."""
    try:
        db = SupabaseClient()
        result = db.select('congress_trades', params={
            'order': 'trade_date.desc',
            'limit': '50'
        })
        if result and len(result) > 0:
            return jsonify(result)
    except Exception as e:
        logger.warning(f"Congress trades DB fetch failed: {e}")
    # Fallback mock data
    mock = [
        {'member': 'Nancy Pelosi', 'party': 'D', 'chamber': 'House', 'ticker': 'NVDA', 'type': 'Buy', 'amount': '$1M–$5M', 'date': '2026-03-10', 'company': 'NVIDIA Corp'},
        {'member': 'Dan Crenshaw', 'party': 'R', 'chamber': 'House', 'ticker': 'AAPL', 'type': 'Buy', 'amount': '$15K–$50K', 'date': '2026-03-08', 'company': 'Apple Inc'},
        {'member': 'Tommy Tuberville', 'party': 'R', 'chamber': 'Senate', 'ticker': 'TSLA', 'type': 'Sell', 'amount': '$50K–$100K', 'date': '2026-03-07', 'company': 'Tesla Inc'},
        {'member': 'Mark Warner', 'party': 'D', 'chamber': 'Senate', 'ticker': 'MSFT', 'type': 'Buy', 'amount': '$100K–$250K', 'date': '2026-03-05', 'company': 'Microsoft Corp'},
        {'member': 'Michael McCaul', 'party': 'R', 'chamber': 'House', 'ticker': 'GOOGL', 'type': 'Buy', 'amount': '$50K–$100K', 'date': '2026-03-04', 'company': 'Alphabet Inc'},
        {'member': 'Josh Gottheimer', 'party': 'D', 'chamber': 'House', 'ticker': 'META', 'type': 'Buy', 'amount': '$15K–$50K', 'date': '2026-03-03', 'company': 'Meta Platforms'},
        {'member': 'Roger Marshall', 'party': 'R', 'chamber': 'Senate', 'ticker': 'AMZN', 'type': 'Sell', 'amount': '$15K–$50K', 'date': '2026-03-01', 'company': 'Amazon.com Inc'},
        {'member': 'Marjorie Greene', 'party': 'R', 'chamber': 'House', 'ticker': 'AMD', 'type': 'Buy', 'amount': '$15K–$50K', 'date': '2026-02-28', 'company': 'Advanced Micro Devices'},
        {'member': 'Brian Schatz', 'party': 'D', 'chamber': 'Senate', 'ticker': 'PLTR', 'type': 'Buy', 'amount': '$50K–$100K', 'date': '2026-02-27', 'company': 'Palantir Technologies'},
        {'member': 'Ro Khanna', 'party': 'D', 'chamber': 'House', 'ticker': 'COIN', 'type': 'Sell', 'amount': '$15K–$50K', 'date': '2026-02-25', 'company': 'Coinbase Global'},
        {'member': 'Pat Toomey', 'party': 'R', 'chamber': 'Senate', 'ticker': 'JPM', 'type': 'Buy', 'amount': '$100K–$250K', 'date': '2026-02-24', 'company': 'JPMorgan Chase'},
        {'member': 'Nancy Pelosi', 'party': 'D', 'chamber': 'House', 'ticker': 'CRWD', 'type': 'Buy', 'amount': '$500K–$1M', 'date': '2026-02-20', 'company': 'CrowdStrike Holdings'},
    ]
    return jsonify(mock)


@app.route('/api/earnings')
def api_earnings():
    """Return upcoming earnings with insider activity signals."""
    from datetime import datetime, timedelta
    now = datetime.utcnow()

    # Static earnings calendar (updated periodically)
    earnings = [
        {'ticker': 'NVDA', 'company': 'NVIDIA Corporation', 'earnings_date': '2026-05-28', 'insider_action': 'Buy'},
        {'ticker': 'AAPL', 'company': 'Apple Inc', 'earnings_date': '2026-04-30', 'insider_action': 'Buy'},
        {'ticker': 'MSFT', 'company': 'Microsoft Corporation', 'earnings_date': '2026-04-22', 'insider_action': 'Buy'},
        {'ticker': 'META', 'company': 'Meta Platforms', 'earnings_date': '2026-04-23', 'insider_action': 'none'},
        {'ticker': 'AMZN', 'company': 'Amazon.com Inc', 'earnings_date': '2026-04-30', 'insider_action': 'none'},
        {'ticker': 'GOOGL', 'company': 'Alphabet Inc', 'earnings_date': '2026-04-24', 'insider_action': 'Sell'},
        {'ticker': 'TSLA', 'company': 'Tesla Inc', 'earnings_date': '2026-04-22', 'insider_action': 'Sell'},
        {'ticker': 'PLTR', 'company': 'Palantir Technologies', 'earnings_date': '2026-05-05', 'insider_action': 'Buy'},
        {'ticker': 'AMD', 'company': 'Advanced Micro Devices', 'earnings_date': '2026-04-28', 'insider_action': 'none'},
        {'ticker': 'COIN', 'company': 'Coinbase Global', 'earnings_date': '2026-05-08', 'insider_action': 'Buy'},
        {'ticker': 'CRWD', 'company': 'CrowdStrike Holdings', 'earnings_date': '2026-06-03', 'insider_action': 'Buy'},
        {'ticker': 'SNOW', 'company': 'Snowflake Inc', 'earnings_date': '2026-05-21', 'insider_action': 'none'},
    ]

    # Enrich with days_until and sort
    try:
        filings = _filings_cache or []
        buy_tickers = {f.get('ticker', '') for f in filings if f.get('transaction_type') == 'Buy'}
        sell_tickers = {f.get('ticker', '') for f in filings if f.get('transaction_type') == 'Sell'}
        for e in earnings:
            t = e['ticker']
            if t in buy_tickers:
                e['insider_action'] = 'Buy'
            elif t in sell_tickers and e['insider_action'] != 'Buy':
                e['insider_action'] = 'Sell'
    except Exception:
        pass

    result = []
    for e in earnings:
        try:
            dt = datetime.strptime(e['earnings_date'], '%Y-%m-%d')
            e['days_until'] = (dt - now).days
        except Exception:
            e['days_until'] = 999
        result.append(e)

    result.sort(key=lambda x: x['days_until'])
    return jsonify(result)


if __name__ == '__main__':
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == 'api':
        port = int(os.getenv('PORT', 5000))
        app.run(host='0.0.0.0', port=port, debug=False)
    else:
        run_scheduler()
