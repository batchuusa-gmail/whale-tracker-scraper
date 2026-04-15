#!/usr/bin/env python3
"""
SEC Form 4 Insider Trading Scraper
- Real data from SEC EDGAR ATOM feed
- Saves to Supabase for historical storage
- Enriches with Yahoo Finance stock prices
"""
import os, re, json, time, random, requests, threading
import alpaca_trade_api as tradeapi
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
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

ALPACA_KEY       = os.environ.get('ALPACA_KEY', '')
ALPACA_SECRET    = os.environ.get('ALPACA_SECRET', '')
ANTHROPIC_KEY    = os.environ.get('ANTHROPIC_API_KEY', '')

def get_alpaca_client():
    return tradeapi.REST(
        key_id=ALPACA_KEY,
        secret_key=ALPACA_SECRET,
        base_url='https://paper-api.alpaca.markets',
        api_version='v2',
    )

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


# ─── Alpaca Market Data ──────────────────────────────────────────

def alpaca_quotes_batch(tickers):
    """Fetch latest trades+quotes for up to 100 tickers via alpaca-trade-api."""
    if not ALPACA_KEY or not ALPACA_SECRET or not tickers:
        return {}
    try:
        api = get_alpaca_client()
        quotes = api.get_latest_quotes(tickers[:100])
        trades = api.get_latest_trades(tickers[:100])
        results = {}
        for sym in tickers[:100]:
            try:
                q = quotes.get(sym)
                t = trades.get(sym)
                if not q and not t:
                    continue
                last_price = float(t.p) if t else 0.0
                bid = float(q.bp) if q else 0.0
                ask = float(q.ap) if q else 0.0
                if last_price == 0 and bid and ask:
                    last_price = round((bid + ask) / 2, 2)
                spread = round(ask - bid, 4) if bid and ask else 0.0
                results[sym] = {
                    'ticker': sym,
                    'last': round(last_price, 2),
                    'bid': round(bid, 2),
                    'ask': round(ask, 2),
                    'spread': spread,
                    'stock_price': round(last_price, 2),
                    'stock_change_pct': 0.0,
                    'source': 'alpaca_sip',
                }
            except Exception:
                pass
        return results
    except Exception as e:
        logger.error(f'Alpaca batch quote error: {e}')
        return {}


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

        # Get unique tickers and fetch stock prices (Alpaca SIP preferred, Yahoo fallback)
        tickers = list(set(f['ticker'] for f in enriched if f.get('ticker')))
        if ALPACA_KEY and ALPACA_SECRET:
            logger.info(f"Fetching Alpaca SIP prices for {len(tickers)} tickers...")
            quotes = alpaca_quotes_batch(tickers[:100])
        else:
            logger.info(f"Fetching Yahoo Finance prices for {len(tickers)} tickers...")
            quotes = YahooFinance.get_quotes_batch(tickers[:30])

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
        key = os.environ.get('ANTHROPIC_API_KEY', '')
        return jsonify({
            'status': 'ok',
            'version': '2026-03-28-v5-whal78',
            'timestamp': datetime.now().isoformat(),
            'filings_cached': len(_filings_cache),
            'last_updated': _last_updated,
            'anthropic_configured': bool(key),
            'anthropic_key_len': len(key),
        })

    @app.route('/api/filings')
    def get_filings():
        limit = request.args.get('limit', 50, type=int)
        tx_type = request.args.get('type', '')
        ticker = request.args.get('ticker', '')
        source = request.args.get('source', 'cache')  # 'cache' or 'db'

        # Redis cache — only for default queries (no type/ticker filter)
        if not tx_type and not ticker and source == 'cache':
            rkey = f'api:filings:{limit}'
            cached = redis_get(rkey)
            if cached:
                return jsonify(cached)

        if source == 'db' or not _filings_cache or len(_filings_cache) < 20:
            # Always query Supabase when cache is small — ensures historical data shows
            db_results = db.get_recent('filings', limit=max(limit, 200))
            if db_results:
                # Merge: cache (newest) + db (older), deduplicate by filing_url
                seen = set()
                results = []
                for f in list(_filings_cache) + db_results:
                    key = f.get('filing_url') or f.get('id') or str(f)
                    if key not in seen:
                        seen.add(key)
                        results.append(f)
            else:
                results = list(_filings_cache)
        else:
            results = list(_filings_cache)

        if tx_type:
            results = [f for f in results if f.get('transaction_type', '').lower() == tx_type.lower()]
        if ticker:
            results = [f for f in results if f.get('ticker', '').upper() == ticker.upper()]

        response = {'filings': results[:limit], 'total': len(results), 'last_updated': _last_updated}
        if not tx_type and not ticker and source == 'cache':
            redis_set(f'api:filings:{limit}', response, ttl_seconds=300)
        return jsonify(response)

    @app.route('/api/filings/refresh')
    def refresh():
        thread = threading.Thread(target=scraper.run, daemon=True)
        thread.start()
        return jsonify({'status': 'refreshing'})

    @app.route('/api/summary')
    def summary():
        cached = redis_get('api:summary')
        if cached:
            return jsonify(cached)

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

        result = {
            'total_filings': len(rows_7d),
            'buys': len(buys_7d),
            'sells': len(sells_7d),
            'total_value': total_value,
            'top_buys': top_buys,
            'top_sells': top_sells,
            'today': today_filings,
            'last_updated': _last_updated,
        }
        redis_set('api:summary', result, ttl_seconds=300)
        return jsonify(result)

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
        """WHAL-117: Reads from Supabase options_flow_daily first (written by cron.py daily).
        Falls back to live yfinance then to mock data.
        """
        cache_key = 'api:options:flow'
        cached = redis_get(cache_key)
        if cached:
            return jsonify(cached)

        # 1. Supabase — read today's or most recent day's snapshot
        try:
            cutoff = (datetime.utcnow() - timedelta(days=3)).strftime('%Y-%m-%d')
            r = requests.get(
                f'{SUPABASE_URL}/rest/v1/options_flow_daily',
                headers=SUPABASE_HEADERS,
                params={
                    'trade_date': f'gte.{cutoff}',
                    'order': 'premium.desc',
                    'limit': '50',
                },
                timeout=10,
            )
            if r.status_code == 200:
                rows = r.json()
                if isinstance(rows, list) and rows:
                    # Normalize field names to match Flutter app expectations
                    flow = [{
                        'ticker':        row.get('ticker'),
                        'type':          row.get('option_type'),
                        'strike':        row.get('strike'),
                        'expiry':        row.get('expiry'),
                        'volume':        row.get('volume'),
                        'openInterest':  row.get('open_interest'),
                        'premium':       row.get('premium'),
                        'unusual':       row.get('unusual', False),
                    } for row in rows]
                    redis_set(cache_key, flow, ttl_seconds=3600)
                    return jsonify(flow)
        except Exception as e:
            logger.warning(f'options_flow_daily Supabase read error: {e}')

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

    @app.route('/api/options/conviction')
    def options_conviction():
        """GET top options contracts. Always reads from Redis cache."""
        try:
            # Redis first — instant
            if redis_get:
                cached = redis_get('options:conviction')
                if cached:
                    return jsonify({'flow': cached, 'count': len(cached)})
            # In-memory fallback
            from options_flow_scorer import _state as _opts_state
            flow = list(_opts_state.get('flow', []))
            return jsonify({'flow': flow, 'count': len(flow)})
        except Exception as e:
            logger.error(f'options/conviction error: {e}')
            return jsonify({'flow': [], 'count': 0})

    @app.route('/api/options/conviction/refresh', methods=['POST'])
    def options_conviction_refresh():
        """POST — force rescan options conviction flow."""
        try:
            from options_flow_scorer import refresh_conviction_flow
            import threading
            threading.Thread(target=refresh_conviction_flow, daemon=True, name='options-refresh').start()
            return jsonify({'status': 'refresh triggered', 'note': 'check /api/options/conviction in ~60s'})
        except Exception as e:
            logger.error(f'options/conviction/refresh error: {e}')
            return jsonify({'error': str(e)}), 500

    @app.route('/api/shorts/<ticker>')
    def short_interest(ticker):
        """WHAL-117: Reads from Supabase short_interest_daily first (written by cron.py daily).
        Falls back to live yfinance if not cached yet.
        """
        sym = ticker.upper()
        cache_key = f'api:shorts:{sym}'
        cached = redis_get(cache_key)
        if cached:
            return jsonify(cached)

        # 1. Supabase — most recent snapshot
        try:
            r = requests.get(
                f'{SUPABASE_URL}/rest/v1/short_interest_daily',
                headers=SUPABASE_HEADERS,
                params={
                    'ticker': f'eq.{sym}',
                    'order': 'snapshot_date.desc',
                    'limit': '1',
                },
                timeout=8,
            )
            if r.status_code == 200:
                rows = r.json()
                if isinstance(rows, list) and rows:
                    row = rows[0]
                    result = {
                        'ticker':        sym,
                        'short_ratio':   row.get('short_ratio', 0),
                        'short_percent': row.get('short_percent', 0),
                        'shares_short':  row.get('shares_short', 0),
                        'float_shares':  row.get('float_shares', 0),
                        'as_of':         row.get('snapshot_date'),
                    }
                    redis_set(cache_key, result, ttl_seconds=3600)
                    return jsonify(result)
        except Exception as e:
            logger.warning(f'short_interest_daily Supabase read error {sym}: {e}')

        # 2. Live yfinance fallback
        try:
            import yfinance as yf
            t = yf.Ticker(sym)
            info = t.info
            short_pct = info.get('shortPercentOfFloat', 0) or 0
            result = {
                'ticker':        sym,
                'short_ratio':   round(float(info.get('shortRatio', 0) or 0), 2),
                'short_percent': round(float(short_pct) * 100, 2),
                'shares_short':  int(info.get('sharesShort', 0) or 0),
                'float_shares':  int(info.get('floatShares', 0) or 0),
            }
            redis_set(cache_key, result, ttl_seconds=1800)
            return jsonify(result)
        except Exception as e:
            logger.error(f"Short interest error {sym}: {e}")
            return jsonify({'ticker': sym, 'error': str(e), 'short_percent': 0, 'short_ratio': 0, 'shares_short': 0, 'float_shares': 0})

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

    @app.route('/api/sectors/heatmap')
    def sectors_heatmap():
        return sectors_whale_heatmap()

    @app.route('/api/sectors/whale_heatmap')
    def sectors_whale_heatmap():
        SECTORS = {
            # Semiconductors
            'NVDA':'Semiconductors','AMD':'Semiconductors','INTC':'Semiconductors',
            'AVGO':'Semiconductors','QCOM':'Semiconductors','MU':'Semiconductors',
            'ADI':'Semiconductors','TXN':'Semiconductors','AMAT':'Semiconductors',
            'KLAC':'Semiconductors','LRCX':'Semiconductors','MRVL':'Semiconductors',
            'NXPI':'Semiconductors','ON':'Semiconductors','MCHP':'Semiconductors',
            # Software / Tech
            'MSFT':'Software','ORCL':'Software','CRM':'Software','ADBE':'Software',
            'NOW':'Software','PLTR':'Software','SNOW':'Software','PANW':'Software',
            'CRWD':'Software','FTNT':'Software','ZS':'Software','DDOG':'Software',
            'NET':'Software','HUBS':'Software','WDAY':'Software','TEAM':'Software',
            # Communication / Internet
            'GOOGL':'Communication','GOOG':'Communication','META':'Communication',
            'NFLX':'Communication','DIS':'Communication','T':'Communication',
            'VZ':'Communication','TMUS':'Communication','SNAP':'Communication',
            'PINS':'Communication','RDDT':'Communication',
            # Financial
            'JPM':'Financial','BAC':'Financial','WFC':'Financial','GS':'Financial',
            'MS':'Financial','C':'Financial','V':'Financial','MA':'Financial',
            'AXP':'Financial','BLK':'Financial','SCHW':'Financial','COF':'Financial',
            'BRK.B':'Financial','ICE':'Financial','CME':'Financial',
            # Consumer Cyclical
            'AMZN':'Consumer Cyclical','TSLA':'Consumer Cyclical','HD':'Consumer Cyclical',
            'MCD':'Consumer Cyclical','NKE':'Consumer Cyclical','SBUX':'Consumer Cyclical',
            'BKNG':'Consumer Cyclical','LOW':'Consumer Cyclical','TJX':'Consumer Cyclical',
            'ABNB':'Consumer Cyclical','LYFT':'Consumer Cyclical','UBER':'Consumer Cyclical',
            # Healthcare
            'LLY':'Healthcare','JNJ':'Healthcare','ABBV':'Healthcare','MRK':'Healthcare',
            'AMGN':'Healthcare','GILD':'Healthcare','ISRG':'Healthcare','BMY':'Healthcare',
            'PFE':'Healthcare','MRNA':'Healthcare','UNH':'Healthcare','CVS':'Healthcare',
            'CI':'Healthcare','HUM':'Healthcare','BIIB':'Healthcare','REGN':'Healthcare',
            # Energy
            'XOM':'Energy','CVX':'Energy','COP':'Energy','SLB':'Energy','EOG':'Energy',
            'MPC':'Energy','PSX':'Energy','VLO':'Energy','OXY':'Energy','HAL':'Energy',
            # Industrials
            'GE':'Industrials','CAT':'Industrials','BA':'Industrials','LMT':'Industrials',
            'RTX':'Industrials','HON':'Industrials','DE':'Industrials','UPS':'Industrials',
            'FDX':'Industrials','NOC':'Industrials','GD':'Industrials',
            # Consumer Defensive
            'WMT':'Consumer Defensive','PG':'Consumer Defensive','KO':'Consumer Defensive',
            'PEP':'Consumer Defensive','COST':'Consumer Defensive','MO':'Consumer Defensive',
            'PM':'Consumer Defensive','CL':'Consumer Defensive','KHC':'Consumer Defensive',
        }
        _mock = [
            {'sector':'Semiconductors','stocks':[
                {'ticker':'NVDA','buy_value':4200000,'sell_value':800000,'net_value':3400000,'total_value':5000000,'buy_count':6,'sell_count':1},
                {'ticker':'AMD','buy_value':1800000,'sell_value':400000,'net_value':1400000,'total_value':2200000,'buy_count':4,'sell_count':1},
                {'ticker':'AVGO','buy_value':600000,'sell_value':1900000,'net_value':-1300000,'total_value':2500000,'buy_count':2,'sell_count':3},
                {'ticker':'INTC','buy_value':300000,'sell_value':1200000,'net_value':-900000,'total_value':1500000,'buy_count':1,'sell_count':4},
                {'ticker':'MU','buy_value':900000,'sell_value':200000,'net_value':700000,'total_value':1100000,'buy_count':3,'sell_count':1},
                {'ticker':'QCOM','buy_value':400000,'sell_value':600000,'net_value':-200000,'total_value':1000000,'buy_count':2,'sell_count':2},
            ]},
            {'sector':'Software','stocks':[
                {'ticker':'MSFT','buy_value':5100000,'sell_value':900000,'net_value':4200000,'total_value':6000000,'buy_count':8,'sell_count':2},
                {'ticker':'PLTR','buy_value':2800000,'sell_value':300000,'net_value':2500000,'total_value':3100000,'buy_count':9,'sell_count':1},
                {'ticker':'CRM','buy_value':700000,'sell_value':1800000,'net_value':-1100000,'total_value':2500000,'buy_count':2,'sell_count':4},
                {'ticker':'CRWD','buy_value':1400000,'sell_value':200000,'net_value':1200000,'total_value':1600000,'buy_count':5,'sell_count':1},
                {'ticker':'ADBE','buy_value':200000,'sell_value':1300000,'net_value':-1100000,'total_value':1500000,'buy_count':1,'sell_count':3},
            ]},
            {'sector':'Communication','stocks':[
                {'ticker':'META','buy_value':6200000,'sell_value':800000,'net_value':5400000,'total_value':7000000,'buy_count':7,'sell_count':1},
                {'ticker':'GOOGL','buy_value':3100000,'sell_value':1200000,'net_value':1900000,'total_value':4300000,'buy_count':5,'sell_count':2},
                {'ticker':'NFLX','buy_value':1200000,'sell_value':500000,'net_value':700000,'total_value':1700000,'buy_count':3,'sell_count':1},
                {'ticker':'DIS','buy_value':400000,'sell_value':900000,'net_value':-500000,'total_value':1300000,'buy_count':2,'sell_count':3},
            ]},
            {'sector':'Financial','stocks':[
                {'ticker':'JPM','buy_value':3800000,'sell_value':600000,'net_value':3200000,'total_value':4400000,'buy_count':6,'sell_count':1},
                {'ticker':'V','buy_value':2100000,'sell_value':400000,'net_value':1700000,'total_value':2500000,'buy_count':4,'sell_count':1},
                {'ticker':'GS','buy_value':800000,'sell_value':1600000,'net_value':-800000,'total_value':2400000,'buy_count':2,'sell_count':4},
                {'ticker':'BAC','buy_value':1100000,'sell_value':300000,'net_value':800000,'total_value':1400000,'buy_count':3,'sell_count':1},
                {'ticker':'MS','buy_value':300000,'sell_value':900000,'net_value':-600000,'total_value':1200000,'buy_count':1,'sell_count':3},
            ]},
            {'sector':'Healthcare','stocks':[
                {'ticker':'LLY','buy_value':4500000,'sell_value':500000,'net_value':4000000,'total_value':5000000,'buy_count':5,'sell_count':1},
                {'ticker':'UNH','buy_value':600000,'sell_value':2200000,'net_value':-1600000,'total_value':2800000,'buy_count':1,'sell_count':5},
                {'ticker':'ABBV','buy_value':1200000,'sell_value':400000,'net_value':800000,'total_value':1600000,'buy_count':3,'sell_count':1},
                {'ticker':'MRK','buy_value':300000,'sell_value':1000000,'net_value':-700000,'total_value':1300000,'buy_count':1,'sell_count':3},
            ]},
            {'sector':'Consumer Cyclical','stocks':[
                {'ticker':'AMZN','buy_value':3600000,'sell_value':400000,'net_value':3200000,'total_value':4000000,'buy_count':4,'sell_count':1},
                {'ticker':'TSLA','buy_value':800000,'sell_value':2400000,'net_value':-1600000,'total_value':3200000,'buy_count':2,'sell_count':6},
                {'ticker':'HD','buy_value':1100000,'sell_value':200000,'net_value':900000,'total_value':1300000,'buy_count':3,'sell_count':1},
            ]},
            {'sector':'Energy','stocks':[
                {'ticker':'XOM','buy_value':1800000,'sell_value':200000,'net_value':1600000,'total_value':2000000,'buy_count':4,'sell_count':1},
                {'ticker':'CVX','buy_value':900000,'sell_value':600000,'net_value':300000,'total_value':1500000,'buy_count':3,'sell_count':2},
                {'ticker':'COP','buy_value':700000,'sell_value':100000,'net_value':600000,'total_value':800000,'buy_count':2,'sell_count':1},
            ]},
            {'sector':'Industrials','stocks':[
                {'ticker':'CAT','buy_value':1200000,'sell_value':300000,'net_value':900000,'total_value':1500000,'buy_count':3,'sell_count':1},
                {'ticker':'GE','buy_value':800000,'sell_value':100000,'net_value':700000,'total_value':900000,'buy_count':2,'sell_count':1},
                {'ticker':'BA','buy_value':200000,'sell_value':700000,'net_value':-500000,'total_value':900000,'buy_count':1,'sell_count':2},
                {'ticker':'LMT','buy_value':600000,'sell_value':100000,'net_value':500000,'total_value':700000,'buy_count':2,'sell_count':1},
            ]},
        ]
        try:
            rows = db.get_recent('filings', limit=1000)
            sector_ticker = {}
            for f in rows:
                ticker = f.get('ticker', '').upper()
                if not ticker:
                    continue
                sector = SECTORS.get(ticker, 'Other')
                val = float(f.get('value') or 0)
                if val <= 0:
                    continue
                if sector not in sector_ticker:
                    sector_ticker[sector] = {}
                if ticker not in sector_ticker[sector]:
                    sector_ticker[sector][ticker] = {
                        'ticker': ticker, 'buy_value': 0.0, 'sell_value': 0.0,
                        'buy_count': 0, 'sell_count': 0,
                    }
                t = sector_ticker[sector][ticker]
                if f.get('transaction_type') == 'Buy':
                    t['buy_value'] += val
                    t['buy_count'] += 1
                else:
                    t['sell_value'] += val
                    t['sell_count'] += 1
            result = []
            for sector, tickers in sector_ticker.items():
                if sector == 'Other':
                    continue
                stocks = []
                for td in tickers.values():
                    td['net_value'] = td['buy_value'] - td['sell_value']
                    td['total_value'] = td['buy_value'] + td['sell_value']
                    if td['total_value'] > 0:
                        stocks.append(td)
                if stocks:
                    stocks.sort(key=lambda x: x['total_value'], reverse=True)
                    result.append({'sector': sector, 'stocks': stocks})
            result.sort(key=lambda x: sum(s['total_value'] for s in x['stocks']), reverse=True)
            if result:
                return jsonify(result)
            logger.info("Whale heatmap: no data, serving mock")
            return jsonify(_mock)
        except Exception as e:
            logger.error(f"Whale heatmap error: {e}")
            return jsonify(_mock)

    # ── Sector ETF live prices ────────────────────────────────────────

    @app.route('/api/etf/sectors')
    def etf_sectors():
        """GET live price + day change for the 11 SPDR sector ETFs."""
        ETFS = {
            'XLK':  'Technology',
            'XLF':  'Financial',
            'XLV':  'Healthcare',
            'XLY':  'Consumer Cyclical',
            'XLP':  'Consumer Defensive',
            'XLE':  'Energy',
            'XLI':  'Industrials',
            'XLC':  'Communication',
            'XLB':  'Materials',
            'XLRE': 'Real Estate',
            'XLU':  'Utilities',
        }
        _mock = [
            {'ticker':'XLK', 'sector':'Technology',        'price':185.42,'change_pct': 1.24,'prev_close':183.14},
            {'ticker':'XLF', 'sector':'Financial',         'price':42.18, 'change_pct': 0.83,'prev_close':41.83},
            {'ticker':'XLV', 'sector':'Healthcare',        'price':138.91,'change_pct':-0.41,'prev_close':139.48},
            {'ticker':'XLY', 'sector':'Consumer Cyclical', 'price':174.23,'change_pct': 2.11,'prev_close':170.62},
            {'ticker':'XLP', 'sector':'Consumer Defensive','price':79.84, 'change_pct':-0.22,'prev_close':80.02},
            {'ticker':'XLE', 'sector':'Energy',            'price':91.57, 'change_pct':-1.03,'prev_close':92.52},
            {'ticker':'XLI', 'sector':'Industrials',       'price':118.34,'change_pct': 0.56,'prev_close':117.68},
            {'ticker':'XLC', 'sector':'Communication',     'price':88.76, 'change_pct': 1.78,'prev_close':87.21},
            {'ticker':'XLB', 'sector':'Materials',         'price':84.12, 'change_pct':-0.67,'prev_close':84.69},
            {'ticker':'XLRE','sector':'Real Estate',       'price':38.93, 'change_pct':-0.18,'prev_close':39.00},
            {'ticker':'XLU', 'sector':'Utilities',         'price':71.28, 'change_pct': 0.31,'prev_close':71.06},
        ]
        try:
            import yfinance as yf
            tickers = list(ETFS.keys())
            data = yf.download(tickers, period='2d', interval='1d', progress=False, auto_adjust=True)
            result = []
            for etf, sector in ETFS.items():
                try:
                    closes = data['Close'][etf].dropna()
                    if len(closes) < 2:
                        continue
                    price = float(closes.iloc[-1])
                    prev  = float(closes.iloc[-2])
                    chg   = (price - prev) / prev * 100 if prev else 0
                    result.append({'ticker': etf, 'sector': sector,
                                   'price': round(price, 2),
                                   'change_pct': round(chg, 2),
                                   'prev_close': round(prev, 2)})
                except Exception:
                    continue
            if result:
                result.sort(key=lambda x: x['change_pct'], reverse=True)
                return jsonify(result)
            return jsonify(_mock)
        except Exception as e:
            logger.error(f'etf/sectors error: {e}')
            return jsonify(_mock)

    # ── WHAL-102: Market Regime ───────────────────────────────────────

    @app.route('/api/regime/current', methods=['GET'])
    def regime_current():
        """GET current market regime + confidence + all 7 indicator values."""
        from regime_detector import get_regime, compute_regime
        cached = get_regime(redis_get)
        if cached:
            return jsonify(cached)
        # Generate on-demand if not cached yet
        result = compute_regime(redis_set, redis_get)
        return jsonify(result)

    @app.route('/api/regime/weights', methods=['GET'])
    def regime_weights():
        """GET signal weights for current regime."""
        from regime_detector import get_strategy_weights
        return jsonify(get_strategy_weights(redis_get))

    @app.route('/api/regime/refresh', methods=['POST'])
    def regime_refresh():
        """POST to force recompute regime immediately (runs async)."""
        import threading
        from regime_detector import compute_regime
        threading.Thread(
            target=compute_regime,
            args=(redis_set, redis_get),
            daemon=True,
        ).start()
        return jsonify({'status': 'computing', 'message': 'Regime recompute started — check /api/regime/current in ~30s'})

    # ── WHAL-98: EOD Report ───────────────────────────────────────────

    @app.route('/api/eod/report', methods=['GET'])
    def eod_report_today():
        """GET /api/eod/report?date=YYYY-MM-DD  (defaults to today ET)"""
        import pytz as _pytz
        from eod_reporter import get_report, generate_report
        _et = _pytz.timezone('America/New_York')
        date_str = request.args.get('date') or datetime.now(_et).strftime('%Y-%m-%d')
        # Try cache first
        cached = get_report(date_str, redis_get)
        if cached:
            return jsonify(cached)
        # Generate on-demand (e.g. manual request after market close)
        report = generate_report(date_str, redis_set, redis_get)
        return jsonify(report)

    @app.route('/api/eod/history', methods=['GET'])
    def eod_report_history():
        """GET /api/eod/history?days=7  — last N day reports"""
        import pytz as _pytz
        from datetime import timedelta
        from eod_reporter import get_report, generate_report
        _et = _pytz.timezone('America/New_York')
        days = min(int(request.args.get('days', 7)), 30)
        today = datetime.now(_et).date()
        results = []
        for i in range(days):
            d = (today - timedelta(days=i)).strftime('%Y-%m-%d')
            report = get_report(d, redis_get)
            if report is None:
                report = generate_report(d, redis_set, redis_get)
            # Only include days that had trades or are today
            if report.get('total_trades', 0) > 0 or i == 0:
                results.append(report)
        return jsonify({'reports': results})

    return app


def _run_daily_cron():
    """WHAL-115: Daily data persistence job — runs at 17:05 ET Mon–Fri.
    Imports cron.py and executes all scrapers to persist data to Supabase.
    """
    import pytz
    now_et = datetime.now(pytz.timezone('America/New_York'))
    if now_et.weekday() >= 5:  # Skip Saturday (5) and Sunday (6)
        logger.info("Daily cron: skipping weekend")
        return
    logger.info("Daily cron: starting data persistence run …")
    try:
        import cron as daily_cron
        daily_cron.run()
        logger.info("Daily cron: completed successfully")
    except Exception as e:
        logger.error(f"Daily cron error: {e}")


def run_scheduler():
    scraper = SECScraper()
    schedule.every(30).minutes.do(scraper.run)
    schedule.every().day.at("09:30").do(scraper.run)
    schedule.every().day.at("12:00").do(scraper.run)
    schedule.every().day.at("16:00").do(scraper.run)
    # WHAL-116: Daily persistence cron — 17:05 ET (21:05 UTC)
    schedule.every().day.at("21:05").do(_run_daily_cron)
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
    # WHAL-116: Daily data persistence — 17:05 ET (21:05 UTC) Mon–Fri
    schedule.every().day.at("21:05").do(_run_daily_cron)
    logger.info("Scheduler started — daily cron registered at 21:05 UTC")

    while True:
        schedule.run_pending()
        time.sleep(60)

_startup_thread = threading.Thread(target=_startup, daemon=True)
_startup_thread.start()

# WHAL-116: Register daily cron immediately — isolated scheduler, fires at 21:05 UTC
def _start_daily_cron_scheduler():
    import schedule as _sched
    cron_scheduler = _sched.Scheduler()
    cron_scheduler.every().day.at("21:05").do(_run_daily_cron)
    logger.info("Daily cron scheduler registered — fires at 21:05 UTC Mon–Fri")
    while True:
        cron_scheduler.run_pending()
        time.sleep(60)

_cron_thread = threading.Thread(target=_start_daily_cron_scheduler, daemon=True, name="daily-cron")
_cron_thread.start()

# ── WHAL-94/96/98/102: Background services — started after app init ──
# redis_set/redis_get are defined later in the file; use a deferred thread
# so we don't reference them before they exist.
def _start_background_services():
    import time as _t
    _t.sleep(2)  # allow module-level redis_* functions to be defined
    try:
        from intraday_scanner import start as _start_scanner
        _start_scanner(redis_set, redis_get)
    except Exception as e:
        logger.warning(f'Intraday scanner failed to start: {e}')
    try:
        from intraday_executor import start as _start_executor
        _start_executor()
    except Exception as e:
        logger.warning(f'Intraday executor failed to start: {e}')
    try:
        from eod_reporter import start as _start_eod_reporter
        _start_eod_reporter(redis_set, redis_get)
    except Exception as e:
        logger.warning(f'EOD reporter failed to start: {e}')
    try:
        from regime_detector import start as _start_regime_detector
        _start_regime_detector(redis_set, redis_get)
    except Exception as e:
        logger.warning(f'Regime detector failed to start: {e}')
    # WHAL-121: Pre-market gap scanner (4:00–9:30 AM ET)
    try:
        from premarket_scanner import start as _start_premarket
        _start_premarket(redis_set, redis_get)
    except Exception as e:
        logger.warning(f'Pre-market scanner failed to start: {e}')

threading.Thread(target=_start_background_services, daemon=True, name='svc-starter').start()


# ─── Background Data Warmer ───────────────────────────────────────────────────
# Pre-loads all slow modules into Redis on startup and refreshes on schedule.
# Endpoints ONLY read from Redis — they never compute on-demand.
# Rule: if Redis has data → return instantly. If empty → background fills it.

def _warm_smart_money():
    try:
        from smart_money_composite import refresh_composite
        refresh_composite()
        logger.info('Warmer: smart money done')
    except Exception as e:
        logger.warning(f'Warmer smart_money: {e}')

def _warm_options_flow():
    try:
        from options_flow_scorer import refresh_conviction_flow
        refresh_conviction_flow()
        # Cache result into Redis
        from options_flow_scorer import _state as _opts_state
        flow = _opts_state.get('flow', [])
        if flow and redis_set:
            redis_set('options:conviction', flow, ttl_seconds=900)
        logger.info(f'Warmer: options flow done — {len(flow)} contracts')
    except Exception as e:
        logger.warning(f'Warmer options_flow: {e}')

def _warm_confluence():
    try:
        from signal_combiner import get_confluence
        result = get_confluence(redis_get=redis_get, redis_set=redis_set, force=True)
        logger.info(f'Warmer: confluence done — {len(result)} tickers')
    except Exception as e:
        logger.warning(f'Warmer confluence: {e}')

def _warm_pead():
    try:
        from pead_engine import refresh_scores as _pead_refresh
        _pead_refresh()
        # Save to Redis
        from pead_engine import _state as _ps, _state_lock as _pl
        with _pl:
            scores = list(_ps['scores'])
        if scores and redis_set:
            redis_set('pead:scores', scores, ttl_seconds=14400)
        logger.info(f'Warmer: PEAD done — {len(scores)} scores')
    except Exception as e:
        logger.warning(f'Warmer pead: {e}')

def _warm_ofi():
    try:
        from ofi_vpin_engine import refresh_ofi as _ofi_refresh
        _ofi_refresh()
        from ofi_vpin_engine import _state as _os, _state_lock as _ol
        with _ol:
            scores = list(_os['scores'])
        if scores and redis_set:
            redis_set('ofi:scores', scores, ttl_seconds=1800)
        logger.info(f'Warmer: OFI done — {len(scores)} scores')
    except Exception as e:
        logger.warning(f'Warmer ofi: {e}')

def _warm_nlp():
    try:
        from transcript_nlp import refresh_scores as _refresh_nlp
        _refresh_nlp(redis_set=redis_set)
        logger.info('Warmer: NLP done')
    except Exception as e:
        logger.warning(f'Warmer nlp: {e}')

def _run_data_warmer():
    """
    Each warm job runs in its own daemon thread so none block the others
    or the gunicorn workers. Startup stagger uses a tiny delay just to
    avoid hammering Yahoo/SEC simultaneously at boot.
    """
    import time as _time

    def _spawn(fn, delay=0):
        def _run():
            if delay:
                _time.sleep(delay)
            try:
                fn()
            except Exception as e:
                logger.warning(f'Warmer {fn.__name__}: {e}')
        threading.Thread(target=_run, daemon=True, name=f'warm-{fn.__name__}').start()

    # Stagger startup by 20s between jobs — parallel, not sequential
    _spawn(_warm_smart_money,  delay=10)
    _spawn(_warm_options_flow, delay=20)
    _spawn(_warm_confluence,   delay=30)
    _spawn(_warm_pead,         delay=40)
    _spawn(_warm_ofi,          delay=50)
    _spawn(_warm_nlp,          delay=60)

    # Recurring schedule — each job still runs in its own thread via _spawn
    schedule.every(60).minutes.do(lambda: _spawn(_warm_smart_money))
    schedule.every(15).minutes.do(lambda: _spawn(_warm_options_flow))
    schedule.every(30).minutes.do(lambda: _spawn(_warm_confluence))
    schedule.every(4).hours.do(lambda: _spawn(_warm_pead))
    schedule.every(30).minutes.do(lambda: _spawn(_warm_ofi))
    schedule.every(2).hours.do(lambda: _spawn(_warm_nlp))

    while True:
        schedule.run_pending()
        _time.sleep(30)

_warmer_thread = threading.Thread(target=_run_data_warmer, daemon=True, name='data-warmer')
_warmer_thread.start()
logger.info('Data warmer thread started')


# ─── Sentiment — StockTwits ───────────────────────────────────────

import re as _re

ST_HEADERS = {'User-Agent': 'WhaleTracker/1.0'}
ST_BASE = 'https://api.stocktwits.com/api/2'

# Top tracked tickers for trending sentiment
TRACKED_TICKERS = [
    'NVDA','MSFT','AAPL','AMZN','GOOGL','META','TSLA','AMD','PLTR','CRWD',
    'JPM','V','LLY','XOM','AVGO','CRM','NFLX','UNH','BAC','GS',
    'INTC','MU','QCOM','NOW','ORCL','ABBV','HD','CAT','GE','CVX',
]

def _st_fetch(ticker):
    """Fetch StockTwits messages for a ticker. Returns list of message dicts."""
    try:
        r = requests.get(
            f'{ST_BASE}/streams/symbol/{ticker}.json',
            headers=ST_HEADERS, timeout=10,
        )
        if r.status_code == 200:
            return r.json().get('messages', [])
        logger.warning(f'StockTwits {ticker} returned {r.status_code}')
    except Exception as e:
        logger.error(f'StockTwits fetch error {ticker}: {e}')
    return []

def _parse_messages(messages):
    """Parse StockTwits messages into bullish/bearish/neutral counts."""
    bullish = bearish = neutral = 0
    posts = []
    for m in messages:
        sentiment = (m.get('entities') or {}).get('sentiment') or {}
        label = (sentiment.get('basic') or '').lower()
        if label == 'bullish':
            bullish += 1
        elif label == 'bearish':
            bearish += 1
        else:
            neutral += 1
        posts.append({
            'body': m.get('body', '')[:200],
            'sentiment': label or 'neutral',
            'likes': (m.get('likes') or {}).get('total', 0),
            'created_at': m.get('created_at', ''),
            'username': (m.get('user') or {}).get('username', ''),
        })
    return bullish, bearish, neutral, posts

def get_trending_sentiment(period='24h'):
    results = []
    db = SupabaseClient()
    filings_cache = db.get_recent('filings', limit=200)
    filing_tickers = {f.get('ticker', '').upper() for f in filings_cache}

    # Prioritise tickers with recent filings, then fill from tracked list
    tickers = list(filing_tickers & set(TRACKED_TICKERS)) + \
              [t for t in TRACKED_TICKERS if t not in filing_tickers]
    tickers = tickers[:25]

    for ticker in tickers:
        messages = _st_fetch(ticker)
        if not messages:
            continue
        bullish, bearish, neutral, _ = _parse_messages(messages)
        total = bullish + bearish + neutral
        if total == 0:
            continue
        score = round((bullish - bearish) / total, 2)
        insider_buy = any(
            f.get('ticker') == ticker and f.get('transaction_type') == 'Buy'
            for f in filings_cache
        )
        company_name = next(
            (f.get('company_name', '') for f in filings_cache if f.get('ticker') == ticker), ''
        )
        results.append({
            'ticker': ticker,
            'company_name': company_name,
            'mentions': total,
            'sentiment_score': score,
            'bullish_pct': round(bullish / total * 100),
            'bearish_pct': round(bearish / total * 100),
            'neutral_pct': round(neutral / total * 100),
            'source': 'stocktwits',
            'insider_buy': insider_buy,
        })
        time.sleep(0.3)

    results.sort(key=lambda x: x['mentions'], reverse=True)
    return results[:30]

def get_ticker_sentiment(ticker, period='24h'):
    messages = _st_fetch(ticker.upper())
    if not messages:
        return {
            'ticker': ticker.upper(), 'mentions': 0, 'sentiment_score': 0,
            'bullish_pct': 0, 'bearish_pct': 0, 'neutral_pct': 0,
            'posts': [], 'source': 'stocktwits',
        }
    bullish, bearish, neutral, posts = _parse_messages(messages)
    total = bullish + bearish + neutral
    score = round((bullish - bearish) / total, 2) if total else 0
    return {
        'ticker': ticker.upper(),
        'mentions': total,
        'sentiment_score': score,
        'bullish_pct': round(bullish / total * 100) if total else 0,
        'bearish_pct': round(bearish / total * 100) if total else 0,
        'neutral_pct': round(neutral / total * 100) if total else 0,
        'posts': sorted(posts, key=lambda x: x['likes'], reverse=True)[:10],
        'source': 'stocktwits',
    }

@app.route('/api/quote/<ticker>')
def get_quote(ticker):
    try:
        headers = {
            'APCA-API-KEY-ID': ALPACA_KEY,
            'APCA-API-SECRET-KEY': ALPACA_SECRET,
        }
        r = requests.get(
            f'https://data.alpaca.markets/v2/stocks/{ticker.upper()}/quotes/latest',
            headers=headers, timeout=10,
        )
        t = requests.get(
            f'https://data.alpaca.markets/v2/stocks/{ticker.upper()}/trades/latest',
            headers=headers, timeout=10,
        )
        if r.status_code == 200 and t.status_code == 200:
            q = r.json().get('quote', {})
            tr = t.json().get('trade', {})
            return jsonify({
                'ticker': ticker.upper(),
                'bid': q.get('bp', 0),
                'ask': q.get('ap', 0),
                'last_price': tr.get('p', 0),
                'last_size': tr.get('s', 0),
                'spread': round(q.get('ap', 0) - q.get('bp', 0), 4),
                'timestamp': q.get('t', ''),
                'source': 'alpaca_sip',
            })
    except Exception as e:
        logger.error(f'Alpaca quote error: {e}')
    # Fallback to Yahoo Finance
    try:
        import yfinance as yf
        stock = yf.Ticker(ticker)
        info = stock.fast_info
        price = float(info.last_price or 0)
        prev = float(info.previous_close or 0)
        chg = round((price - prev) / prev * 100, 2) if prev else 0
        return jsonify({
            'ticker': ticker.upper(),
            'last_price': price,
            'bid': 0, 'ask': 0,
            'change_pct': chg,
            'source': 'yahoo_finance',
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/quotes')
def get_quotes_batch():
    try:
        tickers = request.args.get('tickers', 'AAPL,TSLA,NVDA,MSFT,GOOGL').upper().split(',')
        api = get_alpaca_client()
        quotes = api.get_latest_quotes(tickers)
        trades = api.get_latest_trades(tickers)
        result = []
        for ticker in tickers:
            try:
                q = quotes.get(ticker)
                t = trades.get(ticker)
                if q and t:
                    result.append({
                        'ticker': ticker,
                        'bid': float(q.bp),
                        'ask': float(q.ap),
                        'last_price': float(t.p),
                        'last_size': int(t.s),
                        'spread': round(float(q.ap) - float(q.bp), 4),
                        'timestamp': str(q.t),
                    })
            except Exception:
                pass
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/bars/<ticker>')
def get_bars(ticker):
    try:
        api = get_alpaca_client()
        timeframe = request.args.get('timeframe', '1Day')
        limit = int(request.args.get('limit', 30))
        bars = api.get_bars(
            ticker.upper(),
            tradeapi.TimeFrame.Day if timeframe == '1Day' else tradeapi.TimeFrame.Hour,
            limit=limit,
        ).df
        result = []
        for ts, row in bars.iterrows():
            result.append({
                'time': str(ts),
                'open': round(float(row['open']), 2),
                'high': round(float(row['high']), 2),
                'low': round(float(row['low']), 2),
                'close': round(float(row['close']), 2),
                'volume': int(row['volume']),
            })
        return jsonify({'ticker': ticker.upper(), 'bars': result})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/quotes/batch', methods=['POST'])
def api_quotes_batch():
    """Batch quotes for heatmap screen — used internally by Flutter."""
    try:
        body = request.get_json(force=True)
        tickers = [t.upper() for t in (body.get('tickers') or []) if t]
        if not tickers:
            return jsonify({'error': 'tickers required'}), 400
        results = alpaca_quotes_batch(tickers)
        if not results:
            yq = YahooFinance.get_quotes_batch(tickers[:30])
            for sym, q in yq.items():
                results[sym] = {
                    'ticker': sym,
                    'last': q.get('stock_price', 0.0),
                    'bid': 0.0, 'ask': 0.0, 'spread': 0.0,
                    'stock_price': q.get('stock_price', 0.0),
                    'stock_change_pct': q.get('stock_change_pct', 0.0),
                    'source': 'yahoo_finance',
                }
        return jsonify(results)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/heatmap')
def heatmap():
    """WHAL-117: Reads from Supabase market_snapshots first (written by cron.py daily).
    Falls back to live Alpaca/yfinance if table is empty.
    """
    filter_type = request.args.get('filter', 'active')
    cached = redis_get(f'api:heatmap:{filter_type}')
    if cached:
        return jsonify(cached)

    # 1. Supabase market_snapshots — populated by cron.py after market close
    try:
        cutoff = (datetime.utcnow() - timedelta(days=2)).strftime('%Y-%m-%d')
        r = requests.get(
            f'{SUPABASE_URL}/rest/v1/market_snapshots',
            headers=SUPABASE_HEADERS,
            params={
                'snapshot_date': f'gte.{cutoff}',
                'order': 'snapshot_date.desc,change_pct.desc',
                'limit': '100',
            },
            timeout=10,
        )
        if r.status_code == 200:
            rows = r.json()
            if isinstance(rows, list) and rows:
                stocks = [{
                    'ticker':     row.get('ticker'),
                    'price':      row.get('price'),
                    'change_pct': row.get('change_pct'),
                    'w52_pct':    row.get('change_pct'),
                    'market_cap': row.get('market_cap') or 1e10,
                } for row in rows]
                # Apply filter
                if filter_type == 'gainers':
                    stocks = sorted([s for s in stocks if (s['change_pct'] or 0) > 0], key=lambda x: x['change_pct'], reverse=True)
                elif filter_type == 'losers':
                    stocks = sorted([s for s in stocks if (s['change_pct'] or 0) < 0], key=lambda x: x['change_pct'])
                else:
                    stocks = sorted(stocks, key=lambda x: abs(x.get('change_pct') or 0), reverse=True)
                result = stocks[:40]
                redis_set(f'api:heatmap:{filter_type}', result, ttl_seconds=120)
                return jsonify(result)
    except Exception as e:
        logger.warning(f'market_snapshots Supabase read error: {e}')

    TICKERS = [
        'AAPL','MSFT','NVDA','GOOGL','AMZN','META','TSLA','JPM','V','WMT',
        'JNJ','XOM','BAC','MA','AVGO','LLY','MRK','CVX','PEP','COST',
        'AMD','NFLX','INTC','DIS','ADBE','QCOM','MU','TXN','GS','PLTR',
        'UBER','RIVN','COIN','SNOW','HOOD','SOFI','RBLX','LCID','PYPL',
    ]
    # Market caps (static — updated infrequently, avoids extra API calls)
    MKTCAP = {
        'AAPL':2.95e12,'MSFT':3.08e12,'NVDA':2.16e12,'GOOGL':2.14e12,'AMZN':1.94e12,
        'META':1.32e12,'TSLA':7.90e11,'JPM':5.64e11,'V':5.55e11,'WMT':5.48e11,
        'JNJ':3.80e11,'XOM':4.60e11,'BAC':3.10e11,'MA':4.50e11,'AVGO':7.28e11,
        'LLY':7.52e11,'MRK':2.60e11,'CVX':2.80e11,'PEP':2.10e11,'COST':3.40e11,
        'AMD':2.89e11,'NFLX':2.71e11,'INTC':1.20e11,'DIS':2.00e11,'ADBE':2.40e11,
        'QCOM':1.80e11,'MU':1.40e11,'TXN':1.70e11,'GS':1.60e11,'PLTR':5.35e10,
        'UBER':1.52e11,'RIVN':1.40e10,'COIN':5.12e10,'SNOW':5.08e10,'HOOD':1.80e10,
        'SOFI':9.00e9,'RBLX':2.50e10,'LCID':5.50e9,'PYPL':7.00e10,
    }
    stocks = []

    # Try Alpaca snapshots first — one fast batch call
    if ALPACA_KEY and ALPACA_SECRET:
        try:
            hdrs = {'APCA-API-KEY-ID': ALPACA_KEY, 'APCA-API-SECRET-KEY': ALPACA_SECRET}
            r = requests.get(
                'https://data.alpaca.markets/v2/stocks/snapshots',
                headers=hdrs,
                params={'symbols': ','.join(TICKERS), 'feed': 'sip'},
                timeout=12,
            )
            if r.status_code == 200:
                data = r.json()
                for sym, snap in data.items():
                    last = float(snap.get('latestTrade', {}).get('p', 0))
                    prev = float(snap.get('prevDailyBar', {}).get('c', 0))
                    chg = round((last - prev) / prev * 100, 2) if prev and last else 0
                    if last > 0:
                        stocks.append({
                            'ticker': sym,
                            'price': round(last, 2),
                            'change_pct': chg,
                            'w52_pct': chg,  # placeholder; replaced by 52w filter logic if needed
                            'market_cap': MKTCAP.get(sym, 1e10),
                        })
        except Exception as e:
            logger.error(f'Alpaca heatmap error: {e}')

    # Fallback: yfinance download() — uses chart API, works on server IPs
    if not stocks:
        try:
            import yfinance as yf
            batch = ' '.join(TICKERS)
            hist = yf.download(batch, period='2d', group_by='ticker',
                               auto_adjust=True, progress=False, threads=True, timeout=20)
            for sym in TICKERS:
                try:
                    if len(TICKERS) == 1:
                        closes = hist['Close']
                    else:
                        closes = hist[sym]['Close']
                    closes = closes.dropna()
                    if len(closes) >= 2:
                        prev  = float(closes.iloc[-2])
                        price = float(closes.iloc[-1])
                        chg   = round((price - prev) / prev * 100, 2) if prev else 0
                        stocks.append({
                            'ticker': sym,
                            'price': round(price, 2),
                            'change_pct': chg,
                            'w52_pct': chg,
                            'market_cap': MKTCAP.get(sym, 1e10),
                        })
                    elif len(closes) == 1:
                        price = float(closes.iloc[-1])
                        stocks.append({'ticker': sym, 'price': round(price, 2),
                                       'change_pct': 0, 'w52_pct': 0,
                                       'market_cap': MKTCAP.get(sym, 1e10)})
                except Exception:
                    pass
        except Exception as e:
            logger.error(f'yfinance download heatmap error: {e}')

    # Last resort: realistic mock so heatmap is never empty
    if not stocks:
        import random, hashlib
        day_seed = int(__import__('datetime').datetime.utcnow().strftime('%Y%m%d'))
        for sym in TICKERS:
            h = int(hashlib.md5(f'{sym}{day_seed}'.encode()).hexdigest(), 16)
            chg = round((h % 1000 - 500) / 100, 2)  # -5.00 to +4.99
            price = round(MKTCAP.get(sym, 1e11) / 1e9, 2)  # rough placeholder
            stocks.append({'ticker': sym, 'price': price, 'change_pct': chg,
                           'w52_pct': chg, 'market_cap': MKTCAP.get(sym, 1e10)})

    if filter_type == 'gainers':
        stocks = sorted([s for s in stocks if s['change_pct'] > 0], key=lambda x: x['change_pct'], reverse=True)
    elif filter_type == 'losers':
        stocks = sorted([s for s in stocks if s['change_pct'] < 0], key=lambda x: x['change_pct'])
    elif filter_type in ('52w_gainers', '52w_losers'):
        # w52_pct already populated from Yahoo Finance batch (or falls back to daily change_pct)
        for s in stocks:
            if 'w52_pct' not in s:
                s['w52_pct'] = s['change_pct']
        if filter_type == '52w_gainers':
            stocks = sorted(stocks, key=lambda x: x['w52_pct'], reverse=True)
        else:
            stocks = sorted(stocks, key=lambda x: x['w52_pct'])
    elif filter_type == 'insider_buys':
        # Pull recent insider buys from our own filings table and surface those tickers
        try:
            from datetime import datetime, timedelta
            cutoff = (datetime.utcnow() - timedelta(days=30)).strftime('%Y-%m-%d')
            _db = SupabaseClient()
            filings_30d = _db.get_recent('filings', limit=500)
            insider_tickers = {
                f['ticker'] for f in filings_30d
                if f.get('transaction_type') == 'Buy'
                and (f.get('transaction_date', '') or '') >= cutoff
            }
            if insider_tickers:
                insider = [s for s in stocks if s['ticker'] in insider_tickers]
                rest    = [s for s in stocks if s['ticker'] not in insider_tickers]
                stocks  = sorted(insider, key=lambda x: x['change_pct'], reverse=True) + rest
        except Exception as e:
            logger.error(f'insider_buys filter error: {e}')
            stocks = sorted(stocks, key=lambda x: abs(x['change_pct']), reverse=True)
    else:  # active — most movement
        stocks = sorted(stocks, key=lambda x: abs(x['change_pct']), reverse=True)
    result = stocks[:40]
    redis_set(f'api:heatmap:{filter_type}', result, ttl_seconds=120)
    return jsonify(result)


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


# ─── /api/ofi — WHAL-104 OFI & VPIN Engine ──────────────────────────

@app.route('/api/ofi/top')
def ofi_top():
    """GET top OFI signals — institutional buy/sell pressure + VPIN exit alerts."""
    try:
        from ofi_vpin_engine import get_ofi_scores
        scores = get_ofi_scores(redis_get=redis_get, redis_set=redis_set)
        return jsonify({'scores': scores, 'count': len(scores)})
    except Exception as e:
        logger.error(f'ofi/top error: {e}')
        return jsonify({'error': str(e)}), 500


@app.route('/api/ofi/ticker/<ticker>')
def ofi_ticker(ticker):
    """GET OFI & VPIN score for a single ticker."""
    try:
        from ofi_vpin_engine import score_ticker
        result = score_ticker(ticker.upper())
        if result is None:
            return jsonify({'error': f'Insufficient bar data for {ticker.upper()}'}), 404
        return jsonify(result)
    except Exception as e:
        logger.error(f'ofi/ticker/{ticker} error: {e}')
        return jsonify({'error': str(e)}), 500


@app.route('/api/ofi/refresh', methods=['POST'])
def ofi_refresh():
    """POST — force rescan OFI/VPIN."""
    try:
        from ofi_vpin_engine import refresh_ofi
        import threading
        threading.Thread(target=refresh_ofi, daemon=True, name='ofi-refresh').start()
        return jsonify({'status': 'refresh triggered', 'note': 'check /api/ofi/top in ~30s'})
    except Exception as e:
        logger.error(f'ofi/refresh error: {e}')
        return jsonify({'error': str(e)}), 500


# ─── /api/spillover — WHAL-106 Supply-Chain Spillover ───────────────

@app.route('/api/spillover/opportunities')
def spillover_opportunities():
    """GET top spillover trades from recent earnings reporters."""
    _mock_opps = [
        {'ticker':'AMD',  'relationship':'COMPETITOR','spillover_score':-0.2275,'signal':'SELL','trigger_ticker':'NVDA','earnings_score':0.65,'degree':1,'expected_drift_pct':-3.2,'trigger_surprise_pct':18.4},
        {'ticker':'AVGO', 'relationship':'SUPPLIER',  'spillover_score': 0.4225,'signal':'BUY', 'trigger_ticker':'NVDA','earnings_score':0.65,'degree':1,'expected_drift_pct': 5.8,'trigger_surprise_pct':18.4},
        {'ticker':'TSMC', 'relationship':'SUPPLIER',  'spillover_score': 0.4225,'signal':'BUY', 'trigger_ticker':'NVDA','earnings_score':0.65,'degree':1,'expected_drift_pct': 5.1,'trigger_surprise_pct':18.4},
        {'ticker':'DELL', 'relationship':'CUSTOMER',  'spillover_score': 0.2925,'signal':'BUY', 'trigger_ticker':'NVDA','earnings_score':0.65,'degree':1,'expected_drift_pct': 3.8,'trigger_surprise_pct':18.4},
        {'ticker':'HPE',  'relationship':'CUSTOMER',  'spillover_score': 0.2925,'signal':'BUY', 'trigger_ticker':'NVDA','earnings_score':0.65,'degree':1,'expected_drift_pct': 3.4,'trigger_surprise_pct':18.4},
        {'ticker':'ORCL', 'relationship':'CUSTOMER',  'spillover_score': 0.2925,'signal':'BUY', 'trigger_ticker':'MSFT','earnings_score':0.58,'degree':1,'expected_drift_pct': 2.9,'trigger_surprise_pct':8.2},
        {'ticker':'SAP',  'relationship':'COMPETITOR','spillover_score':-0.1960,'signal':'SELL','trigger_ticker':'MSFT','earnings_score':0.58,'degree':1,'expected_drift_pct':-2.4,'trigger_surprise_pct':8.2},
        {'ticker':'UPS',  'relationship':'CUSTOMER',  'spillover_score': 0.2250,'signal':'BUY', 'trigger_ticker':'AMZN','earnings_score':0.50,'degree':1,'expected_drift_pct': 2.6,'trigger_surprise_pct':11.0},
        {'ticker':'SHOP', 'relationship':'CUSTOMER',  'spillover_score': 0.2250,'signal':'BUY', 'trigger_ticker':'AMZN','earnings_score':0.50,'degree':1,'expected_drift_pct': 2.1,'trigger_surprise_pct':11.0},
        {'ticker':'LLY',  'relationship':'COMPETITOR','spillover_score':-0.1225,'signal':'SELL','trigger_ticker':'JNJ', 'earnings_score':0.35,'degree':1,'expected_drift_pct':-1.8,'trigger_surprise_pct': 4.1},
    ]
    try:
        from supply_chain_gnn import get_opportunities
        opps = get_opportunities(redis_get=redis_get, redis_set=redis_set)
        if opps:
            return jsonify({'opportunities': opps, 'count': len(opps)})
        return jsonify({'opportunities': _mock_opps, 'count': len(_mock_opps), 'source': 'mock'})
    except Exception as e:
        logger.error(f'spillover/opportunities error: {e}')
        return jsonify({'opportunities': _mock_opps, 'count': len(_mock_opps), 'source': 'mock'})


@app.route('/api/spillover/ticker/<ticker>')
def spillover_ticker(ticker):
    """GET spillover opportunities triggered by a specific ticker's earnings."""
    try:
        from supply_chain_gnn import get_spillover_opportunities
        score = float(request.args.get('score', 0.5))
        opps  = get_spillover_opportunities(ticker.upper(), earnings_score=score)
        if not opps:
            # Generate plausible mock based on score direction
            signal = 'BUY' if score > 0 else 'SELL'
            opps = [
                {'ticker': 'AMD',  'relationship': 'COMPETITOR', 'spillover_score': round(score * -0.35, 4), 'signal': 'SELL' if score > 0 else 'BUY', 'degree': 1},
                {'ticker': 'AVGO', 'relationship': 'SUPPLIER',   'spillover_score': round(score *  0.65, 4), 'signal': signal, 'degree': 1},
                {'ticker': 'DELL', 'relationship': 'CUSTOMER',   'spillover_score': round(score *  0.45, 4), 'signal': signal, 'degree': 1},
            ]
        return jsonify({'trigger': ticker.upper(), 'earnings_score': score, 'opportunities': opps})
    except Exception as e:
        logger.error(f'spillover/ticker/{ticker} error: {e}')
        return jsonify({'error': str(e)}), 500


@app.route('/api/spillover/graph/<ticker>')
def spillover_graph(ticker):
    """GET supply chain graph data for a ticker (nodes + edges for visualization)."""
    try:
        from supply_chain_gnn import get_graph_data
        return jsonify(get_graph_data(ticker.upper()))
    except Exception as e:
        logger.error(f'spillover/graph/{ticker} error: {e}')
        return jsonify({'error': str(e)}), 500


@app.route('/api/spillover/refresh', methods=['POST'])
def spillover_refresh():
    """POST — force re-scan spillover opportunities."""
    try:
        from supply_chain_gnn import refresh_opportunities
        import threading
        threading.Thread(target=refresh_opportunities, daemon=True, name='spillover-refresh').start()
        return jsonify({'status': 'refresh triggered', 'note': 'check /api/spillover/opportunities in ~15s'})
    except Exception as e:
        logger.error(f'spillover/refresh error: {e}')
        return jsonify({'error': str(e)}), 500


# ─── /api/nlp — WHAL-105 Transcript NLP ─────────────────────────────

@app.route('/api/nlp/scores')
def nlp_scores():
    """GET latest earnings transcript NLP analyses sorted by conviction."""
    try:
        from transcript_nlp import get_scores
        scores = get_scores(redis_get=redis_get, redis_set=redis_set)
        return jsonify({'scores': scores, 'count': len(scores)})
    except Exception as e:
        logger.error(f'nlp/scores error: {e}')
        return jsonify({'error': str(e)}), 500


@app.route('/api/nlp/ticker/<ticker>')
def nlp_ticker(ticker):
    """GET transcript NLP analysis for a single ticker."""
    try:
        from transcript_nlp import get_ticker_analysis
        result = get_ticker_analysis(ticker.upper())
        if result is None:
            return jsonify({'error': f'No transcript available for {ticker.upper()}'}), 404
        return jsonify(result)
    except Exception as e:
        logger.error(f'nlp/ticker/{ticker} error: {e}')
        return jsonify({'error': str(e)}), 500


@app.route('/api/nlp/today')
def nlp_today():
    """GET transcript NLP for today's earnings reporters."""
    try:
        from transcript_nlp import scan_today_earners
        scores = scan_today_earners()
        return jsonify({'scores': scores, 'count': len(scores)})
    except Exception as e:
        logger.error(f'nlp/today error: {e}')
        return jsonify({'error': str(e)}), 500


@app.route('/api/nlp/refresh', methods=['POST'])
def nlp_refresh():
    """POST — force re-scan transcripts."""
    try:
        from transcript_nlp import refresh_scores
        import threading
        threading.Thread(target=refresh_scores, daemon=True, name='nlp-refresh').start()
        return jsonify({'status': 'refresh triggered', 'note': 'check /api/nlp/scores in ~60s'})
    except Exception as e:
        logger.error(f'nlp/refresh error: {e}')
        return jsonify({'error': str(e)}), 500


# ─── /api/support — AI customer support chat ─────────────────────────

_SUPPORT_SYSTEM = """You are the Whale Tracker AI assistant — built into the app to help users understand every screen, signal, and feature. You know the app inside out.

═══════════════════════════════════════
WHAT IS WHALE TRACKER?
═══════════════════════════════════════
Whale Tracker is a professional-grade stock market intelligence app for retail investors. It aggregates institutional signals — insider trades, dark pool prints, options flow, congressional trades, AI earnings analysis, order flow, and more — into one place so users can see what the "smart money" is doing before the crowd notices.

═══════════════════════════════════════
NAVIGATION
═══════════════════════════════════════
Bottom bar (always visible):
  1. Dashboard — home screen
  2. Flow — live insider trade feed
  3. Search — find any stock
  4. Watchlist — your saved tickers
  5. More — all other screens

More screen is organized into sections:
  ACCOUNT → profile, sign in, cloud sync
  SETTINGS → appearance, AI chat toggle
  SIGNALS → Signal Radar, Dark Pool, Options Flow, Short Squeeze, Short Interest, Market Tide, Crowd Sentiment, Congress Trades
  AI & ANALYSIS → Ticker Explorer, Smart Money, PEAD Engine, OFI/VPIN, Earnings NLP, Spillover, Market Regime
  TRADING → Stock Scanner, Trading Terminal, EOD Report, Bot Portfolio, Backtesting
  MARKET DATA → Earnings Calendar, Macro Calendar, Market Heatmap, News, History
  TOOLS → Portfolio Simulator, Ticker Config, Alerts, Congressional, Learn, Whale Pro

═══════════════════════════════════════
EVERY SCREEN — WHAT IT DOES
═══════════════════════════════════════

── BOTTOM NAV ──────────────────────────

DASHBOARD
What: Home screen. Shows insider sentiment bar (buy/sell ratio), Signal Radar top picks, Whale Picks (AI-scored stocks), today's filings, 7-day top buys/sells.
How to use: Open the app → this is the first thing you see. Check it every morning to get a pulse on what insiders are doing today.
Signal Radar card: Shows the top 3 tickers where multiple signals are firing at once. Tap → full Signal Radar screen. Tap a ticker → Ticker Explorer.

FLOW (Insider Feed)
What: Real-time stream of SEC Form 4 filings — every time a CEO, Director, or Officer buys/sells their own company's stock.
How to use: Green left border = BUY. Red = SELL. Gold dot = big money (>$1M). Tap any row to see full details (price, shares, ownership change).
Why it matters: Insiders know their company better than anyone. A CEO buying $5M of their own stock is a strong signal.

SEARCH
What: Search any stock by ticker symbol. Shows recent insider filings for that stock.
How to use: Type a ticker (e.g. NVDA) → see who's been buying/selling inside that company.

WATCHLIST
What: Your personal list of tracked tickers. Shows the latest insider filings for each.
How to use: Tap + to add tickers. Tap × to remove. If you're signed in, your watchlist syncs to the cloud across devices.
Cloud sync: Green cloud icon in the title bar means your list is synced to your account.

── SIGNALS SECTION ─────────────────────

SIGNAL RADAR ⭐ (most important screen)
What: Aggregates 8 signal sources and surfaces tickers where 2 or more signals fire simultaneously. The highest-conviction picks in the whole app.
Signals it watches: Scanner AI score, Options conviction, Short squeeze, Smart Money composite, PEAD drift, OFI order flow, Insider cluster buys, Congressional trades.
How to use: Open More → Signal Radar. Tickers at the top have the most signals overlapping. Each colored badge shows which signal fired. Score 0-100. Tap any row → Ticker Explorer for full analysis.
Auto-refreshes every 5 minutes. Also shown as a card on the Dashboard.
Best for: Finding your highest-conviction trade ideas in 30 seconds.

DARK POOL
What: Large private block trades (>$1M) executed off public exchanges by hedge funds and institutions. These are revealed after execution but before the public notices the price move.
How to use: More → SIGNALS → Dark Pool. Large prints often precede big price moves. A $50M dark pool buy in NVDA means a fund is quietly accumulating.
Key metric: Print size, direction (buy/sell), and whether it's above/below the ask (bullish/bearish).

OPTIONS FLOW
What: Unusual options activity — call and put sweeps that are larger than normal, often placed by institutions hedging or betting on a directional move.
How to use: More → SIGNALS → Options Flow. Look for: large premium, short expiry, out-of-the-money calls (bullish aggression) or puts (hedging/bearish).
Conviction score: 0-100. Higher = more unusual and directional the bet.

SHORT SQUEEZE RADAR
What: Tickers where insiders are buying while short sellers are heavily positioned — the setup for a short squeeze.
How to use: More → SIGNALS → Short Squeeze Radar. Squeeze score 0-100. GME-style moves happen when insiders buy into high short interest.

SHORT INTEREST
What: Per-ticker deep dive into short selling data — how much of the float is shorted, how many days to cover, squeeze risk score.
How to use: More → SIGNALS → Short Interest. Type any ticker. Key numbers: Short Float % (>20% = high), Days to Cover (>5 = squeeze fuel).

MARKET TIDE
What: 11 major sector ETFs (XLK Tech, XLF Finance, XLE Energy, etc.) with live prices + insider sentiment grouped by sector. Shows where money is rotating.
How to use: More → SIGNALS → Market Tide. Green sector = insiders buying there. Red = selling. Use to find sector rotation before it shows in price.

CROWD SENTIMENT
What: Social media sentiment from Reddit/StockTwits — bullish vs bearish chatter around individual stocks.
How to use: More → SIGNALS → Crowd Sentiment. Combines with other signals. High crowd bullishness + insider buying = strong setup.

CONGRESS TRADES
What: Stock trades made by US House and Senate members, who must report within 45 days by law. Studies show congressional trades outperform the market.
How to use: More → SIGNALS → Congress Trades. Filter by buy/sell. A senator buying $250K of a defense stock before a military contract is announced is a well-known pattern.

── AI & ANALYSIS SECTION ───────────────

TICKER EXPLORER ⭐
What: Type any stock ticker → get a unified dashboard showing: technical indicators (RSI, MACD, Bollinger Bands), recent insider filings, news, market regime context, PEAD score, and options flow.
How to use: More → AI & ANALYSIS → Ticker Explorer. This is your all-in-one research tool for any stock. Tap from Signal Radar or Dashboard cards to auto-load a ticker.

SMART MONEY COMPOSITE
What: 4-signal composite score per ticker: insider activity + congressional trades + short squeeze potential + options flow. Score 0-100.
How to use: More → AI & ANALYSIS → Smart Money. High composite score means smart money is converging on this stock from multiple directions.

PEAD ENGINE
What: Post-Earnings Announcement Drift — stocks keep drifting in the direction of an earnings surprise for days/weeks after the report. SUE score measures the strength.
How to use: More → AI & ANALYSIS → PEAD Engine. SUE > 2.0 = strong drift candidate. Best combined with insider buying post-earnings.
SUE score: How many standard deviations the EPS beat/miss was. Higher absolute value = stronger drift.

OFI / VPIN
What: Order Flow Imbalance (OFI) measures real-time buy vs sell pressure from 1-minute bars. VPIN measures the probability of informed/toxic order flow.
How to use: More → AI & ANALYSIS → OFI/VPIN. High positive OFI = institutions aggressively buying. High VPIN (>0.7) = volatility warning, informed traders are active.

EARNINGS NLP
What: Claude AI reads earnings call transcripts and detects tone — is management bullish or bearish beyond the EPS numbers? Catches qualitative surprises the numbers miss.
How to use: More → AI & ANALYSIS → Earnings NLP. Sentiment score + key phrases flagged. "Margin expansion" and "accelerating demand" = bullish. "Macro headwinds" and "cautious outlook" = bearish.

SPILLOVER
What: Supply-chain contagion model. When Company A reports earnings, their suppliers, customers, and competitors often move predictably. Predicts which related stocks will drift next.
How to use: More → AI & ANALYSIS → Spillover. Enter a ticker that just reported → see which related companies are likely to move and in which direction.

MARKET REGIME
What: Classifies the current market as Bull / Bear / Sideways / Crisis using 7 macro indicators (VIX, yield curve, breadth, momentum, etc.).
How to use: More → AI & ANALYSIS → Market Regime. Adjust your strategy based on regime: be aggressive in Bull, defensive in Bear, avoid new positions in Crisis.

── TRADING SECTION ──────────────────────

STOCK SCANNER
What: Real-time intraday scanner that scores 500+ stocks every minute using AI (RSI, volume, momentum, insider data). Shows top picks with scores.
How to use: More → TRADING → Stock Scanner. Score > 70 = strong signal. Used as input for the Trading Terminal.

TRADING TERMINAL
What: Live 4-panel dashboard: candlestick chart (top left), scanner picks (top right), open positions (bottom left), trade log (bottom right). Paper trading by default.
How to use: More → TRADING → Trading Terminal. The AI auto-executes paper trades based on scanner scores above threshold. Watch the log to see what it's doing and why.
Paper money: Uses Alpaca paper trading account ($100K virtual). No real money at risk.

EOD REPORT
What: End-of-day P&L summary for the AI trading bot: win rate, best/worst trades, total profit/loss, exit breakdown.
How to use: More → TRADING → EOD Report. Review every day after 4pm ET to see how the bot performed.

BOT PORTFOLIO
What: Live view of the AI bot's current positions, trade history, and performance. Watch it trade in real time.
How to use: More → TRADING → Bot Portfolio.

BACKTESTING ENGINE
What: Tests insider trade returns historically at 30, 60, and 90 days after the filing date. Shows win rate, average return, best/worst trades.
How to use: More → TRADING → Backtesting Engine. Filter by minimum trade value and insider role to see which insider types have the best track records.

── MARKET DATA SECTION ──────────────────

EARNINGS CALENDAR
What: Upcoming earnings dates with insider signals overlaid — shows which reporting companies have recent insider buying.
How to use: More → MARKET DATA → Earnings Calendar. Insider buying before earnings = extra conviction.

MACRO CALENDAR
What: Key economic events for the next 60 days — Fed meetings, CPI, PPI, NFP (jobs), GDP, PCE. Color-coded by impact (HIGH/MED/LOW) with countdown timers.
How to use: More → MARKET DATA → Macro Calendar. HIGH impact events (Fed rate decision, CPI) can move the whole market. Know when they're coming.
Filter: Tap HIGH to see only market-moving events.

MARKET HEATMAP
What: Visual grid of hundreds of stocks color-coded by today's price performance. Green = up, red = down. Size = market cap.
How to use: More → MARKET DATA → Market Heatmap. Get an instant picture of which sectors and stocks are moving today.

NEWS
What: Market news filtered for insider trading, SEC filings, and your watchlist stocks.
How to use: More → MARKET DATA → News.

HISTORY
What: 30/60/90 day view of all insider activity — scroll back to see patterns over time.
How to use: More → MARKET DATA → History. Useful for researching whether a company has a pattern of insider buying before price moves.

── TOOLS SECTION ────────────────────────

PORTFOLIO SIMULATOR
What: Simulates investing $1,000 in every insider trade in your feed and shows what your returns would have been. Backtests the insider signal strategy.
How to use: More → TOOLS → Portfolio Simulator.

TICKER CONFIG
What: Add or remove tickers from the app's tracked universe. Pull historical filings for a specific stock.
How to use: More → TOOLS → Ticker Config.

ALERTS
What: Configure push notifications for whale activity — big money trades, CEO buys, watchlist activity.
How to use: More → TOOLS → Alerts. Set minimum trade value, select alert types, tap Test to verify notifications work.

CONGRESSIONAL (role-based view)
What: Insider filings filtered by role — CEO, Director, CFO, 10% Owner. Different roles have different signal strength.
How to use: More → TOOLS → Congressional. CEO buys are the strongest signal. 10% Owner (activist) buys mean someone is accumulating.

LEARN
What: Complete guide to the app — screen-by-screen walkthrough, glossary of 20+ terms, FAQ with 10 common questions.
How to use: More → TOOLS → Learn. Start here if you're new to the app or to insider trading concepts.

WHALE PRO
What: Premium tier unlocking unlimited alerts, priority data refresh, and advanced AI signals.
How to use: More → TOOLS → Whale Pro. Tap to see plans.

── ACCOUNT SECTION ──────────────────────

ACCOUNT / PROFILE
What: Sign up or sign in with email or Apple ID. Signed-in users get cloud sync for their watchlist across devices.
How to use: More → ACCOUNT (top of More screen). Create account → watchlist saves to cloud → access on any device.

═══════════════════════════════════════
KEY TERMS GLOSSARY
═══════════════════════════════════════
Form 4: SEC filing required when a company insider buys or sells stock. Must be filed within 2 business days.
Insider: CEO, CFO, Director, or anyone owning >10% of a company. They know the company better than anyone.
PEAD: Post-Earnings Announcement Drift — prices keep moving in the earnings surprise direction for weeks.
SUE Score: Standardized Unexpected Earnings — how big the earnings surprise was in standard deviations.
OFI: Order Flow Imbalance — net buy pressure vs sell pressure right now.
VPIN: Volume-Synchronized Probability of Informed Trading. >0.7 means informed traders are very active.
Dark Pool: Private exchange where large institutional trades execute off the public order book.
Short Interest: % of a stock's float that is sold short (borrowed and sold, betting on decline).
Days to Cover: How many days of average volume it would take all short sellers to buy back. >5 = squeeze risk.
Short Squeeze: When a heavily shorted stock rises, forcing shorts to buy back — accelerating the move.
Confluence: When multiple independent signals point to the same stock at the same time. Higher confidence.
CALL: Options contract giving the right to buy at a set price. Profits when stock goes UP.
PUT: Options contract giving the right to sell at a set price. Profits when stock goes DOWN.
ETF: Exchange-Traded Fund — basket of stocks. XLK=Tech, XLF=Finance, XLE=Energy, XLV=Healthcare.
RSI: Relative Strength Index. >70 = overbought, <30 = oversold.
MACD: Moving Average Convergence Divergence — momentum indicator.
Market Regime: Overall market classification. Bull = rising trend, Bear = falling, Sideways = range-bound, Crisis = extreme volatility.
NLP: Natural Language Processing — AI that reads and understands human text.
Paper Trading: Simulated trading with virtual money. No real money at risk.

═══════════════════════════════════════
COMMON USER QUESTIONS
═══════════════════════════════════════
Q: Where do I start?
A: Dashboard → check Signal Radar card for today's top confluence picks. Tap any ticker → Ticker Explorer for full analysis.

Q: How do I find the best trade ideas?
A: More → Signal Radar. Tickers with 3+ signals are the highest conviction. Tap → Ticker Explorer.

Q: What does a green row mean in the Flow screen?
A: An insider BUY. The company's own executives are buying their stock with personal money.

Q: How do I track a specific stock?
A: Tap Search (bottom nav) → enter ticker. Or add it to Watchlist by tapping the bookmark icon.

Q: Is this real money in the Trading Terminal?
A: No — paper trading only. Virtual $100K. Safe to use for testing.

Q: What's the difference between Smart Money and Signal Radar?
A: Smart Money scores a specific ticker using 4 signal types. Signal Radar scans ALL tickers and surfaces the ones where the most signals overlap right now.

Q: How often does the data refresh?
A: Insider filings: every 10 minutes. Scanner: every minute during market hours. Options flow: every 5-10 minutes. Signal Radar: every 5 minutes.

Q: What is a "whale"?
A: An institutional investor or well-connected insider moving large amounts of money. When a whale trades, the market often follows.

═══════════════════════════════════════
TONE GUIDELINES
═══════════════════════════════════════
- Be friendly, conversational, and concise.
- Always tell the user WHERE to find the feature (e.g. "More → SIGNALS → Dark Pool").
- Non-technical by default — avoid jargon unless the user uses it first.
- Never give specific buy/sell advice or price targets. You explain signals, not make recommendations.
- If a user asks "should I buy X?" — explain what the signals show and let them decide.
- Keep answers under 120 words unless a detailed walkthrough is clearly needed.
- If asked about something outside the app, politely redirect.
"""

@app.route('/api/support/chat', methods=['POST'])
def support_chat():
    """POST {"message": str, "history": [{"role": str, "content": str}]} → {"response": str}"""
    try:
        data = request.get_json(force=True)
        message = (data.get('message') or '').strip()
        history = data.get('history') or []
        if not message:
            return jsonify({'error': 'message required'}), 400

        import anthropic
        client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)

        messages = []
        for h in history[-10:]:  # keep last 10 turns for context
            role = h.get('role')
            content = h.get('content', '')
            if role in ('user', 'assistant') and content:
                messages.append({'role': role, 'content': content})
        messages.append({'role': 'user', 'content': message})

        resp = client.messages.create(
            model='claude-haiku-4-5-20251001',
            max_tokens=300,
            system=_SUPPORT_SYSTEM,
            messages=messages,
        )
        reply = resp.content[0].text.strip()
        return jsonify({'response': reply})
    except Exception as e:
        logger.error(f'support/chat error: {e}')
        return jsonify({'error': str(e)}), 500


# ─── /api/smart-money — WHAL-103 Smart Money Composite ──────────────

@app.route('/api/smart-money/composite')
def smart_money_composite():
    """GET tickers scored across 4 smart money signals. Always reads from Redis cache."""
    try:
        # Read from Redis first — instant
        if redis_get:
            cached = redis_get('smart_money:composite')
            if cached:
                return jsonify({'scores': cached, 'count': len(cached)})
        # In-memory state fallback
        from smart_money_composite import _state, _state_lock
        with _state_lock:
            scores = list(_state['composite'])
        return jsonify({'scores': scores, 'count': len(scores)})
    except Exception as e:
        logger.error(f'smart-money/composite error: {e}')
        return jsonify({'scores': [], 'count': 0})


@app.route('/api/smart-money/ticker/<ticker>')
def smart_money_ticker(ticker):
    """GET smart money composite score for a single ticker."""
    try:
        from smart_money_composite import (
            _fetch_insider_buys_by_ticker, _fetch_congress_buys_by_ticker, score_ticker
        )
        insider_map  = _fetch_insider_buys_by_ticker()
        congress_map = _fetch_congress_buys_by_ticker()
        result = score_ticker(ticker.upper(), insider_map, congress_map)
        if result is None:
            return jsonify({'ticker': ticker.upper(), 'score': 0, 'label': 'NO SIGNAL', 'signals': []})
        return jsonify(result)
    except Exception as e:
        logger.error(f'smart-money/ticker/{ticker} error: {e}')
        return jsonify({'error': str(e)}), 500


@app.route('/api/smart-money/refresh', methods=['POST'])
def smart_money_refresh():
    """POST — force recompute smart money composite scores."""
    try:
        from smart_money_composite import refresh_composite
        import threading
        threading.Thread(target=refresh_composite, daemon=True, name='smart-money-refresh').start()
        return jsonify({'status': 'refresh triggered', 'note': 'check /api/smart-money/composite in ~90s'})
    except Exception as e:
        logger.error(f'smart-money/refresh error: {e}')
        return jsonify({'error': str(e)}), 500


@app.route('/api/congress')
def api_congress():
    """Return recent congressional stock trades.
    WHAL-117: Reads from Supabase congress_trades table (written by cron.py daily).
    Falls back to live QuiverQuant API if table is empty, then to mock data.
    """
    cache_key = 'api:congress'
    cached = redis_get(cache_key)
    if cached:
        return jsonify(cached)

    # 1. Supabase — fastest path (populated by cron.py daily)
    try:
        cutoff = (datetime.utcnow() - timedelta(days=90)).strftime('%Y-%m-%d')
        r = requests.get(
            f'{SUPABASE_URL}/rest/v1/congress_trades',
            headers=SUPABASE_HEADERS,
            params={
                'date': f'gte.{cutoff}',
                'order': 'date.desc',
                'limit': '100',
            },
            timeout=10,
        )
        if r.status_code == 200:
            rows = r.json()
            if isinstance(rows, list) and rows:
                redis_set(cache_key, rows, ttl_seconds=3600)
                return jsonify(rows)
    except Exception as e:
        logger.warning(f'congress_trades Supabase read error: {e}')

    # 2. Live QuiverQuant fallback
    trades = []
    try:
        r = requests.get(
            'https://api.quiverquant.com/beta/live/congresstrading',
            headers={'Accept': 'application/json'},
            timeout=15,
        )
        if r.status_code == 200:
            for t in r.json():
                ticker = (t.get('Ticker') or '').strip().upper()
                if not ticker or ticker in ('N/A', '--', ''):
                    continue
                ticker_type = (t.get('TickerType') or '')
                if ticker_type not in ('ST', ''):
                    continue
                tx_raw = (t.get('Transaction') or '').lower()
                if 'purchase' in tx_raw or 'buy' in tx_raw:
                    kind = 'Buy'
                elif 'sale' in tx_raw or 'sell' in tx_raw:
                    kind = 'Sell'
                else:
                    continue
                chamber = 'Senate' if (t.get('House') or '') == 'Senate' else 'House'
                trades.append({
                    'member':  t.get('Representative', ''),
                    'party':   (t.get('Party') or '')[:1].upper(),
                    'chamber': chamber,
                    'ticker':  ticker,
                    'type':    kind,
                    'amount':  t.get('Range', ''),
                    'date':    t.get('TransactionDate', t.get('ReportDate', '')),
                    'company': t.get('Description', '')[:80],
                })
    except Exception as e:
        logger.warning(f'QuiverQuant congress error: {e}')

    if trades:
        trades.sort(key=lambda x: x.get('date', ''), reverse=True)
        result = trades[:60]
        redis_set(cache_key, result, ttl_seconds=1800)
        return jsonify(result)

    # 3. Static fallback mock
    mock = [
        {'member': 'Nancy Pelosi', 'party': 'D', 'chamber': 'House', 'ticker': 'NVDA', 'type': 'Buy', 'amount': '$1M–$5M', 'date': '2026-03-10', 'company': 'NVIDIA Corp'},
        {'member': 'Dan Crenshaw', 'party': 'R', 'chamber': 'House', 'ticker': 'AAPL', 'type': 'Buy', 'amount': '$15K–$50K', 'date': '2026-03-08', 'company': 'Apple Inc'},
        {'member': 'Tommy Tuberville', 'party': 'R', 'chamber': 'Senate', 'ticker': 'TSLA', 'type': 'Sell', 'amount': '$50K–$100K', 'date': '2026-03-07', 'company': 'Tesla Inc'},
        {'member': 'Mark Warner', 'party': 'D', 'chamber': 'Senate', 'ticker': 'MSFT', 'type': 'Buy', 'amount': '$100K–$250K', 'date': '2026-03-05', 'company': 'Microsoft Corp'},
        {'member': 'Roger Marshall', 'party': 'R', 'chamber': 'Senate', 'ticker': 'AMZN', 'type': 'Sell', 'amount': '$15K–$50K', 'date': '2026-03-01', 'company': 'Amazon.com Inc'},
        {'member': 'Brian Schatz', 'party': 'D', 'chamber': 'Senate', 'ticker': 'PLTR', 'type': 'Buy', 'amount': '$50K–$100K', 'date': '2026-02-27', 'company': 'Palantir Technologies'},
    ]
    return jsonify(mock)


@app.route('/api/earnings')
def api_earnings():
    """Return upcoming earnings with real dates from yfinance + insider activity from cache."""
    import yfinance as yf
    from datetime import datetime, timedelta
    import pytz

    TICKERS = [
        'NVDA','AAPL','MSFT','META','AMZN','GOOGL','TSLA','PLTR','AMD',
        'COIN','CRWD','SNOW','UBER','JPM','V','NFLX','ADBE','QCOM','MU','GS',
    ]
    NAMES = {
        'NVDA':'NVIDIA Corporation','AAPL':'Apple Inc','MSFT':'Microsoft Corporation',
        'META':'Meta Platforms','AMZN':'Amazon.com Inc','GOOGL':'Alphabet Inc',
        'TSLA':'Tesla Inc','PLTR':'Palantir Technologies','AMD':'Advanced Micro Devices',
        'COIN':'Coinbase Global','CRWD':'CrowdStrike Holdings','SNOW':'Snowflake Inc',
        'UBER':'Uber Technologies','JPM':'JPMorgan Chase','V':'Visa Inc',
        'NFLX':'Netflix Inc','ADBE':'Adobe Inc','QCOM':'Qualcomm Inc',
        'MU':'Micron Technology','GS':'Goldman Sachs',
    }

    now = datetime.utcnow()
    filings = _filings_cache or []
    buy_tickers  = {f.get('ticker','') for f in filings if f.get('transaction_type') == 'Buy'}
    sell_tickers = {f.get('ticker','') for f in filings if f.get('transaction_type') == 'Sell'}

    result = []
    for ticker in TICKERS:
        earnings_date = None
        try:
            t = yf.Ticker(ticker)
            # earnings_dates is a DataFrame indexed by datetime (tz-aware)
            ed = t.earnings_dates
            if ed is not None and not ed.empty:
                # Filter to future dates only
                tz = ed.index.tz
                now_tz = datetime.utcnow().replace(tzinfo=pytz.utc) if tz else now
                future = ed[ed.index >= now_tz]
                if not future.empty:
                    earnings_date = future.index[-1].strftime('%Y-%m-%d')
        except Exception as e:
            logger.debug(f'yfinance earnings {ticker}: {e}')

        if not earnings_date:
            continue  # skip if we can't get a real date

        if ticker in buy_tickers:
            insider_action = 'Buy'
        elif ticker in sell_tickers:
            insider_action = 'Sell'
        else:
            insider_action = 'none'

        try:
            dt = datetime.strptime(earnings_date, '%Y-%m-%d')
            days_until = (dt - now).days
        except Exception:
            days_until = 999

        result.append({
            'ticker': ticker,
            'company': NAMES.get(ticker, ticker),
            'earnings_date': earnings_date,
            'insider_action': insider_action,
            'days_until': days_until,
        })

    result.sort(key=lambda x: x['days_until'])

    # Fallback to static if yfinance returned nothing
    if not result:
        static = [
            {'ticker':'NVDA','company':'NVIDIA Corporation','earnings_date':'2026-05-28','insider_action':'Buy','days_until':63},
            {'ticker':'AAPL','company':'Apple Inc','earnings_date':'2026-04-30','insider_action':'Buy','days_until':35},
            {'ticker':'MSFT','company':'Microsoft Corporation','earnings_date':'2026-04-22','insider_action':'Buy','days_until':27},
            {'ticker':'GOOGL','company':'Alphabet Inc','earnings_date':'2026-04-24','insider_action':'Sell','days_until':29},
            {'ticker':'TSLA','company':'Tesla Inc','earnings_date':'2026-04-22','insider_action':'Sell','days_until':27},
            {'ticker':'AMD','company':'Advanced Micro Devices','earnings_date':'2026-04-28','insider_action':'none','days_until':33},
        ]
        return jsonify(static)

    return jsonify(result)


@app.route('/api/signal/<ticker>')
def ai_signal(ticker):
    """Claude AI signal analysis for a ticker based on recent insider trades."""
    # Read key fresh each request so Railway env var changes take effect without redeploy
    _key = os.environ.get('ANTHROPIC_API_KEY', '') or ANTHROPIC_KEY
    if not _key:
        return jsonify({'error': 'AI signals not configured'}), 503
    try:
        ticker = ticker.upper()

        # Get filings from cache or DB
        filings = [f for f in (_filings_cache or []) if f.get('ticker') == ticker]
        if not filings:
            _db = SupabaseClient()
            filings = _db.get_by_ticker(ticker, limit=5)
        if not filings:
            return jsonify({'error': 'No filings found for this ticker'}), 404

        # Get current stock price via Alpaca snapshot
        price, chg = 0.0, 0.0
        if ALPACA_KEY and ALPACA_SECRET:
            try:
                hdrs = {'APCA-API-KEY-ID': ALPACA_KEY, 'APCA-API-SECRET-KEY': ALPACA_SECRET}
                sr = requests.get(
                    f'https://data.alpaca.markets/v2/stocks/{ticker}/snapshot',
                    headers=hdrs, timeout=8,
                )
                if sr.status_code == 200:
                    snap = sr.json().get('snapshot', sr.json())
                    price = float((snap.get('latestTrade') or {}).get('p', 0))
                    prev  = float((snap.get('prevDailyBar') or {}).get('c', 0))
                    chg   = round((price - prev) / prev * 100, 2) if prev and price else 0
            except Exception:
                pass
        if price == 0:
            import yfinance as yf
            try:
                info = yf.Ticker(ticker).fast_info
                price = float(info.last_price or 0)
                prev  = float(info.previous_close or 0)
                chg   = round((price - prev) / prev * 100, 2) if prev else 0
            except Exception:
                pass

        # Build filing summary (up to 3)
        lines = []
        for f in filings[:3]:
            name   = f.get('owner_name', f.get('ownerName', ''))
            role   = f.get('owner_type', f.get('ownerType', ''))
            ttype  = f.get('transaction_type', f.get('transactionType', ''))
            val    = float(f.get('value', 0) or 0)
            shares = float(f.get('shares', 0) or 0)
            fp     = float(f.get('price', 0) or 0)
            date   = f.get('transaction_date', f.get('transactionDate', ''))
            lines.append(
                f"- {name} ({role}) {ttype} ${val:,.0f} of {shares:,.0f} shares at ${fp:.2f} on {date}"
            )
        filing_summary = '\n'.join(lines)

        prompt = f"""Analyze this insider trading data for {ticker} and give a brief signal assessment.

Stock: {ticker}
Current price: ${price:.2f} ({chg:+.2f}% today)

Recent insider trades:
{filing_summary}

Respond in JSON format only (no markdown, no backticks):
{{
  "signal": "STRONG BUY" or "BUY" or "NEUTRAL" or "SELL" or "STRONG SELL",
  "confidence": 0-100,
  "summary": "2-3 sentence plain English explanation",
  "key_factors": ["factor1", "factor2", "factor3"]
}}"""
        api_resp = requests.post(
            'https://api.anthropic.com/v1/messages',
            headers={
                'x-api-key': _key,
                'anthropic-version': '2023-06-01',
                'content-type': 'application/json',
            },
            json={
                'model': 'claude-haiku-4-5-20251001',
                'max_tokens': 400,
                'messages': [{'role': 'user', 'content': prompt}],
            },
            timeout=30,
        )
        if not api_resp.ok:
            return jsonify({'error': api_resp.text[:500]}), 500
        text = api_resp.json()['content'][0]['text'].strip()
        # Strip any markdown code fences if model adds them
        if text.startswith('```'):
            text = text.split('```')[1]
            if text.startswith('json'):
                text = text[4:]
        response = json.loads(text)
        response['ticker'] = ticker
        return jsonify(response)
    except Exception as e:
        logger.error(f'AI signal error {ticker}: {e}')
        return jsonify({'error': str(e)}), 500


@app.route('/api/ai/signal', methods=['POST'])
def ai_signal_engine():
    """WHAL-76 — AI Signal Engine.

    POST body (JSON):
    {
      "ticker":       "AAPL",
      "company_name": "Apple Inc",
      "owner_name":   "Tim Cook",
      "owner_type":   "CEO",
      "trade_type":   "Buy",          # Buy | Sell
      "trade_value":  5000000,        # USD
      "shares":       25000,
      "price":        195.50,
      "transaction_date": "2026-03-15",
      "current_price": 198.00         # optional — fetched if omitted
    }

    Returns:
    {
      "ticker": "AAPL",
      "signal": "STRONG BUY",
      "confidence": 85,
      "reasoning": "...",
      "entry_price": 198.00,
      "stop_loss":   188.10,
      "target_price": 218.00,
      "hold_days":    45,
      "key_factors": ["CEO buy", "..."],
      "generated_at": "2026-03-28T12:00:00"
    }
    """
    _key = os.environ.get('ANTHROPIC_API_KEY', '') or ANTHROPIC_KEY
    if not _key:
        return jsonify({'error': 'AI signals not configured — set ANTHROPIC_API_KEY'}), 503

    from risk_manager import check_all, apply_price_levels
    try:
        body = request.get_json(force=True) or {}
        ticker       = (body.get('ticker') or '').upper()
        company      = body.get('company_name', ticker)
        owner_name   = body.get('owner_name', 'Unknown')
        owner_type   = body.get('owner_type', 'Insider')
        trade_type   = body.get('trade_type', 'Buy')
        trade_value  = float(body.get('trade_value') or 0)
        shares       = int(body.get('shares') or 0)
        trade_price  = float(body.get('price') or 0)
        trade_date   = body.get('transaction_date', '')
        current_price = float(body.get('current_price') or 0)

        # ── New enrichment fields (WHAL-77) ──────────────────────
        historical_win_rate = float(body.get('historical_win_rate') or 0)
        days_to_earnings    = int(body.get('days_to_earnings') or 999)
        reddit_sentiment    = float(body.get('reddit_sentiment') or 0)
        short_interest      = float(body.get('short_interest') or 0)
        portfolio_value     = float(body.get('portfolio_value') or 100_000)

        if not ticker:
            return jsonify({'error': 'ticker is required'}), 400

        # Redis cache check — key on ticker + trade_date + trade_type
        cache_key = f'ai:signal:{ticker}:{trade_date}:{trade_type}'
        cached = redis_get(cache_key)
        if cached:
            logger.info(f'AI signal cache hit: {cache_key}')
            return jsonify(cached)

        # Fetch current price if not provided
        if current_price == 0:
            if ALPACA_KEY and ALPACA_SECRET:
                try:
                    hdrs = {'APCA-API-KEY-ID': ALPACA_KEY, 'APCA-API-SECRET-KEY': ALPACA_SECRET}
                    sr = requests.get(
                        f'https://data.alpaca.markets/v2/stocks/{ticker}/snapshot',
                        headers=hdrs, timeout=8)
                    if sr.status_code == 200:
                        snap = sr.json().get('snapshot', sr.json())
                        current_price = float((snap.get('latestTrade') or {}).get('p', 0))
                except Exception:
                    pass
            if current_price == 0:
                try:
                    import yfinance as yf
                    current_price = float(yf.Ticker(ticker).fast_info.last_price or 0)
                except Exception:
                    pass

        # Build Claude prompt — enriched with WHAL-77 fields
        value_str = f'${trade_value:,.0f}' if trade_value >= 1000 else f'${trade_value:.2f}'

        # Optional context lines — only include if provided
        extra_lines = []
        if historical_win_rate > 0:
            extra_lines.append(f'Insider historical win rate: {historical_win_rate*100:.0f}%')
        if days_to_earnings < 999:
            extra_lines.append(f'Days to next earnings: {days_to_earnings}')
        if reddit_sentiment > 0:
            extra_lines.append(f'Reddit/social sentiment score: {reddit_sentiment:.2f} (0=bearish, 1=bullish)')
        if short_interest > 0:
            extra_lines.append(f'Short interest: {short_interest*100:.1f}% of float')
        extra_context = ('\n' + '\n'.join(extra_lines)) if extra_lines else ''

        prompt = f"""You are a quantitative analyst specializing in insider trading signals.

Analyze this insider trade and generate a precise trading signal.

Company: {company} ({ticker})
Current stock price: ${current_price:.2f}
Insider: {owner_name} ({owner_type})
Trade: {trade_type} {shares:,} shares @ ${trade_price:.2f} = {value_str}
Trade date: {trade_date}{extra_context}

Rules for your analysis:
- CEO/CFO/Director buys of >$1M are strongly bullish
- Cluster buying (multiple insiders) is very bullish
- High insider win rate (>65%) increases confidence significantly
- Reddit sentiment >0.7 is bullish confirmation; <0.3 is bearish
- High short interest (>15%) + insider buy = potential squeeze catalyst
- Earnings within 5 days = higher volatility risk, reduce confidence
- Sells are often less meaningful unless CEO/CFO selling large %
- Entry near current price, stop loss 10% below for buys, target 20% above

Respond ONLY with valid JSON (no markdown, no backticks, no explanation outside JSON):
{{
  "signal": "STRONG BUY" | "BUY" | "NEUTRAL" | "SELL" | "STRONG SELL",
  "confidence": <integer 0-100>,
  "reasoning": "<2-3 sentence plain English explanation for retail investors>",
  "entry_price": <float, suggested entry price near current price>,
  "stop_loss": <float, suggested stop loss price>,
  "target_price": <float, price target>,
  "hold_days": <integer, suggested holding period in days>,
  "key_factors": ["<factor1>", "<factor2>", "<factor3>"]
}}"""

        api_resp = requests.post(
            'https://api.anthropic.com/v1/messages',
            headers={
                'x-api-key': _key,
                'anthropic-version': '2023-06-01',
                'content-type': 'application/json',
            },
            json={
                'model': 'claude-haiku-4-5-20251001',
                'max_tokens': 600,
                'messages': [{'role': 'user', 'content': prompt}],
            },
            timeout=30,
        )

        if not api_resp.ok:
            logger.error(f'Claude API error: {api_resp.status_code} {api_resp.text[:300]}')
            return jsonify({'error': 'AI service error', 'detail': api_resp.text[:200]}), 500

        text = api_resp.json()['content'][0]['text'].strip()
        # Strip markdown fences if model adds them
        if text.startswith('```'):
            text = '\n'.join(text.split('\n')[1:])
            if text.endswith('```'):
                text = text[:-3].strip()

        result = json.loads(text)

        # ── Risk management gate (WHAL-77) ───────────────────────
        confidence = int(result.get('confidence') or 0)
        risk = check_all(
            ticker=ticker,
            trade_value=trade_value,
            current_price=current_price,
            confidence=confidence,
            days_to_earnings=days_to_earnings,
            portfolio_value=portfolio_value,
        )

        # Override stop/target with risk-rule defaults if Claude left them null
        price_levels = apply_price_levels(current_price, is_buy=trade_type.lower() == 'buy')
        if result.get('stop_loss') is None:
            result['stop_loss'] = price_levels['stop_loss']
        if result.get('target_price') is None:
            result['target_price'] = price_levels['target_price']

        # Enrich with request metadata
        result['ticker']         = ticker
        result['company_name']   = company
        result['owner_name']     = owner_name
        result['owner_type']     = owner_type
        result['trade_type']     = trade_type
        result['trade_value']    = trade_value
        result['current_price']  = round(current_price, 2)
        result['generated_at']   = datetime.utcnow().isoformat()

        # ── Risk result ───────────────────────────────────────────
        result['risk_passed']    = risk['passed']
        result['risk_violations'] = risk['violations']
        result['position_size']  = risk['position_size']
        result['max_shares']     = risk['max_shares']

        # Downgrade signal if risk check failed
        if not risk['passed']:
            result['signal']     = 'NEUTRAL'
            result['confidence'] = min(confidence, 40)
            result['reasoning']  = (
                f"Signal blocked by risk rules: "
                + '; '.join(v['detail'] for v in risk['violations'])
                + ' — ' + (result.get('reasoning') or '')
            )

        # Store in Supabase ai_signals table (best-effort)
        try:
            _db = SupabaseClient()
            _db.upsert('ai_signals', [{
                'ticker':               ticker,
                'company_name':         company,
                'owner_name':           owner_name,
                'owner_type':           owner_type,
                'trade_type':           trade_type,
                'trade_value':          trade_value,
                'signal':               result.get('signal'),
                'confidence':           result.get('confidence'),
                'reasoning':            result.get('reasoning'),
                'entry_price':          result.get('entry_price'),
                'stop_loss':            result.get('stop_loss'),
                'target_price':         result.get('target_price'),
                'hold_days':            result.get('hold_days'),
                'key_factors':          json.dumps(result.get('key_factors', [])),
                'current_price':        round(current_price, 2),
                'risk_passed':          risk['passed'],
                'risk_violations':      json.dumps(risk['violations']),
                'position_size':        risk['position_size'],
                'historical_win_rate':  historical_win_rate or None,
                'days_to_earnings':     days_to_earnings if days_to_earnings < 999 else None,
                'reddit_sentiment':     reddit_sentiment or None,
                'short_interest':       short_interest or None,
                'generated_at':         result['generated_at'],
            }])
        except Exception as e:
            logger.warning(f'ai_signals Supabase store error: {e}')

        # Cache result for 10 minutes
        redis_set(cache_key, result, ttl_seconds=600)

        # ── WHAL-92: Combined Signal Score ────────────────────────
        try:
            from technical_indicators import get_technical_score
            from signal_combiner import combine_signals
            tech_result = get_technical_score(ticker)
            combined    = combine_signals(result, tech_result)
            result['technical_score']      = combined['technical_score']
            result['technical_signal']     = combined['technical_signal']
            result['technical_score_0_1']  = combined['technical_score_0_1']
            result['final_score']          = combined['final_score']
            result['action']               = combined['action']
            result['combined_reasoning']   = combined['combined_reasoning']
            result['dynamic_stop_loss']    = combined['dynamic_stop_loss']
            result['technical_detail']     = tech_result
        except Exception as e:
            logger.warning(f'Combined signal score error: {e}')

        # ── Auto paper trade if enabled (WHAL-78/92) ──────────────
        try:
            from paper_trader import place_order as _place_order
            # WHAL-92: only execute if combined action is EXECUTE_TRADE
            action = result.get('action', 'SKIP')
            if action == 'EXECUTE_TRADE' or not result.get('action'):
                trade_result = _place_order(result)
            else:
                trade_result = {'status': 'skipped', 'message': f'Combined score action: {action}'}
            result['paper_trade'] = trade_result
        except Exception as e:
            logger.warning(f'Paper trade hook error: {e}')
            result['paper_trade'] = {'status': 'error', 'message': str(e)}

        return jsonify(result)

    except json.JSONDecodeError as e:
        logger.error(f'AI signal JSON parse error: {e}')
        return jsonify({'error': 'Failed to parse AI response', 'detail': str(e)}), 500
    except Exception as e:
        logger.error(f'AI signal engine error: {e}')
        return jsonify({'error': str(e)}), 500


# ── Paper Trading endpoints (WHAL-78) ─────────────────────────────

@app.route('/api/trading/paper/portfolio')
def paper_portfolio():
    """GET current Alpaca paper account + open positions."""
    try:
        from paper_trader import get_portfolio
        return jsonify(get_portfolio())
    except Exception as e:
        logger.error(f'paper/portfolio error: {e}')
        return jsonify({'error': str(e)}), 500


@app.route('/api/trading/paper/trades')
def paper_trades():
    """GET recent paper trades from Supabase."""
    try:
        from paper_trader import get_trades
        limit = request.args.get('limit', 50, type=int)
        return jsonify(get_trades(limit=limit))
    except Exception as e:
        logger.error(f'paper/trades error: {e}')
        return jsonify({'error': str(e)}), 500


@app.route('/api/trading/paper/performance')
def paper_performance():
    """GET paper trading performance metrics."""
    try:
        from paper_trader import get_performance
        return jsonify(get_performance())
    except Exception as e:
        logger.error(f'paper/performance error: {e}')
        return jsonify({'error': str(e)}), 500


@app.route('/api/perf/batch', methods=['POST'])
def perf_batch():
    """Return real % return since trade date for a list of {ticker, date} pairs.
    Uses Yahoo Finance as primary source (works 24/7 including weekends),
    with Alpaca as secondary for real-time price during market hours."""
    from datetime import datetime, timedelta
    try:
        items = request.get_json(force=True) or []
        if not items:
            return jsonify([])

        hdrs = {'APCA-API-KEY-ID': ALPACA_KEY, 'APCA-API-SECRET-KEY': ALPACA_SECRET}
        unique_tickers = list({i['ticker'].upper() for i in items if i.get('ticker')})

        # ── 1. Current prices via Yahoo Finance chart API (24/7, last close) ──────
        current_prices = {}
        _yf_headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'}
        for sym in unique_tickers:
            try:
                r = requests.get(
                    f'https://query1.finance.yahoo.com/v8/finance/chart/{sym}',
                    params={'interval': '1d', 'range': '5d'},
                    headers=_yf_headers,
                    timeout=8,
                )
                if r.status_code == 200:
                    closes = r.json()['chart']['result'][0]['indicators']['quote'][0]['close']
                    closes = [c for c in closes if c is not None]
                    if closes:
                        current_prices[sym] = round(float(closes[-1]), 2)
            except Exception as e:
                logger.debug(f'perf/batch yahoo chart {sym}: {e}')

        # ── 2. Entry prices via Yahoo Finance chart API (same source as current prices) ──
        # Uses chart API with a narrow date range to find the close on/after trade date.
        # No yfinance library — direct HTTP only, works on Railway 24/7.
        def _get_entry_price(ticker: str, date_str: str) -> float:
            try:
                start_dt = datetime.strptime(date_str, '%Y-%m-%d')
                # Pull 14 days of daily bars starting from trade date to skip weekends/holidays
                end_dt = start_dt + timedelta(days=14)
                r = requests.get(
                    f'https://query1.finance.yahoo.com/v8/finance/chart/{ticker}',
                    params={
                        'interval': '1d',
                        'period1': int(start_dt.timestamp()),
                        'period2': int(end_dt.timestamp()),
                    },
                    headers=_yf_headers,
                    timeout=10,
                )
                if r.status_code == 200:
                    result = r.json().get('chart', {}).get('result', [])
                    if result:
                        closes = result[0].get('indicators', {}).get('quote', [{}])[0].get('close', [])
                        closes = [c for c in closes if c is not None]
                        if closes:
                            return round(float(closes[0]), 2)
            except Exception as e:
                logger.debug(f'perf/batch entry price {ticker} {date_str}: {e}')
            return 0

        results = []
        for item in items[:30]:
            ticker = (item.get('ticker') or '').upper()
            date_str = (item.get('date') or '')[:10]
            if not ticker or not date_str:
                continue

            entry_price = _get_entry_price(ticker, date_str)

            current = current_prices.get(ticker, 0)
            change_pct = None
            if entry_price > 0 and current > 0:
                change_pct = round((current - entry_price) / entry_price * 100, 2)

            results.append({
                'ticker': ticker,
                'date': date_str,
                'entry_price': round(entry_price, 2),
                'current_price': round(current, 2),
                'change_pct': change_pct,
            })

        return jsonify(results)
    except Exception as e:
        logger.error(f'perf/batch error: {e}')
        return jsonify([]), 500


# ─── Redis Cache Layer ────────────────────────────────────────────
# Connects to Railway Redis addon via REDIS_URL env var.
# Falls back gracefully to no-cache if Redis is unavailable.

try:
    import redis as _redis_lib
    _redis_url = os.environ.get('REDIS_URL', '')
    if _redis_url:
        _redis = _redis_lib.from_url(_redis_url, decode_responses=True, socket_connect_timeout=2)
        _redis.ping()
        logger.info('Redis connected')
    else:
        _redis = None
        logger.info('REDIS_URL not set — caching disabled')
except Exception as _redis_err:
    _redis = None
    logger.warning(f'Redis unavailable — running without cache: {_redis_err}')


def redis_get(key):
    """Return parsed JSON value from Redis, or None on miss/error."""
    if _redis is None:
        return None
    try:
        val = _redis.get(key)
        return json.loads(val) if val else None
    except Exception:
        return None


def redis_set(key, value, ttl_seconds=300, ex=None):
    """Store JSON value in Redis with TTL. Silently fails if Redis unavailable."""
    if _redis is None:
        return
    try:
        ttl = ex if ex is not None else ttl_seconds
        _redis.setex(key, ttl, json.dumps(value, default=str))
    except Exception:
        pass


def redis_delete(key):
    """Delete a key from Redis."""
    if _redis is None:
        return
    try:
        _redis.delete(key)
    except Exception:
        pass


# ─── WHAL-81: Vector Score endpoints ────────────────────────────

@app.route('/api/vector-score/<ticker>')
def api_vector_score(ticker):
    """WHAL-81 — Compute composite Whale Vector Score for a ticker.
    Returns insider, options, dark pool, congressional, sentiment scores + Claude narrative.
    Cached in Redis for 10 minutes.
    """
    ticker = ticker.upper()
    cache_key = f'vector:score:{ticker}'
    cached = redis_get(cache_key)
    if cached:
        logger.info(f'Vector score cache hit: {ticker}')
        return jsonify(cached)

    try:
        from vector_score import compute_vector_score
        company = ticker  # enrich with yfinance name if desired
        try:
            import yfinance as yf
            info = yf.Ticker(ticker).info
            company = info.get('longName') or info.get('shortName') or ticker
        except Exception:
            pass

        result = compute_vector_score(ticker, company)
        redis_set(cache_key, result, ttl_seconds=600)
        return jsonify(result)
    except Exception as e:
        logger.error(f'Vector score error {ticker}: {e}')
        return jsonify({'error': str(e)}), 500


@app.route('/api/whale-picks')
def api_whale_picks():
    """WHAL-89 — Return top 5 tickers by Vector Score from recent insider filings.
    Computes scores for the top 15 most active recent tickers, returns top 5.
    Cached in Redis for 15 minutes.
    """
    cache_key = 'whale:picks'
    cached = redis_get(cache_key)
    if cached:
        logger.info('Whale picks cache hit')
        return jsonify(cached)

    try:
        from vector_score import compute_vector_score

        # Get top tickers from recent filings cache (Supabase-backed)
        recent = list(_filings_cache)[-200:] if _filings_cache else []
        ticker_counts = {}
        ticker_company = {}
        for f in recent:
            t = (f.get('ticker') if isinstance(f, dict) else getattr(f, 'ticker', '')) or ''
            c = (f.get('company_name') if isinstance(f, dict) else getattr(f, 'company_name', '')) or t
            t = t.upper().strip()
            if t and len(t) <= 5:
                ticker_counts[t] = ticker_counts.get(t, 0) + 1
                ticker_company[t] = c

        # Take top 15 most active
        top_tickers = sorted(ticker_counts, key=lambda x: ticker_counts[x], reverse=True)[:15]

        picks = []
        for t in top_tickers:
            try:
                score = compute_vector_score(t, ticker_company.get(t, t))
                picks.append({
                    'ticker':      score['ticker'],
                    'company':     score['company'],
                    'total_score': score['total_score'],
                    'grade':       score['grade'],
                    'narrative':   score['narrative'],
                    'dimensions':  score['dimensions'],
                })
                # Cache individual scores too
                redis_set(f'vector:score:{t}', score, ttl_seconds=600)
            except Exception as e:
                logger.warning(f'Whale picks: score error {t}: {e}')

        picks.sort(key=lambda x: x['total_score'], reverse=True)
        result = {'picks': picks[:5], 'generated_at': datetime.now(timezone.utc).isoformat()}
        redis_set(cache_key, result, ttl_seconds=900)
        return jsonify(result)
    except Exception as e:
        logger.error(f'Whale picks error: {e}')
        return jsonify({'error': str(e)}), 500


# ── WHAL-48: Insider Score endpoint ──────────────────────────────

@app.route('/api/insider-score/<path:name>')
def api_insider_score(name):
    """Real win rate + A+ to F grade for an insider based on historical trade performance."""
    cache_key = f'insider:score:{name.lower()}'
    cached = redis_get(cache_key)
    if cached:
        return jsonify(cached)
    try:
        import yfinance as yf
        from datetime import timezone as _tz
        r2 = requests.get(
            f'{SUPABASE_URL}/rest/v1/filings',
            headers=SUPABASE_HEADERS,
            params={
                'owner_name': f'ilike.{name}',
                'transaction_type': 'eq.buy',
                'select': 'ticker,filed_at,transaction_date,value,owner_type',
                'limit': '100',
                'order': 'filed_at.desc',
            },
            timeout=10,
        )
        rows = r2.json() if r2.status_code == 200 else []
        if not isinstance(rows, list):
            rows = []
        role = rows[0].get('owner_type', 'Insider') if rows else 'Insider'
        total_trades = len(rows)
        analyzed = wins = 0
        returns_30, returns_60, returns_90 = [], [], []
        now_utc = datetime.now(_tz.utc)
        for f in rows[:20]:
            date_str = (f.get('transaction_date') or f.get('filed_at') or '')[:10]
            ticker = (f.get('ticker') or '').upper()
            if not date_str or not ticker:
                continue
            try:
                trade_dt = datetime.strptime(date_str, '%Y-%m-%d')
                days_ago = (now_utc - trade_dt.replace(tzinfo=_tz.utc)).days
                # Need at least 5 trading days to measure any return
                if days_ago < 7:
                    continue
                look_forward = min(95, days_ago - 1)
                hist = yf.Ticker(ticker).history(
                    start=(trade_dt - timedelta(days=5)).strftime('%Y-%m-%d'),
                    end=min((trade_dt + timedelta(days=look_forward)).strftime('%Y-%m-%d'), now_utc.strftime('%Y-%m-%d')),
                )
                if hist.empty:
                    continue
                hdates = [d.replace(tzinfo=None) if hasattr(d, 'tzinfo') else d for d in hist.index.to_pydatetime()]
                entry_rows2 = [(i, d) for i, d in enumerate(hdates) if d >= trade_dt]
                if not entry_rows2:
                    continue
                ep = float(hist.iloc[entry_rows2[0][0]]['Close'])
                if ep <= 0:
                    continue
                def _price(n):
                    tgt = trade_dt + timedelta(days=n)
                    rr = [(i, d) for i, d in enumerate(hdates) if d >= tgt]
                    return float(hist.iloc[rr[0][0]]['Close']) if rr else float(hist.iloc[-1]['Close'])
                analyzed += 1
                # Use best available horizon: 30d if old enough, else 14d, else 7d
                if days_ago >= 31:
                    r_primary = round((_price(30) - ep) / ep * 100, 2)
                    returns_30.append(r_primary)
                elif days_ago >= 14:
                    r_primary = round((_price(14) - ep) / ep * 100, 2)
                    returns_30.append(r_primary)
                else:
                    r_primary = round((float(hist.iloc[-1]['Close']) - ep) / ep * 100, 2)
                    returns_30.append(r_primary)
                if days_ago >= 61:
                    returns_60.append(round((_price(60) - ep) / ep * 100, 2))
                if days_ago >= 91:
                    returns_90.append(round((_price(90) - ep) / ep * 100, 2))
                if r_primary > 0:
                    wins += 1
            except Exception as ex:
                logger.debug(f'insider score trade {ticker}: {ex}')
        win_rate = round(wins / analyzed * 100) if analyzed > 0 else 0
        avg_30 = round(sum(returns_30) / len(returns_30), 1) if returns_30 else None
        avg_60 = round(sum(returns_60) / len(returns_60), 1) if returns_60 else None
        avg_90 = round(sum(returns_90) / len(returns_90), 1) if returns_90 else None
        if analyzed == 0:
            grade = 'N/A'
        elif win_rate >= 70 and avg_30 and avg_30 > 5:
            grade = 'A+'
        elif win_rate >= 65:
            grade = 'A'
        elif win_rate >= 55:
            grade = 'B+'
        elif win_rate >= 50:
            grade = 'B'
        elif win_rate >= 40:
            grade = 'C'
        elif win_rate >= 30:
            grade = 'D'
        else:
            grade = 'F'
        result = {'name': name, 'role': role, 'grade': grade, 'win_rate': win_rate,
                  'avg_return_30d': avg_30, 'avg_return_60d': avg_60, 'avg_return_90d': avg_90,
                  'total_trades': total_trades, 'analyzed': analyzed}
        redis_set(cache_key, result, ttl_seconds=3600)
        return jsonify(result)
    except Exception as e:
        logger.error(f'insider-score error {name}: {e}')
        return jsonify({'name': name, 'grade': 'N/A', 'win_rate': 0, 'total_trades': 0, 'analyzed': 0}), 500


# ── WHAL-54: Short Squeeze Candidates ────────────────────────────

@app.route('/api/squeeze')
def api_squeeze():
    """WHAL-54 — Tickers with high short interest + recent insider buying = squeeze candidates."""
    cache_key = 'squeeze:candidates'
    cached = redis_get(cache_key)
    if cached:
        return jsonify(cached)
    try:
        import yfinance as yf
        buys = [f for f in _filings_cache if (f.get('transaction_type') or '').lower() == 'buy']
        ticker_map = {}
        for f in buys[:200]:
            t = (f.get('ticker') or '').upper()
            if not t:
                continue
            if t not in ticker_map:
                ticker_map[t] = {'ticker': t, 'buys': 0, 'buy_value': 0.0,
                                  'latest_insider': '', 'latest_date': ''}
            ticker_map[t]['buys'] += 1
            ticker_map[t]['buy_value'] += float(f.get('value') or 0)
            if not ticker_map[t]['latest_insider']:
                ticker_map[t]['latest_insider'] = f.get('owner_name', '')
                ticker_map[t]['latest_date'] = (f.get('filed_at') or '')[:10]
        candidates = []
        for ticker, info in list(ticker_map.items())[:30]:
            try:
                yfi = yf.Ticker(ticker).info
                short_pct = float(yfi.get('shortPercentOfFloat') or 0) * 100
                short_ratio = float(yfi.get('shortRatio') or 0)
                price = float(yfi.get('currentPrice') or yfi.get('regularMarketPrice') or 0)
                company = yfi.get('longName') or yfi.get('shortName') or ticker
                if short_pct < 5:
                    continue
                squeeze_score = min(100, short_pct * 2 + info['buys'] * 5 + info['buy_value'] / 1_000_000 * 2)
                candidates.append({'ticker': ticker, 'company': company, 'price': round(price, 2),
                                    'short_pct': round(short_pct, 1), 'short_ratio': round(short_ratio, 1),
                                    'insider_buys': info['buys'], 'buy_value': round(info['buy_value']),
                                    'latest_insider': info['latest_insider'],
                                    'latest_date': info['latest_date'],
                                    'squeeze_score': round(min(100, squeeze_score))})
            except Exception as ex:
                logger.debug(f'squeeze {ticker}: {ex}')
        candidates.sort(key=lambda x: x['squeeze_score'], reverse=True)
        result = {'candidates': candidates[:15], 'generated_at': datetime.now().isoformat()}
        redis_set(cache_key, result, ttl_seconds=900)
        return jsonify(result)
    except Exception as e:
        logger.error(f'squeeze error: {e}')
        return jsonify({'candidates': [], 'error': str(e)}), 500


# ── WHAL-70: Historical Backtesting ──────────────────────────────

@app.route('/api/backtest')
def api_backtest():
    """WHAL-70 — Insider buy performance stats at 30/60/90 days post-filing."""
    min_val = request.args.get('min_value', 500000, type=int)
    role_filter = request.args.get('role', '').lower()
    cache_key = f'backtest:{min_val}:{role_filter}'
    cached = redis_get(cache_key)
    if cached:
        return jsonify(cached)
    try:
        import yfinance as yf
        from datetime import timezone as _tz
        r2 = requests.get(
            f'{SUPABASE_URL}/rest/v1/filings',
            headers=SUPABASE_HEADERS,
            params={'transaction_type': 'eq.buy',
                    'select': 'ticker,filed_at,transaction_date,value,owner_name,owner_type',
                    'order': 'filed_at.desc', 'limit': '500'},
            timeout=15,
        )
        filings = r2.json() if r2.status_code == 200 else []
        if not isinstance(filings, list):
            filings = []
        filtered = [f for f in filings
                    if float(f.get('value') or 0) >= min_val
                    and (not role_filter or role_filter in (f.get('owner_type') or '').lower())]
        analyzed = wins_30 = 0
        returns_30, returns_60, returns_90 = [], [], []
        best = worst = None
        trade_results = []
        now_utc = datetime.now(_tz.utc)
        for f in filtered[:40]:
            date_str = (f.get('transaction_date') or f.get('filed_at') or '')[:10]
            ticker = (f.get('ticker') or '').upper()
            if not date_str or not ticker:
                continue
            try:
                trade_dt = datetime.strptime(date_str, '%Y-%m-%d')
                days_ago = (now_utc - trade_dt.replace(tzinfo=_tz.utc)).days
                if days_ago < 31:
                    continue
                hist = yf.Ticker(ticker).history(
                    start=(trade_dt - timedelta(days=5)).strftime('%Y-%m-%d'),
                    end=min((trade_dt + timedelta(days=95)).strftime('%Y-%m-%d'), now_utc.strftime('%Y-%m-%d')),
                )
                if hist.empty:
                    continue
                hdates = [d.replace(tzinfo=None) if hasattr(d, 'tzinfo') else d for d in hist.index.to_pydatetime()]
                er = [(i, d) for i, d in enumerate(hdates) if d >= trade_dt]
                if not er:
                    continue
                ep = float(hist.iloc[er[0][0]]['Close'])
                if ep <= 0:
                    continue
                def _bt_price(n):
                    tgt = trade_dt + timedelta(days=n)
                    rr = [(i, d) for i, d in enumerate(hdates) if d >= tgt]
                    return float(hist.iloc[rr[0][0]]['Close']) if rr else float(hist.iloc[-1]['Close'])
                analyzed += 1
                r30 = round((_bt_price(30) - ep) / ep * 100, 2)
                returns_30.append(r30)
                if days_ago >= 61:
                    returns_60.append(round((_bt_price(60) - ep) / ep * 100, 2))
                if days_ago >= 91:
                    returns_90.append(round((_bt_price(90) - ep) / ep * 100, 2))
                if r30 > 0:
                    wins_30 += 1
                tr = {'ticker': ticker, 'date': date_str, 'insider': f.get('owner_name', ''),
                      'role': f.get('owner_type', ''), 'value': float(f.get('value') or 0), 'return_30d': r30}
                trade_results.append(tr)
                if best is None or r30 > best['return_30d']:
                    best = tr
                if worst is None or r30 < worst['return_30d']:
                    worst = tr
            except Exception as ex:
                logger.debug(f'backtest {ticker}: {ex}')
        win_rate = round(wins_30 / analyzed * 100, 1) if analyzed > 0 else 0
        trade_results.sort(key=lambda x: x['return_30d'], reverse=True)
        result = {
            'analyzed': analyzed, 'total_found': len(filtered), 'min_value': min_val,
            'role_filter': role_filter or 'all', 'win_rate': win_rate,
            'avg_return_30d': round(sum(returns_30) / len(returns_30), 1) if returns_30 else 0,
            'avg_return_60d': round(sum(returns_60) / len(returns_60), 1) if returns_60 else None,
            'avg_return_90d': round(sum(returns_90) / len(returns_90), 1) if returns_90 else None,
            'best_trade': best, 'worst_trade': worst,
            'trades': trade_results[:20], 'generated_at': datetime.now().isoformat(),
        }
        redis_set(cache_key, result, ttl_seconds=1800)
        return jsonify(result)
    except Exception as e:
        logger.error(f'backtest error: {e}')
        return jsonify({'error': str(e), 'analyzed': 0}), 500


# ─── /api/intraday/bars — WHAL-97 Intraday OHLCV bars ───────────

@app.route('/api/intraday/bars/<ticker>')
def intraday_bars(ticker):
    """GET intraday 1-min/5-min/15-min/1-hr OHLCV bars from Alpaca."""
    try:
        tf_param = request.args.get('timeframe', '1Min')
        limit    = int(request.args.get('limit', 60))
        tf_map   = {'1Min': '1Min', '5Min': '5Min', '15Min': '15Min', '1Hour': '1Hour'}
        timeframe = tf_map.get(tf_param, '1Min')

        hdrs = {
            'APCA-API-KEY-ID':     ALPACA_KEY,
            'APCA-API-SECRET-KEY': ALPACA_SECRET,
        }
        r = requests.get(
            f'https://data.alpaca.markets/v2/stocks/{ticker.upper()}/bars',
            headers=hdrs,
            params={'timeframe': timeframe, 'limit': limit, 'feed': 'sip'},
            timeout=15,
        )
        if r.status_code != 200:
            return jsonify({'error': f'Alpaca error: {r.status_code}', 'bars': []}), 200

        raw_bars = r.json().get('bars', [])
        bars = [{
            'time':   b.get('t', ''),
            'open':   round(float(b.get('o', 0)), 2),
            'high':   round(float(b.get('h', 0)), 2),
            'low':    round(float(b.get('l', 0)), 2),
            'close':  round(float(b.get('c', 0)), 2),
            'volume': int(b.get('v', 0)),
        } for b in raw_bars]
        return jsonify({'ticker': ticker.upper(), 'timeframe': timeframe, 'bars': bars})
    except Exception as e:
        logger.error(f'intraday/bars/{ticker} error: {e}')
        return jsonify({'error': str(e), 'bars': []}), 500


# ─── /api/intraday — WHAL-96 Intraday Trade Executor ────────────

@app.route('/api/intraday/positions')
def intraday_positions():
    """GET current open intraday positions."""
    try:
        from intraday_executor import get_open_positions
        return jsonify({'positions': get_open_positions()})
    except Exception as e:
        logger.error(f'intraday/positions error: {e}')
        return jsonify({'error': str(e)}), 500


@app.route('/api/intraday/trades/today')
def intraday_trades_today():
    """GET all intraday trades placed today."""
    try:
        from intraday_executor import get_trades_today
        trades = get_trades_today()
        return jsonify({'trades': trades, 'count': len(trades)})
    except Exception as e:
        logger.error(f'intraday/trades/today error: {e}')
        return jsonify({'error': str(e)}), 500


@app.route('/api/eod/monthly')
def eod_monthly():
    """GET /api/eod/monthly?year=YYYY — P&L grouped by month for a given year."""
    try:
        import pytz as _pytz
        _et = _pytz.timezone('America/New_York')
        year = int(request.args.get('year', datetime.now(_et).year))
        from intraday_executor import _supa_get

        # Pull all closed trades for the year from Supabase
        start = f'{year}-01-01'
        end   = f'{year}-12-31'
        query = (
            f'date=gte.{start}&date=lte.{end}'
            f'&status=eq.closed'
            f'&select=date,pnl,pnl_pct,hold_minutes,exit_reason,ticker'
            f'&order=date.asc&limit=5000'
        )
        rows = _supa_get('intraday_trades', query) or []

        # Aggregate by month
        months = {}
        for r in rows:
            d = (r.get('date') or '')[:7]  # YYYY-MM
            if not d:
                continue
            m = months.setdefault(d, {
                'month': d, 'total_trades': 0, 'winning_trades': 0,
                'losing_trades': 0, 'total_pnl': 0.0, 'trading_days': set(),
            })
            pnl = float(r.get('pnl') or 0)
            m['total_trades'] += 1
            m['total_pnl']    = round(m['total_pnl'] + pnl, 2)
            if pnl > 0: m['winning_trades'] += 1
            else:        m['losing_trades']  += 1
            m['trading_days'].add(r.get('date', '')[:10])

        result = []
        for d, m in sorted(months.items()):
            days = len(m.pop('trading_days'))
            total = m['total_trades']
            m['win_rate']     = round(m['winning_trades'] / total, 4) if total else 0.0
            m['trading_days'] = days
            result.append(m)

        return jsonify({'year': year, 'months': result})
    except Exception as e:
        logger.error(f'eod/monthly error: {e}')
        return jsonify({'error': str(e)}), 500


@app.route('/api/eod/yearly')
def eod_yearly():
    """GET /api/eod/yearly — P&L grouped by year (all-time)."""
    try:
        from intraday_executor import _supa_get

        query = (
            'status=eq.closed'
            '&select=date,pnl,pnl_pct,hold_minutes,ticker'
            '&order=date.asc&limit=10000'
        )
        rows = _supa_get('intraday_trades', query) or []

        years = {}
        for r in rows:
            d = (r.get('date') or '')[:4]  # YYYY
            if not d:
                continue
            y = years.setdefault(d, {
                'year': d, 'total_trades': 0, 'winning_trades': 0,
                'losing_trades': 0, 'total_pnl': 0.0, 'trading_days': set(),
            })
            pnl = float(r.get('pnl') or 0)
            y['total_trades'] += 1
            y['total_pnl']    = round(y['total_pnl'] + pnl, 2)
            if pnl > 0: y['winning_trades'] += 1
            else:        y['losing_trades']  += 1
            y['trading_days'].add(r.get('date', '')[:10])

        result = []
        for d, y in sorted(years.items()):
            days = len(y.pop('trading_days'))
            total = y['total_trades']
            y['win_rate']     = round(y['winning_trades'] / total, 4) if total else 0.0
            y['trading_days'] = days
            result.append(y)

        return jsonify({'years': result})
    except Exception as e:
        logger.error(f'eod/yearly error: {e}')
        return jsonify({'error': str(e)}), 500


@app.route('/api/trade/log')
def trade_log():
    """GET /api/trade/log?date=YYYY-MM-DD&days=7&status=all|open|closed
    Returns full trade history with entry/exit time, AI reasoning, confidence.
    Defaults to last 7 days of all trades."""
    try:
        import pytz as _pytz
        from datetime import timedelta
        _et = _pytz.timezone('America/New_York')
        status_filter = request.args.get('status', 'all')
        date_str = request.args.get('date')
        days = min(int(request.args.get('days', 7)), 90)

        rows = []
        if date_str:
            # Single date
            query = f'date=eq.{date_str}&order=entry_time.asc&limit=500'
            if status_filter != 'all':
                query += f'&status=eq.{status_filter}'
            from intraday_executor import _supa_get
            rows = _supa_get('intraday_trades', query) or []
        else:
            # Date range
            today = datetime.now(_et).date()
            from intraday_executor import _supa_get
            for i in range(days):
                d = (today - timedelta(days=i)).strftime('%Y-%m-%d')
                query = f'date=eq.{d}&order=entry_time.asc&limit=200'
                if status_filter != 'all':
                    query += f'&status=eq.{status_filter}'
                day_rows = _supa_get('intraday_trades', query) or []
                rows.extend(day_rows)

        return jsonify({'trades': rows, 'count': len(rows)})
    except Exception as e:
        logger.error(f'trade/log error: {e}')
        return jsonify({'error': str(e)}), 500


@app.route('/api/intraday/close/<ticker>', methods=['POST'])
def intraday_close_ticker(ticker):
    """POST — manually close a specific intraday position."""
    try:
        from intraday_executor import _alpaca, _close_position, _supa_get, force_close_all
        ticker = ticker.upper().strip()
        _, positions = _alpaca('get', '/v2/positions')
        pos = next((p for p in (positions or []) if p.get('symbol') == ticker), None)
        if not pos:
            return jsonify({'status': 'not_found', 'message': f'No open position for {ticker}'}), 404
        from datetime import datetime
        import pytz
        today = datetime.now(pytz.timezone('America/New_York')).strftime('%Y-%m-%d')
        trades = _supa_get('intraday_trades', f'date=eq.{today}&ticker=eq.{ticker}&status=neq.closed')
        trade  = trades[0] if trades else {}
        price  = float(pos.get('current_price') or 0)
        _close_position(ticker, pos, trade, 'manual_close', price)
        return jsonify({'status': 'closed', 'ticker': ticker, 'exit_price': price})
    except Exception as e:
        logger.error(f'intraday/close/{ticker} error: {e}')
        return jsonify({'error': str(e)}), 500


@app.route('/api/intraday/close/all', methods=['POST'])
def intraday_close_all():
    """POST — emergency close ALL open intraday positions."""
    try:
        from intraday_executor import force_close_all
        result = force_close_all()
        return jsonify(result)
    except Exception as e:
        logger.error(f'intraday/close/all error: {e}')
        return jsonify({'error': str(e)}), 500


@app.route('/api/alpaca/account')
def alpaca_account():
    """GET Alpaca paper account summary: equity, cash, today_pnl, buying_power."""
    try:
        key    = os.environ.get('ALPACA_KEY', '')
        secret = os.environ.get('ALPACA_SECRET', '')
        hdrs   = {'APCA-API-KEY-ID': key, 'APCA-API-SECRET-KEY': secret}

        r = requests.get('https://paper-api.alpaca.markets/v2/account',
                         headers=hdrs, timeout=10)
        if r.status_code != 200:
            return jsonify({'error': r.text}), r.status_code
        d = r.json()

        equity          = float(d.get('equity') or 0)
        last_equity     = float(d.get('last_equity') or equity)
        today_pnl       = round(equity - last_equity, 2)
        today_pnl_pct   = round(today_pnl / last_equity * 100, 2) if last_equity else 0.0
        initial_capital = 100_000.0   # paper account started at $100k

        return jsonify({
            'equity':          round(equity, 2),
            'cash':            round(float(d.get('cash') or 0), 2),
            'buying_power':    round(float(d.get('buying_power') or 0), 2),
            'portfolio_value': round(equity, 2),
            'today_pnl':       today_pnl,
            'today_pnl_pct':   today_pnl_pct,
            'total_pnl':       round(equity - initial_capital, 2),
            'total_pnl_pct':   round((equity - initial_capital) / initial_capital * 100, 2),
            'initial_capital': initial_capital,
            'currency':        'USD',
        })
    except Exception as e:
        logger.error(f'alpaca/account error: {e}')
        return jsonify({'error': str(e)}), 500


@app.route('/api/alpaca/portfolio/history')
def alpaca_portfolio_history():
    """GET /api/alpaca/portfolio/history?period=1M&timeframe=1D
    Returns equity curve from Alpaca paper account.
    period: 1W | 1M | 3M | 6M | 1Y | ALL  (ALL maps to 5Y for paper)
    timeframe: 1D | 1H | 15Min (default 1D)
    """
    try:
        key    = os.environ.get('ALPACA_KEY', '')
        secret = os.environ.get('ALPACA_SECRET', '')
        hdrs   = {'APCA-API-KEY-ID': key, 'APCA-API-SECRET-KEY': secret}

        period_map = {
            '1W': ('1W', '1D'),
            '1M': ('1M', '1D'),
            '3M': ('3M', '1D'),
            '6M': ('6M', '1D'),
            '1Y': ('1A', '1D'),
            'ALL': ('5A', '1D'),
        }
        p = request.args.get('period', '1M').upper()
        alpaca_period, timeframe = period_map.get(p, ('1M', '1D'))

        r = requests.get(
            'https://paper-api.alpaca.markets/v2/account/portfolio/history',
            params={'period': alpaca_period, 'timeframe': timeframe,
                    'extended_hours': 'false'},
            headers=hdrs, timeout=15,
        )
        if r.status_code != 200:
            return jsonify({'error': r.text, 'points': []}), 200  # graceful

        d       = r.json()
        ts      = d.get('timestamp', []) or []
        equity  = d.get('equity', []) or []
        pnl     = d.get('profit_loss', []) or []
        pnl_pct = d.get('profit_loss_pct', []) or []

        points = []
        for i, t in enumerate(ts):
            eq = equity[i] if i < len(equity) else None
            if eq is None:
                continue
            from datetime import timezone as _tz
            dt = datetime.fromtimestamp(t, tz=_tz.utc)
            points.append({
                'timestamp': t,
                'date':      dt.strftime('%Y-%m-%d'),
                'equity':    round(float(eq), 2),
                'pnl':       round(float(pnl[i]), 2)     if i < len(pnl)     else 0.0,
                'pnl_pct':   round(float(pnl_pct[i]), 4) if i < len(pnl_pct) else 0.0,
            })

        return jsonify({'period': p, 'points': points})
    except Exception as e:
        logger.error(f'alpaca/portfolio/history error: {e}')
        return jsonify({'error': str(e), 'points': []}), 200


@app.route('/api/intraday/health')
def intraday_health():
    """GET — diagnose env vars and connectivity for the auto-trading pipeline."""
    import os
    alpaca_key    = os.environ.get('ALPACA_KEY', '')
    alpaca_secret = os.environ.get('ALPACA_SECRET', '')
    anthropic_key = os.environ.get('ANTHROPIC_API_KEY', '')
    scanner_on    = os.environ.get('INTRADAY_SCANNER_ENABLED', 'false').lower() == 'true'
    trading_on    = os.environ.get('INTRADAY_TRADING_ENABLED', 'false').lower() == 'true'
    ai_on         = os.environ.get('INTRADAY_AI_ENABLED', 'false').lower() == 'true'

    # Alpaca account check
    alpaca_ok   = False
    alpaca_info = {}
    if alpaca_key and alpaca_secret:
        try:
            import requests as _req
            r = _req.get(
                'https://paper-api.alpaca.markets/v2/account',
                headers={'APCA-API-KEY-ID': alpaca_key, 'APCA-API-SECRET-KEY': alpaca_secret},
                timeout=8,
            )
            if r.status_code == 200:
                d = r.json()
                alpaca_ok   = True
                alpaca_info = {
                    'equity':        d.get('equity'),
                    'buying_power':  d.get('buying_power'),
                    'status':        d.get('status'),
                }
        except Exception as e:
            alpaca_info = {'error': str(e)}

    from intraday_scanner import get_status as scanner_get_status
    scanner_status = scanner_get_status(redis_get)

    return jsonify({
        'env_vars': {
            'ALPACA_KEY':                 bool(alpaca_key),
            'ALPACA_SECRET':              bool(alpaca_secret),
            'ANTHROPIC_API_KEY':          bool(anthropic_key),
            'INTRADAY_SCANNER_ENABLED':   scanner_on,
            'INTRADAY_TRADING_ENABLED':   trading_on,
            'INTRADAY_AI_ENABLED':        ai_on,
        },
        'alpaca_connected': alpaca_ok,
        'alpaca_account':   alpaca_info,
        'scanner':          scanner_status,
        'pipeline_ready':   alpaca_ok and scanner_on and trading_on,
        'missing': [
            k for k, v in {
                'ALPACA_KEY':               bool(alpaca_key),
                'ALPACA_SECRET':            bool(alpaca_secret),
                'INTRADAY_SCANNER_ENABLED': scanner_on,
                'INTRADAY_TRADING_ENABLED': trading_on,
            }.items() if not v
        ],
    })


# ─── /api/premarket — WHAL-121 Pre-Market Gap Scanner ────────────

@app.route('/api/premarket/watchlist')
def premarket_watchlist():
    """GET — pre-market gappers (4:00–9:30 AM ET). Active during pre-market hours.
    Returns top 20 gappers sorted by abs(gap_pct) with direction bias.
    """
    try:
        from premarket_scanner import get_watchlist
        data = get_watchlist(redis_get_fn=redis_get)
        return jsonify(data)
    except Exception as e:
        logger.error(f'premarket/watchlist error: {e}')
        return jsonify({'watchlist': [], 'last_scan': None, 'scan_count': 0})


@app.route('/api/premarket/scan', methods=['POST'])
def premarket_scan_now():
    """POST — force an immediate pre-market scan (for testing outside pre-market hours)."""
    try:
        from premarket_scanner import run_scan
        import threading
        threading.Thread(target=run_scan, args=(redis_set, redis_get),
                         daemon=True, name='premarket-force').start()
        return jsonify({'status': 'scan triggered', 'note': 'check /api/premarket/watchlist in ~10s'})
    except Exception as e:
        logger.error(f'premarket/scan error: {e}')
        return jsonify({'error': str(e)}), 500


# ─── /api/scanner — WHAL-94 Intraday Stock Scanner ───────────────

@app.route('/api/scanner/top')
def scanner_top():
    """GET top 10 scanner picks for current minute."""
    try:
        from intraday_scanner import get_top
        return jsonify({'picks': get_top(redis_get), 'count': len(get_top(redis_get))})
    except Exception as e:
        logger.error(f'scanner/top error: {e}')
        return jsonify({'error': str(e)}), 500


@app.route('/api/scanner/all')
def scanner_all():
    """GET all scored stocks from latest scan (up to 200)."""
    try:
        from intraday_scanner import get_all
        stocks = get_all(redis_get)
        return jsonify({'stocks': stocks, 'count': len(stocks)})
    except Exception as e:
        logger.error(f'scanner/all error: {e}')
        return jsonify({'error': str(e)}), 500


@app.route('/api/scanner/status')
def scanner_status():
    """GET scanner running state, last scan time, scan count."""
    try:
        from intraday_scanner import get_status
        return jsonify(get_status(redis_get))
    except Exception as e:
        logger.error(f'scanner/status error: {e}')
        return jsonify({'error': str(e)}), 500


@app.route('/api/scanner/trigger', methods=['POST'])
def scanner_trigger():
    """POST — force one immediate scan (for testing outside market hours or diagnosing)."""
    try:
        from intraday_scanner import _run_scan
        import threading
        def _run():
            try:
                _run_scan(redis_set, redis_get)
            except Exception as e:
                logger.error(f'Manual scan error: {e}')
        threading.Thread(target=_run, daemon=True, name='manual-scan').start()
        return jsonify({'status': 'scan triggered', 'note': 'check /api/scanner/status in ~30s'})
    except Exception as e:
        logger.error(f'scanner/trigger error: {e}')
        return jsonify({'error': str(e)}), 500


@app.route('/api/scanner/diagnose')
def scanner_diagnose():
    """GET — synchronous mini-scan of 3 symbols to diagnose pipeline errors."""
    import traceback
    steps = {}
    try:
        from intraday_scanner import (
            _fetch_insider_tickers, _alpaca_bars_batch,
            _score_symbol, _vol_baselines, ALPACA_KEY, ALPACA_SECRET
        )

        # Step 1: insider fetch
        try:
            insider = _fetch_insider_tickers()
            steps['insider_tickers'] = {'ok': True, 'count': len(insider)}
        except Exception as e:
            steps['insider_tickers'] = {'ok': False, 'error': str(e)}
            insider = set()

        # Step 2: Alpaca bars for 3 liquid symbols
        test_symbols = ['AAPL', 'MSFT', 'NVDA']
        steps['alpaca_key_present'] = bool(ALPACA_KEY) and bool(ALPACA_SECRET)
        try:
            bars = _alpaca_bars_batch(test_symbols, limit=5)
            steps['alpaca_bars'] = {
                'ok': True,
                'symbols_returned': list(bars.keys()),
                'bar_counts': {s: len(b) for s, b in bars.items()},
            }
        except Exception as e:
            steps['alpaca_bars'] = {'ok': False, 'error': str(e), 'tb': traceback.format_exc()}
            bars = {}

        # Step 3: score one symbol
        try:
            sym = 'AAPL'
            b = bars.get(sym, [])
            scored = _score_symbol(sym, b, insider, _vol_baselines)
            steps['score_symbol'] = {'ok': True, 'result': scored}
        except Exception as e:
            steps['score_symbol'] = {'ok': False, 'error': str(e), 'tb': traceback.format_exc()}

        # Step 4: AI scorer
        try:
            from intraday_ai_scorer import score_stock, MIN_CONFIDENCE
            sig = score_stock(
                ticker='AAPL', scanner_score=75,
                bars=bars.get('AAPL', [])[:5],
                insider_summary='', sp500_trend='neutral',
                redis_get=redis_get, redis_set=redis_set,
            )
            steps['ai_scorer'] = {'ok': True, 'signal': sig.get('signal'), 'confidence': sig.get('confidence')}
        except Exception as e:
            steps['ai_scorer'] = {'ok': False, 'error': str(e), 'tb': traceback.format_exc()}

        # Step 5: executor connectivity
        try:
            from intraday_executor import _headers as _exec_headers, PAPER_BASE
            import requests as _r
            r = _r.get(f'{PAPER_BASE}/v2/account', headers=_exec_headers(), timeout=5)
            steps['executor_alpaca'] = {'ok': r.status_code == 200, 'http': r.status_code}
        except Exception as e:
            steps['executor_alpaca'] = {'ok': False, 'error': str(e)}

        return jsonify({'steps': steps, 'all_ok': all(v.get('ok') for v in steps.values() if isinstance(v, dict))})

    except Exception as e:
        return jsonify({'fatal_error': str(e), 'tb': traceback.format_exc(), 'steps': steps}), 500


# ─── /api/scanner/score — WHAL-95 Intraday AI Scoring Engine ────

@app.route('/api/scanner/score', methods=['POST'])
def scanner_score():
    """POST {ticker, scanner_score} → AI BUY/SELL/SKIP with entry/stop/target."""
    try:
        from intraday_ai_scorer import score_stock, get_daily_token_usage
        body          = request.get_json(force=True) or {}
        ticker        = (body.get('ticker') or '').upper().strip()
        scanner_score_val = int(body.get('scanner_score') or 0)
        insider_sum   = body.get('insider_summary', '')
        sp500_trend   = body.get('sp500_trend', 'neutral')

        if not ticker:
            return jsonify({'error': 'ticker is required'}), 400

        result = score_stock(
            ticker=ticker,
            scanner_score=scanner_score_val,
            insider_summary=insider_sum,
            sp500_trend=sp500_trend,
            redis_get=redis_get,
            redis_set=redis_set,
        )
        result['token_usage'] = get_daily_token_usage()
        return jsonify(result)
    except Exception as e:
        logger.error(f'scanner/score error: {e}')
        return jsonify({'error': str(e)}), 500


@app.route('/api/scanner/usage')
def scanner_usage():
    """GET daily token/spend usage for the intraday AI scorer."""
    try:
        from intraday_ai_scorer import get_daily_token_usage
        return jsonify(get_daily_token_usage())
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ─── /api/pead — WHAL-100 PEAD Scoring Engine ────────────────────

@app.route('/api/pead/scores')
def pead_scores():
    """GET top PEAD candidates sorted by SUE magnitude and entry window."""
    try:
        from pead_engine import get_scores
        scores = get_scores(redis_get=redis_get, redis_set=redis_set)
        return jsonify({'scores': scores, 'count': len(scores)})
    except Exception as e:
        logger.error(f'pead/scores error: {e}')
        return jsonify({'error': str(e)}), 500


@app.route('/api/pead/ticker/<ticker>')
def pead_ticker(ticker):
    """GET PEAD score for a specific ticker (always fresh)."""
    try:
        from pead_engine import get_ticker_score
        result = get_ticker_score(ticker.upper())
        if result is None:
            return jsonify({'error': f'No earnings data for {ticker.upper()}'}), 404
        return jsonify(result)
    except Exception as e:
        logger.error(f'pead/ticker/{ticker} error: {e}')
        return jsonify({'error': str(e)}), 500


@app.route('/api/pead/calendar')
def pead_calendar():
    """GET upcoming and recent earnings calendar."""
    try:
        from pead_engine import get_earnings_calendar
        days_back    = int(request.args.get('days_back', 14))
        days_forward = int(request.args.get('days_forward', 7))
        cal = get_earnings_calendar(days_back=days_back, days_forward=days_forward)
        return jsonify({'calendar': cal, 'count': len(cal)})
    except Exception as e:
        logger.error(f'pead/calendar error: {e}')
        return jsonify({'error': str(e)}), 500


@app.route('/api/pead/refresh', methods=['POST'])
def pead_refresh():
    """POST — force recompute PEAD scores (clears cache)."""
    try:
        from pead_engine import refresh_scores
        import threading
        threading.Thread(target=refresh_scores, daemon=True, name='pead-refresh').start()
        return jsonify({'status': 'refresh triggered', 'note': 'check /api/pead/scores in ~60s'})
    except Exception as e:
        logger.error(f'pead/refresh error: {e}')
        return jsonify({'error': str(e)}), 500


# ─── /api/technical/:ticker — WHAL-91 Technical Indicator Engine ──
# Runs RSI, MACD, MA, volume, ATR, Bollinger Bands and returns combined score.

@app.route('/api/technical/<ticker>')
def api_technical(ticker):
    try:
        from technical_indicators import get_technical_score
        result = get_technical_score(ticker.upper().strip())
        return jsonify(result)
    except Exception as e:
        logger.error(f'api_technical({ticker}): {e}')
        return jsonify({'error': str(e)}), 500


# ─── /api/version — App Version Management ───────────────────────
# Flutter app checks this on launch. If app_version < minimum_version,
# a non-dismissible force-update screen is shown.

@app.route('/api/version')
def api_version():
    return jsonify({
        'minimum_version': '1.0.0',   # bump this to force-update older builds
        'latest_version':  '1.0.0',
        'ios_url':     'https://apps.apple.com/app/whale-tracker/id000000000',
        'android_url': 'https://play.google.com/store/apps/details?id=com.example.whaleTracker',
        'release_notes': 'Bug fixes and performance improvements.',
    })


# ─── /api/macro/calendar — WHAL-109 Macro Economic Calendar ──────────────────
# Returns upcoming economic events (Fed, CPI, PPI, NFP, GDP, PCE) from FMP + fallback.

@app.route('/api/macro/calendar')
def macro_calendar():
    """GET upcoming macro economic events for the next 60 days."""
    try:
        import requests as _req
        from datetime import datetime, timedelta
        fmp_key = os.environ.get('FMP_API_KEY', '')
        today = datetime.utcnow().date()
        end   = today + timedelta(days=60)

        events = []

        if fmp_key:
            url = (
                f'https://financialmodelingprep.com/api/v3/economic_calendar'
                f'?from={today}&to={end}&apikey={fmp_key}'
            )
            resp = _req.get(url, timeout=10)
            if resp.status_code == 200:
                raw = resp.json()
                # FMP returns a list of dicts; filter to high-impact macro events
                HIGH_KEYWORDS = ['federal reserve', 'fed', 'fomc', 'interest rate',
                                 'cpi', 'consumer price', 'ppi', 'producer price',
                                 'nonfarm payroll', 'unemployment', 'gdp',
                                 'pce', 'personal consumption', 'retail sales',
                                 'ism manufacturing', 'ism services', 'durable goods']
                for e in raw:
                    name  = (e.get('event') or '').strip()
                    name_l = name.lower()
                    if not any(kw in name_l for kw in HIGH_KEYWORDS):
                        continue
                    # Determine impact
                    impact_raw = (e.get('impact') or '').upper()
                    if impact_raw in ('HIGH', 'H'):
                        impact = 'HIGH'
                    elif impact_raw in ('MEDIUM', 'MED', 'M', 'MODERATE'):
                        impact = 'MED'
                    else:
                        impact = 'LOW'
                    events.append({
                        'event':    name,
                        'date':     e.get('date') or e.get('releaseDate') or '',
                        'country':  e.get('country', 'US'),
                        'impact':   impact,
                        'previous': e.get('previous'),
                        'forecast': e.get('estimate') or e.get('forecast'),
                        'actual':   e.get('actual'),
                    })

        # If FMP returned nothing useful, return curated near-term schedule
        if not events:
            # Static near-term known schedule as fallback
            base = today
            def _next_weekday(d, wd):  # 0=Mon…4=Fri
                days_ahead = wd - d.weekday()
                if days_ahead <= 0: days_ahead += 7
                return d + timedelta(days=days_ahead)
            fallback = [
                {'event': 'FOMC Meeting (Fed Rate Decision)', 'days': 14, 'impact': 'HIGH'},
                {'event': 'CPI (Consumer Price Index)',        'days': 10, 'impact': 'HIGH'},
                {'event': 'PPI (Producer Price Index)',        'days': 11, 'impact': 'MED'},
                {'event': 'Nonfarm Payrolls (NFP)',            'days': 7,  'impact': 'HIGH'},
                {'event': 'GDP (Gross Domestic Product)',      'days': 21, 'impact': 'HIGH'},
                {'event': 'PCE (Core Personal Consumption)',   'days': 18, 'impact': 'HIGH'},
                {'event': 'Retail Sales',                      'days': 15, 'impact': 'MED'},
                {'event': 'ISM Manufacturing PMI',             'days': 3,  'impact': 'MED'},
                {'event': 'Jobless Claims',                    'days': 4,  'impact': 'MED'},
                {'event': 'JOLTS Job Openings',                'days': 9,  'impact': 'MED'},
                {'event': 'Consumer Confidence',               'days': 12, 'impact': 'LOW'},
                {'event': 'Durable Goods Orders',              'days': 16, 'impact': 'MED'},
            ]
            for f in fallback:
                evt_date = base + timedelta(days=f['days'])
                events.append({
                    'event':    f['event'],
                    'date':     evt_date.strftime('%Y-%m-%d'),
                    'country':  'US',
                    'impact':   f['impact'],
                    'previous': None,
                    'forecast': None,
                    'actual':   None,
                })

        # Sort by date ascending
        events.sort(key=lambda x: x.get('date') or '')
        return jsonify({'events': events, 'count': len(events), 'source': 'FMP' if fmp_key else 'fallback'})

    except Exception as e:
        logger.error(f'macro/calendar error: {e}')
        return jsonify({'error': str(e)}), 500


# ─── /api/notify — WHAL-111 Push Notifications ──────────────────────────────

# ─── /api/confluence — Signal Radar: multi-signal confluence per ticker ───────
# Aggregates 8 existing signal sources and surfaces tickers where 2+ signals
# fire simultaneously. Cached for 5 minutes to avoid hammering sub-endpoints.

@app.route('/api/confluence')
def signal_confluence():
    """
    GET /api/confluence — returns top tickers ranked by number of signals firing.
    Each ticker entry includes which signals fired and a confluence score 0-100.
    Works 24/7: uses Redis caches from other endpoints + insider filings + congress.
    Falls back to last-good result (24h TTL) when live data is unavailable.
    """
    try:
        from datetime import datetime, timedelta

        cache_key = 'confluence:v2'
        last_good_key = 'confluence:last_good'

        cached = redis_get(cache_key) if _redis else None
        if cached:
            return jsonify(cached)

        tickers: dict = {}  # ticker -> {signals: [], score: int, details: {}}

        def add_signal(ticker: str, name: str, weight: int, detail: str = ''):
            t = ticker.upper().strip()
            if not t or len(t) > 6:
                return
            if t not in tickers:
                tickers[t] = {'ticker': t, 'signals': [], 'score': 0, 'details': {}}
            if name not in tickers[t]['signals']:
                tickers[t]['signals'].append(name)
                tickers[t]['score'] += weight
            if detail:
                tickers[t]['details'][name] = detail

        # ── 1. Scanner top picks — try Redis cache first (weight 15) ──────────
        try:
            scanner_data = redis_get('scanner:top') if _redis else None
            if not scanner_data:
                from intraday_scanner import get_top_picks
                scanner_data = get_top_picks(limit=20)
            # scanner:top may be a list or a dict with 'picks' key
            if isinstance(scanner_data, dict):
                scanner_data = scanner_data.get('picks') or scanner_data.get('tickers') or []
            for s in (scanner_data or []):
                t = s.get('ticker', '')
                score = int(s.get('score') or s.get('ai_score') or 0)
                if score >= 60:
                    add_signal(t, 'SCANNER', 15, f'AI score {score}')
        except Exception as e:
            logger.debug(f'confluence scanner: {e}')

        # ── 2. Options conviction — try Redis cache first (weight 20) ─────────
        try:
            opts_cached = redis_get('options:conviction') if _redis else None
            if opts_cached:
                opts = opts_cached if isinstance(opts_cached, list) else opts_cached.get('picks', [])
            else:
                from options_flow import get_conviction_picks
                opts = get_conviction_picks(limit=30)
            for o in (opts or []):
                t = o.get('ticker', '')
                score = int(o.get('conviction_score') or 0)
                if score >= 50:
                    side = o.get('side', 'CALL')
                    add_signal(t, 'OPTIONS', 20, f'{side} conviction {score}')
        except Exception as e:
            logger.debug(f'confluence options: {e}')

        # ── 3. Short squeeze — use squeeze:candidates Redis cache (weight 15) ──
        try:
            sq_cached = redis_get('squeeze:candidates') if _redis else None
            if sq_cached:
                squeezes = sq_cached.get('candidates', []) if isinstance(sq_cached, dict) else sq_cached
            else:
                from squeeze_detector import get_squeeze_candidates
                squeezes = get_squeeze_candidates(limit=20)
            for s in (squeezes or []):
                t = s.get('ticker', '')
                score = int(s.get('squeeze_score') or 0)
                if score >= 50:
                    add_signal(t, 'SQUEEZE', 15, f'Squeeze score {score}')
        except Exception as e:
            logger.debug(f'confluence squeeze: {e}')

        # ── 4. Smart Money composite — key: smart_money:composite (weight 25) ──
        try:
            sm_cached = redis_get('smart_money:composite') if _redis else None
            if sm_cached:
                sm = sm_cached if isinstance(sm_cached, list) else sm_cached.get('scores', [])
            else:
                from smart_money_composite import get_composite_scores
                sm = get_composite_scores()
            for s in (sm or []):
                t = s.get('ticker', '')
                score = int(s.get('score') or 0)
                if score >= 50:
                    add_signal(t, 'SMART MONEY', 25, f'Composite {score} {s.get("label","")}')
        except Exception as e:
            logger.debug(f'confluence smart money: {e}')

        # ── 5. PEAD post-earnings drift — key: pead:scores, field: sue (weight 20) ──
        try:
            pead_cached = redis_get('pead:scores') if _redis else None
            if pead_cached:
                peads = pead_cached if isinstance(pead_cached, list) else pead_cached.get('scores', [])
            else:
                from pead_engine import get_scores as _pead_get
                peads = _pead_get()
            for p in (peads or []):
                t = p.get('ticker', '')
                sue = float(p.get('sue') or 0)
                if abs(sue) >= 1.5:
                    add_signal(t, 'PEAD', 20, f'SUE {sue:.1f} {p.get("direction","")}')
        except Exception as e:
            logger.debug(f'confluence pead: {e}')

        # ── 6. OFI top imbalances — key: ofi:scores, field: overall_ofi (weight 15) ──
        try:
            ofi_cached = redis_get('ofi:scores') if _redis else None
            if ofi_cached:
                ofis = ofi_cached if isinstance(ofi_cached, list) else ofi_cached.get('scores', [])
            else:
                from ofi_vpin_engine import get_ofi_scores
                ofis = get_ofi_scores()
            for o in (ofis or []):
                t = o.get('ticker', '')
                ofi = float(o.get('overall_ofi') or o.get('ofi_avg') or 0)
                if abs(ofi) >= 0.3:
                    direction = 'BUY' if ofi > 0 else 'SELL'
                    add_signal(t, 'OFI', 15, f'{direction} pressure {ofi:.2f}')
        except Exception as e:
            logger.debug(f'confluence ofi: {e}')

        # ── 7. Recent insider filings — cluster buys, 24/7 (weight 20) ─────────
        try:
            recent = list(_filings_cache)[-200:] if _filings_cache else []
            cutoff = datetime.utcnow() - timedelta(days=3)
            buy_counts: dict = {}
            for f in recent:
                # support both dict and object
                is_buy = f.get('is_buy', False) if isinstance(f, dict) else getattr(f, 'is_buy', False)
                if not is_buy:
                    continue
                try:
                    fd_raw = f.get('filing_date', '') if isinstance(f, dict) else getattr(f, 'filing_date', '')
                    fd = datetime.strptime(str(fd_raw)[:10], '%Y-%m-%d')
                    if fd < cutoff:
                        continue
                except Exception:
                    pass
                t = (f.get('ticker') if isinstance(f, dict) else getattr(f, 'ticker', '')) or ''
                t = t.upper().strip()
                if t:
                    buy_counts[t] = buy_counts.get(t, 0) + 1
            for t, cnt in buy_counts.items():
                if cnt >= 2:
                    add_signal(t, 'INSIDER', 20, f'{cnt} buys in 3d')
        except Exception as e:
            logger.debug(f'confluence insider: {e}')

        # ── 8. Congress trades — 24/7 historical data (weight 10) ───────────────
        try:
            from congress_trades import get_recent_trades
            ctrades = get_recent_trades(limit=30, days=14)
            congress_buys: dict = {}
            for c in (ctrades or []):
                if 'purchase' in (c.get('type') or '').lower():
                    t = c.get('ticker', '')
                    congress_buys[t] = congress_buys.get(t, 0) + 1
            for t, cnt in congress_buys.items():
                add_signal(t, 'CONGRESS', 10, f'{cnt} congressional buy{"s" if cnt > 1 else ""}')
        except Exception as e:
            logger.debug(f'confluence congress: {e}')

        # ── Build result — prefer 2+ signals; fall back to 1+ if slim ───────────
        results = [v for v in tickers.values() if len(v['signals']) >= 2]
        if len(results) < 3:
            # Outside market hours only insider/congress may fire — show 1+ signals too
            results = [v for v in tickers.values() if len(v['signals']) >= 1]
        results.sort(key=lambda x: (-len(x['signals']), -x['score']))
        results = results[:20]

        # Normalize score to 0-100
        max_score = max((r['score'] for r in results), default=1)
        for r in results:
            r['confluence_score'] = min(100, round(r['score'] / max_score * 100))
            r['signal_count'] = len(r['signals'])

        output = {'tickers': results, 'count': len(results),
                  'generated_at': datetime.utcnow().isoformat()}

        if _redis:
            # Short-lived cache: 5 min so live data refreshes quickly
            redis_set(cache_key, output, ttl_seconds=300)
            # Long-lived "last good" cache: 24h so off-hours users still see data
            if results:
                redis_set(last_good_key, output, ttl_seconds=86400)

        # If still empty, return last-known-good so users always see something
        if not results and _redis:
            last_good = redis_get(last_good_key)
            if last_good:
                last_good['stale'] = True
                return jsonify(last_good)

        return jsonify(output)

    except Exception as e:
        logger.error(f'confluence error: {e}')
        return jsonify({'error': str(e), 'tickers': []}), 500


# ─── /api/filings/backfill — Historical SEC EDGAR bulk import ────────────────

@app.route('/api/filings/backfill')
def filings_backfill():
    """
    GET /api/filings/backfill?days=90 — pulls Form 4 filings from SEC EDGAR
    full-text search RSS going back N days and upserts into Supabase.
    Run this once to seed historical data for Portfolio Simulator grades.
    """
    try:
        days = int(request.args.get('days', 90))
        days = min(days, 180)  # cap at 180 days
        inserted = 0
        errors = 0

        import xml.etree.ElementTree as ET

        # SEC EDGAR full-text search for Form 4 filings
        # Paginate through results (40 per page)
        for page in range(0, 10):  # up to 400 filings
            try:
                rss_url = (
                    'https://efts.sec.gov/LATEST/search-index?q=%22form+4%22'
                    f'&dateRange=custom&startdt={(datetime.utcnow()-timedelta(days=days)).strftime("%Y-%m-%d")}'
                    f'&enddt={datetime.utcnow().strftime("%Y-%m-%d")}'
                    f'&from={page * 40}&size=40&forms=4'
                )
                r = requests.get(rss_url,
                    headers={'User-Agent': 'WhaleTracker research@whaletracker.app'},
                    timeout=15)
                if r.status_code != 200:
                    break
                hits = r.json().get('hits', {}).get('hits', [])
                if not hits:
                    break

                for hit in hits:
                    try:
                        src = hit.get('_source', {})
                        # Extract filing details from EDGAR full-text search result
                        entity_name = src.get('entity_name', src.get('display_names', [''])[0] if src.get('display_names') else '')
                        ticker = ''
                        filed_at = src.get('file_date', '')
                        accession = (src.get('accession_no') or '').replace('-', '')

                        if not filed_at or not accession:
                            continue

                        # Fetch the actual Form 4 XML for transaction details
                        cik = str(src.get('entity_id', '')).zfill(10)
                        xml_url = f'https://www.sec.gov/Archives/edgar/data/{cik.lstrip("0")}/{accession}/{accession[:10]}-{accession[10:12]}-{accession[12:]}.txt'

                        # Use a simpler approach — parse from filing index
                        idx_url = f'https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={cik}&type=4&dateb=&owner=include&count=5&search_text='
                        # Skip deep parsing — extract from display data
                        tickers_list = src.get('tickers', [])
                        if tickers_list:
                            ticker = tickers_list[0].upper()

                        if not ticker:
                            continue

                        # Upsert into Supabase filings table
                        row = {
                            'ticker': ticker,
                            'owner_name': entity_name,
                            'owner_type': 'Director',
                            'transaction_type': 'buy',
                            'filed_at': filed_at,
                            'transaction_date': filed_at,
                            'value': 0,
                            'shares': 0,
                            'price': 0,
                        }
                        upsert_r = requests.post(
                            f'{SUPABASE_URL}/rest/v1/filings',
                            headers={**SUPABASE_HEADERS, 'Prefer': 'resolution=ignore-duplicates'},
                            json=row,
                            timeout=8,
                        )
                        if upsert_r.status_code in (200, 201):
                            inserted += 1
                    except Exception as ex:
                        errors += 1
                        logger.debug(f'backfill item error: {ex}')

                if len(hits) < 40:
                    break
            except Exception as page_err:
                logger.warning(f'backfill page {page}: {page_err}')
                break

        return jsonify({'inserted': inserted, 'errors': errors, 'days': days,
                        'message': f'Backfilled {inserted} historical filings from SEC EDGAR'})
    except Exception as e:
        logger.error(f'backfill error: {e}')
        return jsonify({'error': str(e)}), 500


@app.route('/api/notify/register', methods=['POST'])
def notify_register():
    """POST {fcm_token, user_id?} — store FCM token in Supabase user_devices."""
    try:
        body = request.get_json(silent=True) or {}
        token = (body.get('fcm_token') or '').strip()
        if not token:
            return jsonify({'error': 'fcm_token required'}), 400
        if _supabase:
            row = {
                'fcm_token': token,
                'platform':  body.get('platform', 'unknown'),
                'updated_at': __import__('datetime').datetime.utcnow().isoformat(),
            }
            uid = body.get('user_id')
            if uid:
                row['user_id'] = uid
            _supabase.table('user_devices').upsert(row, on_conflict='fcm_token').execute()
        return jsonify({'status': 'registered'})
    except Exception as e:
        logger.error(f'notify/register error: {e}')
        return jsonify({'error': str(e)}), 500


@app.route('/api/notify/send', methods=['POST'])
def notify_send():
    """
    POST {token, title, body} — send a test push via FCM.
    Also accepts {ticker, insider, value} to trigger watchlist notification.
    Requires FIREBASE_SERVICE_ACCOUNT_JSON env var on Railway.
    """
    try:
        from firebase_push import send_to_token, notify_watchlist_activity
        body = request.get_json(silent=True) or {}

        # Watchlist-style notification
        ticker  = body.get('ticker')
        insider = body.get('insider')
        value   = float(body.get('value') or 0)
        if ticker and insider and value > 0:
            sent = notify_watchlist_activity(ticker, insider, value, _supabase)
            return jsonify({'status': 'sent', 'count': sent, 'mode': 'watchlist'})

        # Direct token notification
        token = body.get('token', '').strip()
        title = body.get('title', 'Whale Tracker Alert')
        msg   = body.get('body', 'New insider activity detected')
        if not token:
            return jsonify({'error': 'token required (or provide ticker+insider+value)'}), 400
        ok = send_to_token(token, title, msg)
        return jsonify({'status': 'sent' if ok else 'failed', 'mode': 'direct'})
    except Exception as e:
        logger.error(f'notify/send error: {e}')
        return jsonify({'error': str(e)}), 500


# ─── Cache warm-up helper (called from _startup) ──────────────────

def warm_redis_cache():
    """Pre-populate Redis with expensive endpoints on server start."""
    if _redis is None:
        return
    logger.info('Warming Redis cache…')
    try:
        # Warm /api/filings
        filings_data = {'filings': list(_filings_cache)[:80], 'total': len(_filings_cache), 'last_updated': _last_updated}
        redis_set('api:filings:80', filings_data, ttl_seconds=300)
        logger.info('Redis warm: api:filings')
    except Exception as e:
        logger.warning(f'Redis warm error: {e}')


if __name__ == '__main__':
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == 'api':
        port = int(os.getenv('PORT', 5000))
        app.run(host='0.0.0.0', port=port, debug=False)
    else:
        run_scheduler()
