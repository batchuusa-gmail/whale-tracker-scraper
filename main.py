#!/usr/bin/env python3
"""
SEC Form 4 Insider Trading Scraper
Scrapes SEC EDGAR for new insider trading filings
Run: python main.py
"""

import os
import re
import json
import time
import random
import requests
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import pandas as pd
from supabase import create_client, Client
import schedule
import logging
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# SEC EDGAR Configuration
SEC_BASE_URL = "https://www.sec.gov"
SEC_ARCHIVE_URL = "https://www.sec.gov/Archives/edgar/daily-index"
USER_AGENT = "WhaleTracker/1.0 (contact@whaletracker.app)"

# Headers for SEC requests
SEC_HEADERS = {
    'User-Agent': USER_AGENT,
    'Accept-Encoding': 'gzip, deflate',
    'Host': 'www.sec.gov',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
}

# Rate limiting
SEC_DELAY = 0.1  # 100ms between requests (SEC allows 10 req/sec)


class SECScraper:
    """Scrapes SEC EDGAR for insider trading data"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(SEC_HEADERS)
        
        # Initialize Supabase if credentials available
        supabase_url = os.getenv('SUPABASE_URL')
        supabase_key = os.getenv('SUPABASE_KEY')
        
        if supabase_url and supabase_key:
            self.supabase: Client = create_client(supabase_url, supabase_key)
            logger.info("Supabase connected")
        else:
            self.supabase = None
            logger.warning("Supabase not configured - data will only be logged")
    
    def rate_limit(self):
        """Respect SEC's rate limits"""
        time.sleep(SEC_DELAY + random.uniform(0, 0.1))
    
    def get_daily_index(self, date: datetime) -> list:
        """Get the daily index for a specific date"""
        year = date.year
        month = f"{date.month:02d}"
        day = f"{date.day:02d}"
        
        # Daily-index URL format: /Archives/edgar/daily-index/YYYY/QTRM/MASTER.IDX
        url = f"{SEC_ARCHIVE_URL}/{year}/QTR{date.quarter}/master.idx"
        
        logger.info(f"Fetching: {url}")
        self.rate_limit()
        
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            # Parse the index file
            lines = response.text.split('\n')
            filings = []
            
            for line in lines[10:]:  # Skip header lines
                if not line.strip():
                    continue
                
                parts = line.split('|')
                if len(parts) >= 5:
                    cik = parts[0].strip()
                    name = parts[1].strip()
                    form = parts[2].strip()
                    date_filed = parts[3].strip()
                    filename = parts[4].strip()
                    
                    # Filter for Form 4 (insider trading)
                    if form in ['4', '4/A']:
                        filings.append({
                            'cik': cik,
                            'name': name,
                            'form': form,
                            'date_filed': date_filed,
                            'filename': filename
                        })
            
            logger.info(f"Found {len(filings)} Form 4 filings")
            return filings
            
        except Exception as e:
            logger.error(f"Error fetching daily index: {e}")
            return []
    
    def get_filing_details(self, filename: str) -> dict:
        """Get details from a specific filing"""
        url = f"{SEC_BASE_URL}{filename}"
        self.rate_limit()
        
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'lxml')
            
            # Extract XML content
            xml_content = soup.find('ownershipdocument')
            if not xml_content:
                return {}
            
            def get_text(tag):
                elem = xml_content.find(tag)
                return elem.get_text(strip=True) if elem else ''
            
            # Parse key fields
            return {
                'issuer_name': get_text('issuername'),
                'issuer_ticker': get_text('issuertradingsymbol'),
                'owner_name': get_text('ownername'),
                'owner_type': get_text('ownertype'),
                'transaction_date': get_text('transactiondate'),
                'transaction_code': get_text('transactioncode'),
                'transaction_shares': get_text('transactionshares'),
                'transaction_price': get_text('transactionpricepershare'),
                'shares_owned': get_text('sharesownedfollowingtransaction'),
                'value_owned': get_text('valueofsecuritiesownedfollowingtransaction'),
            }
            
        except Exception as e:
            logger.error(f"Error fetching filing details: {e}")
            return {}
    
    def get_recent_filings(self, days: int = 7) -> list:
        """Get Form 4 filings from the last N days"""
        all_filings = []
        
        for i in range(days):
            date = datetime.now() - timedelta(days=i)
            logger.info(f"Checking filings for {date.strftime('%Y-%m-%d')}")
            
            filings = self.get_daily_index(date)
            all_filings.extend(filings)
            
            # SEC archives might not have today's data yet
            if i == 0 and not filings:
                logger.info("No filings for today yet, checking yesterday")
                continue
        
        return all_filings
    
    def save_to_supabase(self, filings: list):
        """Save filings to Supabase database"""
        if not self.supabase:
            logger.warning("Supabase not configured, skipping save")
            return
        
        try:
            # Upsert filings
            data = [f for f in filings if f.get('ticker')]  # Only with ticker
            
            if data:
                self.supabase.table('filings').upsert(data).execute()
                logger.info(f"Saved {len(data)} filings to Supabase")
                
        except Exception as e:
            logger.error(f"Error saving to Supabase: {e}")
    
    def get_top_traded_stocks(self) -> list:
        """Get top stocks by trading activity"""
        
        # In production, this would analyze actual trading volume
        # For now, return a list of popular stocks
        top_stocks = [
            {'ticker': 'NVDA', 'name': 'NVIDIA Corporation', 'sector': 'Technology'},
            {'ticker': 'AAPL', 'name': 'Apple Inc.', 'sector': 'Technology'},
            {'ticker': 'MSFT', 'name': 'Microsoft Corporation', 'sector': 'Technology'},
            {'ticker': 'META', 'name': 'Meta Platforms Inc.', 'sector': 'Technology'},
            {'ticker': 'TSLA', 'name': 'Tesla Inc.', 'sector': 'Automotive'},
            {'ticker': 'GOOGL', 'name': 'Alphabet Inc.', 'sector': 'Technology'},
            {'ticker': 'AMZN', 'name': 'Amazon.com Inc.', 'sector': 'Consumer Cyclical'},
            {'ticker': 'AMD', 'name': 'Advanced Micro Devices', 'sector': 'Technology'},
            {'ticker': 'AVGO', 'name': 'Broadcom Inc.', 'sector': 'Technology'},
            {'ticker': 'NFLX', 'name': 'Netflix Inc.', 'sector': 'Communication Services'},
            # Add more...
        ]
        
        return top_stocks
    
    def get_company_analysis(self, ticker: str) -> dict:
        """Get comprehensive company analysis"""
        
        # This would fetch from multiple sources:
        # - SEC filings
        # - Yahoo Finance
        # - Market data APIs
        
        # Return mock data for now
        return {
            'ticker': ticker,
            'company_name': ticker,
            'sector': 'Technology',
            'industry': 'Software',
            'description': f'{ticker} - Major technology company',
            'ceo': 'CEO Name',
            'employees': '10000+',
            'headquarters': 'USA',
            'market_cap': 1000000000000,
            'pe_ratio': 25.5,
            'dividend_yield': 0.5,
            'beta': 1.2,
            'price': 150.00,
            'change': 2.50,
            'change_percent': 1.69,
            'volume': 50000000,
            'week52_high': 180.00,
            'week52_low': 120.00,
            'avg_volume': 45000000,
            # Insider activity (from recent Form 4)
            'insider_activity': [],
            # Institutional holders
            'institutional_holders': [],
            # Financial metrics
            'financial_metrics': {
                'revenue': 100000000000,
                'gross_profit': 50000000000,
                'operating_income': 25000000000,
                'net_income': 20000000000,
                'gross_margin': 50.0,
                'operating_margin': 25.0,
                'net_margin': 20.0,
                'roe': 30.0,
                'roa': 15.0,
                'current_ratio': 1.5,
                'quick_ratio': 1.2,
                'debt_to_equity': 0.5,
            }
        }
    
    def run(self):
        """Main scraping job"""
        logger.info("=" * 50)
        logger.info("Starting SEC scraper job")
        logger.info("=" * 50)
        
        # Get recent filings
        filings = self.get_recent_filings(days=1)
        
        logger.info(f"Found {len(filings)} total Form 4 filings")
        
        # Save to database
        if filings:
            self.save_to_supabase(filings)
        
        logger.info("Scraper job complete")
        logger.info("=" * 50)
        
        return {
            'filings_found': len(filings),
            'timestamp': datetime.now().isoformat()
        }


# API endpoints for the Flask app
def create_app():
    """Create Flask API"""
    from flask import Flask, jsonify, request
    
    app = Flask(__name__)
    scraper = SECScraper()
    
    @app.route('/api/health')
    def health():
        return jsonify({'status': 'ok', 'timestamp': datetime.now().isoformat()})
    
    @app.route('/api/filings')
    def get_filings():
        """Get recent insider trading filings"""
        limit = request.args.get('limit', 100, type=int)
        
        # In production, fetch from Supabase
        # For now, return mock data
        return jsonify({
            'filings': [],
            'count': 0
        })
    
    @app.route('/api/top-stocks')
    def get_top_stocks():
        """Get top traded stocks"""
        return jsonify({
            'stocks': scraper.get_top_traded_stocks()
        })
    
    @app.route('/api/company/<ticker>')
    def get_company(ticker):
        """Get company analysis"""
        return jsonify(scraper.get_company_analysis(ticker))
    
    return app


def run_scheduler():
    """Run the scraper on a schedule"""
    scraper = SECScraper()
    
    # Run every hour
    schedule.every().hour.do(scraper.run)
    
    # Also run at specific times
    schedule.every().day().at("09:00").do(scraper.run)  # Market open
    schedule.every().day().at("16:00").do(scraper.run)  # Market close
    
    logger.info("Scheduler started")
    
    # Run once on startup
    scraper.run()
    
    while True:
        schedule.run_pending()
        time.sleep(60)


if __name__ == '__main__':
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == 'api':
        # Run as API server
        app = create_app()
        app.run(host='0.0.0.0', port=int(os.getenv('PORT', 5000)), debug=False)
    else:
        # Run as scheduled scraper
        run_scheduler()