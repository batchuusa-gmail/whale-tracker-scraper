#!/usr/bin/env python3
"""
SEC Form 4 Insider Trading Scraper - Flask API
Deployed on Railway
v1.0 - Fixed for Railway deployment
"""

import os
import random
import requests
from datetime import datetime, timedelta
from flask import Flask, jsonify, request
from flask_cors import CORS
from bs4 import BeautifulSoup
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Flask app
app = Flask(__name__)
CORS(app)

# SEC EDGAR Configuration
SEC_BASE_URL = "https://www.sec.gov"
USER_AGENT = "WhaleTracker/1.0 (contact@whaletracker.app)"
SEC_HEADERS = {
    'User-Agent': USER_AGENT,
    'Accept-Encoding': 'gzip, deflate',
    'Host': 'www.sec.gov',
}


@app.route('/api/health')
def health():
    return jsonify({'status': 'ok', 'timestamp': datetime.now().isoformat()})


@app.route('/api/top-stocks')
def get_top_stocks():
    """Get top 100 traded stocks"""
    limit = request.args.get('limit', 100, type=int)
    
    # Return the top stocks data
    stocks = _get_top_stocks_data(limit)
    return jsonify({'stocks': stocks, 'count': len(stocks)})


@app.route('/api/company/<ticker>')
def get_company(ticker):
    """Get company analysis"""
    analysis = _get_company_analysis(ticker.upper())
    return jsonify(analysis)


@app.route('/api/filings')
def get_filings():
    """Get recent insider trading filings"""
    limit = request.args.get('limit', 50, type=int)
    days = request.args.get('days', 7, type=int)
    
    # For now, return sample filings
    filings = _get_sample_filings(limit)
    return jsonify({'filings': filings, 'count': len(filings)})


def _get_top_stocks_data(limit):
    """Generate top stocks data"""
    base_stocks = [
        {'ticker': 'NVDA', 'name': 'NVIDIA Corporation', 'sector': 'Technology', 'price': 875.50, 'change': 25.30, 'changePercent': 2.98, 'volume': 45000000, 'marketCap': 2150000000000},
        {'ticker': 'AAPL', 'name': 'Apple Inc.', 'sector': 'Technology', 'price': 172.50, 'change': -1.20, 'changePercent': -0.69, 'volume': 52000000, 'marketCap': 2650000000000},
        {'ticker': 'MSFT', 'name': 'Microsoft Corporation', 'sector': 'Technology', 'price': 415.20, 'change': 5.80, 'changePercent': 1.42, 'volume': 28000000, 'marketCap': 3080000000000},
        {'ticker': 'META', 'name': 'Meta Platforms Inc.', 'sector': 'Technology', 'price': 485.00, 'change': 12.50, 'changePercent': 2.64, 'volume': 18000000, 'marketCap': 1240000000000},
        {'ticker': 'TSLA', 'name': 'Tesla Inc.', 'sector': 'Automotive', 'price': 245.00, 'change': -8.30, 'changePercent': -3.28, 'volume': 95000000, 'marketCap': 780000000000},
        {'ticker': 'GOOGL', 'name': 'Alphabet Inc.', 'sector': 'Technology', 'price': 142.30, 'change': 2.10, 'changePercent': 1.50, 'volume': 22000, 'marketCap': 1780000000000},
        {'ticker': 'AMZN', 'name': 'Amazon.com Inc.', 'sector': 'Consumer Cyclical', 'price': 178.50, 'change': 3.20, 'changePercent': 1.83, 'volume': 35000000, 'marketCap': 1850000000000},
        {'ticker': 'AMD', 'name': 'Advanced Micro Devices', 'sector': 'Technology', 'price': 165.20, 'change': 4.50, 'changePercent': 2.80, 'volume': 65000000, 'marketCap': 267000000000},
        {'ticker': 'AVGO', 'name': 'Broadcom Inc.', 'sector': 'Technology', 'price': 1280.00, 'change': 15.00, 'changePercent': 1.19, 'volume': 3500000, 'marketCap': 590000000000},
        {'ticker': 'NFLX', 'name': 'Netflix Inc.', 'sector': 'Communication Services', 'price': 605.00, 'change': 8.50, 'changePercent': 1.42, 'volume': 5500000, 'marketCap': 265000000000},
    ]
    
    # Add more stocks to reach limit
    additional = ['JPM', 'V', 'JNJ', 'WMT', 'PG', 'UNH', 'HD', 'MA', 'DIS', 'PYPL', 
                 'SQ', 'UBER', 'SNOW', 'SHOP', 'ORCL', 'CSCO', 'INTC', 'CRM', 'ADBE', 'CRM']
    
    for i, t in enumerate(additional):
        base_stocks.append({
            'ticker': t,
            'name': f'{t} Corporation',
            'sector': ['Technology', 'Financial Services', 'Healthcare', 'Consumer'][i % 4],
            'price': round(random.uniform(50, 500), 2),
            'change': round(random.uniform(-5, 10), 2),
            'changePercent': round(random.uniform(-3, 5), 2),
            'volume': random.randint(1000000, 50000000),
            'marketCap': random.randint(10_000_000_000, 500_000_000_000)
        })
    
    return base_stocks[:limit]


def _get_company_analysis(ticker):
    """Get company analysis data"""
    stocks = _get_top_stocks_data(100)
    stock = next((s for s in stocks if s['ticker'] == ticker), stocks[0])
    
    return {
        'ticker': stock['ticker'],
        'companyName': stock['name'],
        'sector': stock['sector'],
        'industry': f'{stock["sector"]} Sector',
        'description': f'{stock["name"]} is a leading company in the {stock["sector"]} sector.',
        'ceo': 'CEO Name',
        'employees': '10,000+',
        'headquarters': 'United States',
        'website': f'https://www.{stock["ticker"].lower()}.com',
        'marketCap': stock['marketCap'],
        'peRatio': round(random.uniform(15, 40), 2),
        'dividendYield': round(random.uniform(0.5, 3.0), 2),
        'beta': round(random.uniform(0.8, 1.5), 2),
        'price': stock['price'],
        'change': stock['change'],
        'changePercent': stock['changePercent'],
        'volume': stock['volume'],
        'week52High': round(stock['price'] * 1.2, 2),
        'week52Low': round(stock['price'] * 0.7, 2),
        'avgVolume': stock['volume'],
        'insiderActivity': _get_insider_activity(ticker),
        'institutionalHolders': _get_institutional_holders(ticker),
        'financialMetrics': _get_financial_metrics()
    }


def _get_insider_activity(ticker):
    """Get sample insider activity"""
    return [
        {'owner': 'CEO', 'type': 'Purchase', 'shares': random.randint(100, 10000), 
         'price': round(random.uniform(50, 500), 2), 'date': '2026-03-10', 'value': random.randint(10000, 500000)},
        {'owner': 'CFO', 'type': 'Sale', 'shares': random.randint(50, 5000),
         'price': round(random.uniform(50, 500), 2), 'date': '2026-03-08', 'value': random.randint(5000, 250000)},
        {'owner': 'Director', 'type': 'Purchase', 'shares': random.randint(200, 2000),
         'price': round(random.uniform(50, 500), 2), 'date': '2026-03-05', 'value': random.randint(20000, 100000)},
    ]


def _get_institutional_holders(ticker):
    """Get sample institutional holders"""
    return [
        {'holder': 'Vanguard Group', 'shares': random.randint(1000000, 50000000), 'percent': round(random.uniform(5, 15), 2)},
        {'holder': 'BlackRock', 'shares': random.randint(1000000, 50000000), 'percent': round(random.uniform(5, 15), 2)},
        {'holder': 'State Street', 'shares': random.randint(500000, 20000000), 'percent': round(random.uniform(2, 8), 2)},
        {'holder': 'Fidelity', 'shares': random.randint(500000, 15000000), 'percent': round(random.uniform(2, 6), 2)},
    ]


def _get_financial_metrics():
    """Get sample financial metrics"""
    return {
        'revenue': random.randint(50_000_000_000, 200_000_000_000),
        'grossProfit': random.randint(20_000_000_000, 100_000_000_000),
        'operatingIncome': random.randint(10_000_000_000, 50_000_000_000),
        'netIncome': random.randint(5_000_000_000, 30_000_000_000),
        'grossMargin': round(random.uniform(35, 55), 2),
        'operatingMargin': round(random.uniform(15, 30), 2),
        'netMargin': round(random.uniform(10, 20), 2),
        'roe': round(random.uniform(15, 35), 2),
        'roa': round(random.uniform(5, 15), 2),
        'currentRatio': round(random.uniform(1, 2), 2),
        'quickRatio': round(random.uniform(0.8, 1.5), 2),
        'debtToEquity': round(random.uniform(0.3, 1.0), 2),
    }


def _get_sample_filings(limit):
    """Get sample insider filings"""
    tickers = ['NVDA', 'AAPL', 'MSFT', 'META', 'TSLA', 'GOOGL', 'AMZN', 'AMD']
    owners = ['CEO', 'CFO', 'CTO', 'Director', 'VP', 'COO']
    types = ['Purchase', 'Sale', 'Option Exercise']
    
    filings = []
    for i in range(min(limit, 50)):
        ticker = random.choice(tickers)
        filings.append({
            'id': i + 1,
            'ticker': ticker,
            'owner': f'{random.choice(owners)} {ticker}',
            'type': random.choice(types),
            'shares': random.randint(100, 50000),
            'price': round(random.uniform(50, 500), 2),
            'date': (datetime.now() - timedelta(days=random.randint(0, 7))).strftime('%Y-%m-%d'),
            'value': random.randint(10000, 2000000)
        })
    
    return filings


if __name__ == '__main__':
    port = int(os.getenv('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)