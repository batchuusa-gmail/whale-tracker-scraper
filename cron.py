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

ON_CONFLICT = {
    'congress_trades':    'member,ticker,date,type',
    'options_flow_daily': 'ticker,option_type,strike,expiry,trade_date',
    'market_snapshots':   'ticker,snapshot_date',
    'short_interest_daily': 'ticker,snapshot_date',
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


# ─── Main ─────────────────────────────────────────────────────────

def run():
    start = datetime.now(timezone.utc)
    log.info(f'===== Whale Tracker Daily Cron — {TODAY} =====')

    counts = {
        'congress_trades':    scrape_congress_trades(),
        'market_snapshots':   scrape_market_snapshots(),
        'short_interest':     scrape_short_interest(),
        'options_flow':       scrape_options_flow(),
    }

    elapsed = (datetime.now(timezone.utc) - start).seconds
    log.info(f'===== Done in {elapsed}s — {counts} =====')


if __name__ == '__main__':
    run()
