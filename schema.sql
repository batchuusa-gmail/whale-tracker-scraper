-- WHAL-114: Daily Data Persistence Schema
-- Run this in Supabase SQL Editor: https://supabase.com/dashboard/project/bedurjtazsfbnkisoeee/sql/new

-- ─── Congress Trades ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS congress_trades (
    id          BIGSERIAL PRIMARY KEY,
    member      TEXT        NOT NULL,
    party       TEXT,
    chamber     TEXT,
    ticker      TEXT        NOT NULL,
    type        TEXT        NOT NULL,   -- 'Buy' or 'Sell'
    amount      TEXT,
    date        DATE        NOT NULL,
    company     TEXT,
    scraped_at  TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (member, ticker, date, type)
);
CREATE INDEX IF NOT EXISTS idx_congress_trades_ticker  ON congress_trades (ticker);
CREATE INDEX IF NOT EXISTS idx_congress_trades_date    ON congress_trades (date DESC);

-- ─── Options Flow (daily snapshot) ──────────────────────────────
CREATE TABLE IF NOT EXISTS options_flow_daily (
    id            BIGSERIAL PRIMARY KEY,
    ticker        TEXT        NOT NULL,
    option_type   TEXT        NOT NULL,  -- 'CALL' or 'PUT'
    strike        NUMERIC,
    expiry        DATE,
    volume        INTEGER,
    open_interest INTEGER,
    premium       NUMERIC,
    unusual       BOOLEAN     DEFAULT FALSE,
    trade_date    DATE        NOT NULL,
    scraped_at    TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (ticker, option_type, strike, expiry, trade_date)
);
CREATE INDEX IF NOT EXISTS idx_options_flow_ticker     ON options_flow_daily (ticker);
CREATE INDEX IF NOT EXISTS idx_options_flow_trade_date ON options_flow_daily (trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_options_flow_unusual    ON options_flow_daily (unusual) WHERE unusual = TRUE;

-- ─── Market Snapshots (daily close per ticker) ───────────────────
CREATE TABLE IF NOT EXISTS market_snapshots (
    id             BIGSERIAL PRIMARY KEY,
    ticker         TEXT    NOT NULL,
    price          NUMERIC,
    change_pct     NUMERIC,
    volume         BIGINT,
    market_cap     NUMERIC,
    snapshot_date  DATE    NOT NULL,
    scraped_at     TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (ticker, snapshot_date)
);
CREATE INDEX IF NOT EXISTS idx_market_snapshots_ticker ON market_snapshots (ticker);
CREATE INDEX IF NOT EXISTS idx_market_snapshots_date   ON market_snapshots (snapshot_date DESC);

-- ─── Short Interest (daily per ticker) ──────────────────────────
CREATE TABLE IF NOT EXISTS short_interest_daily (
    id             BIGSERIAL PRIMARY KEY,
    ticker         TEXT    NOT NULL,
    short_ratio    NUMERIC,
    short_percent  NUMERIC,
    shares_short   BIGINT,
    float_shares   BIGINT,
    snapshot_date  DATE    NOT NULL,
    scraped_at     TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (ticker, snapshot_date)
);
CREATE INDEX IF NOT EXISTS idx_short_interest_ticker ON short_interest_daily (ticker);
CREATE INDEX IF NOT EXISTS idx_short_interest_date   ON short_interest_daily (snapshot_date DESC);

-- ─── WHAL-119: New tables for deep scraper ───────────────────────

-- 1. Insider Trades (SEC EDGAR Form 4)
CREATE TABLE IF NOT EXISTS insider_trades (
    id               UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    ticker           TEXT        NOT NULL,
    owner_name       TEXT,
    owner_type       TEXT,
    transaction_type TEXT,
    shares           NUMERIC,
    price            NUMERIC,
    value            NUMERIC,
    filing_date      DATE,
    scraped_at       TIMESTAMPTZ DEFAULT now(),
    UNIQUE (ticker, owner_name, filing_date)
);
CREATE INDEX IF NOT EXISTS idx_insider_trades_ticker ON insider_trades (ticker, scraped_at DESC);

-- 2. Dark Pool Prints (Polygon.io block trades)
CREATE TABLE IF NOT EXISTS dark_pool_prints (
    id             UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    ticker         TEXT        NOT NULL,
    price          NUMERIC,
    size           NUMERIC,
    value          NUMERIC,
    execution_time TIMESTAMPTZ,
    scraped_at     TIMESTAMPTZ DEFAULT now(),
    UNIQUE (ticker, execution_time)
);
CREATE INDEX IF NOT EXISTS idx_dark_pool_ticker ON dark_pool_prints (ticker, scraped_at DESC);

-- 3. Options Flow (Polygon.io unusual activity)
CREATE TABLE IF NOT EXISTS options_flow (
    id             UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    ticker         TEXT        NOT NULL,
    strike         NUMERIC,
    expiry         DATE,
    call_put       TEXT,
    premium        NUMERIC,
    volume         INTEGER,
    open_interest  INTEGER,
    sentiment      TEXT,
    scraped_at     TIMESTAMPTZ DEFAULT now(),
    UNIQUE (ticker, strike, expiry, call_put, scraped_at)
);
CREATE INDEX IF NOT EXISTS idx_options_flow_ticker ON options_flow (ticker, scraped_at DESC);

-- 4. Congressional Trades (Quiver Quant)
CREATE TABLE IF NOT EXISTS congressional_trades (
    id               UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    politician       TEXT,
    ticker           TEXT        NOT NULL,
    transaction_type TEXT,
    amount_range     TEXT,
    trade_date       DATE,
    disclosure_date  DATE,
    scraped_at       TIMESTAMPTZ DEFAULT now(),
    UNIQUE (politician, ticker, trade_date)
);
CREATE INDEX IF NOT EXISTS idx_congressional_ticker ON congressional_trades (ticker, scraped_at DESC);

-- 5. Ticker Fundamentals (yfinance)
CREATE TABLE IF NOT EXISTS ticker_fundamentals (
    id            UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    ticker        TEXT        NOT NULL UNIQUE,
    pe_ratio      NUMERIC,
    market_cap    NUMERIC,
    earnings_date DATE,
    sector        TEXT,
    updated_at    TIMESTAMPTZ DEFAULT now()
);

-- ─── Crypto Trades (crypto_executor.py) ──────────────────────────────────────
CREATE TABLE IF NOT EXISTS crypto_trades (
    id            BIGSERIAL    PRIMARY KEY,
    order_id      TEXT         UNIQUE NOT NULL,
    ticker        TEXT         NOT NULL,          -- BTCUSD
    symbol        TEXT,                            -- BTC/USD
    side          TEXT         NOT NULL DEFAULT 'BUY',
    qty           NUMERIC      NOT NULL,
    entry_price   NUMERIC      NOT NULL,
    exit_price    NUMERIC,
    stop_loss     NUMERIC,
    target_1      NUMERIC,
    target_2      NUMERIC,
    pnl           NUMERIC,
    pnl_pct       NUMERIC,
    confidence    NUMERIC,
    score         INTEGER,
    ai_reasoning  TEXT,
    status        TEXT         NOT NULL DEFAULT 'open', -- open | closed
    partial_sold  BOOLEAN      NOT NULL DEFAULT false,
    exit_reason   TEXT,                            -- stop_loss | target_1 | target_2 | time_stop | force_close
    entry_time    TIMESTAMPTZ,
    exit_time     TIMESTAMPTZ,
    date          DATE,
    asset_class   TEXT         NOT NULL DEFAULT 'crypto',
    created_at    TIMESTAMPTZ  DEFAULT now()
);

CREATE INDEX IF NOT EXISTS crypto_trades_date_idx    ON crypto_trades(date);
CREATE INDEX IF NOT EXISTS crypto_trades_status_idx  ON crypto_trades(status);
CREATE INDEX IF NOT EXISTS crypto_trades_ticker_idx  ON crypto_trades(ticker);
