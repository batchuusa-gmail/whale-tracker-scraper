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
