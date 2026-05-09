-- =============================================================
-- GSE Financial DB — Supabase Setup
-- Run this once in the Supabase SQL Editor to create the schema.
-- Dashboard → SQL Editor → New query → paste → Run
-- =============================================================


-- Daily trading fact table
CREATE TABLE IF NOT EXISTS daily_trading (
    id                      SERIAL PRIMARY KEY,
    date                    DATE           NOT NULL,
    share_code              VARCHAR(20)    NOT NULL,
    year_high               NUMERIC(12,4),
    year_low                NUMERIC(12,4),
    prev_closing_vwap       NUMERIC(12,4),
    opening_price           NUMERIC(12,4),
    last_transaction_price  NUMERIC(12,4),
    closing_vwap            NUMERIC(12,4),
    price_change            NUMERIC(12,4),
    closing_bid             NUMERIC(12,4),
    closing_offer           NUMERIC(12,4),
    total_shares_traded     NUMERIC(20,4),
    total_value_traded      NUMERIC(20,4),
    source_file             VARCHAR(100),
    loaded_at               TIMESTAMPTZ    DEFAULT NOW(),
    CONSTRAINT uq_date_share UNIQUE (date, share_code)
);

-- Index for fast date-range queries (used by Power BI)
CREATE INDEX IF NOT EXISTS idx_daily_trading_date
    ON daily_trading (date DESC);

-- Index for per-stock queries (used by drillthrough page)
CREATE INDEX IF NOT EXISTS idx_daily_trading_share_code
    ON daily_trading (share_code);

-- Composite index for the most common query pattern
CREATE INDEX IF NOT EXISTS idx_daily_trading_date_share
    ON daily_trading (date DESC, share_code);


-- Verify
SELECT 'daily_trading table ready.' AS status;
