-- =========================================================================
-- E15: STOCK MARKET BIG DATA PIPELINE - DATABASE INITIALIZATION SCRIPT
-- RUN THIS SCRIPT IN YOUR SUPABASE SQL EDITOR
-- =========================================================================

-- 1. Table: tickers
-- Description: Stores baseline profile information for each stock.
CREATE TABLE IF NOT EXISTS tickers (
    ticker VARCHAR(10) PRIMARY KEY,
    exchange VARCHAR(10) NOT NULL, -- HOSE or HNX
    industry VARCHAR(255),
    company_name TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now())
);

-- 2. Table: daily_prices
-- Description: Time-series table for storing daily OHLCV (Adjusted) data
CREATE TABLE IF NOT EXISTS daily_prices (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    ticker VARCHAR(10) REFERENCES tickers(ticker),
    trading_date DATE NOT NULL,
    open_price NUMERIC(15, 2),
    high_price NUMERIC(15, 2),
    low_price NUMERIC(15, 2),
    close_price NUMERIC(15, 2), -- Adjusted Close
    volume BIGINT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now()),
    UNIQUE(ticker, trading_date) -- Composite unique constraint to prevent duplicates during upsert
);

-- Optimize queries by creating Index for filtering by Date and Ticker
CREATE INDEX IF NOT EXISTS idx_daily_prices_date ON daily_prices (trading_date);
CREATE INDEX IF NOT EXISTS idx_daily_prices_ticker ON daily_prices (ticker);

-- 3. Table: financial_reports
-- Description: Fundamental data (Quarterly and Annual)
CREATE TABLE IF NOT EXISTS financial_reports (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    ticker VARCHAR(10) REFERENCES tickers(ticker),
    report_type VARCHAR(20) NOT NULL, -- 'quarterly' or 'yearly'
    period VARCHAR(20) NOT NULL, -- e.g., 'Q1-2025' or '2025'
    
    -- Key Metrics (in Billions VND or standard units, standardizing is important)
    revenue NUMERIC(20, 2),
    profit_after_tax NUMERIC(20, 2),
    total_assets NUMERIC(20, 2),
    total_liabilities NUMERIC(20, 2),
    equity NUMERIC(20, 2),
    eps NUMERIC(15, 2),
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now()),
    UNIQUE(ticker, report_type, period)
);

CREATE INDEX IF NOT EXISTS idx_financial_reports_ticker ON financial_reports (ticker);

-- Note: Ensure that the 'pgcrypto' or 'uuid-ossp' extension is enabled in Supabase 
-- for uuid_generate_v4() to work (it is enabled by default on new projects).
