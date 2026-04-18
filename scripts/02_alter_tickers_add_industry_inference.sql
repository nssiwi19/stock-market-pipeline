-- Add audit columns for inferred industry backfill
ALTER TABLE tickers
    ADD COLUMN IF NOT EXISTS industry_inferred TEXT,
    ADD COLUMN IF NOT EXISTS industry_inferred_confidence NUMERIC(6, 4),
    ADD COLUMN IF NOT EXISTS industry_inferred_method VARCHAR(50),
    ADD COLUMN IF NOT EXISTS industry_inferred_at TIMESTAMP WITH TIME ZONE;
