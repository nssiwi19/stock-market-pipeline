-- Add daily_prices OHLCV logic constraint on existing DBs.
ALTER TABLE daily_prices
    DROP CONSTRAINT IF EXISTS check_price_logic;

ALTER TABLE daily_prices
    ADD CONSTRAINT check_price_logic CHECK (
        low_price <= high_price
        AND open_price BETWEEN low_price AND high_price
        AND close_price BETWEEN low_price AND high_price
        AND volume >= 0
    );
