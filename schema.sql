-- ENTSOE ETL Pipeline - PostgreSQL Schema
-- Target: Supabase PostgreSQL database

-- Table 1: Balancing Reserves
CREATE TABLE IF NOT EXISTS balancing_reserves (
    id SERIAL PRIMARY KEY,
    country_code VARCHAR(10) NOT NULL,
    datetime_utc TIMESTAMP WITH TIME ZONE NOT NULL,
    reserve_type TEXT NOT NULL,
    amount_mw FLOAT,
    price_eur FLOAT,
    inserted_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    UNIQUE(country_code, datetime_utc, reserve_type)
);

-- Table 2: Day Ahead Prices
CREATE TABLE IF NOT EXISTS day_ahead_prices (
    id SERIAL PRIMARY KEY,
    country_code VARCHAR(10) NOT NULL,
    datetime_utc TIMESTAMP WITH TIME ZONE NOT NULL,
    price_eur_per_mwh FLOAT NOT NULL,
    inserted_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    UNIQUE(country_code, datetime_utc)
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_balancing_reserves_datetime 
ON balancing_reserves(datetime_utc);

CREATE INDEX IF NOT EXISTS idx_balancing_reserves_country 
ON balancing_reserves(country_code);

CREATE INDEX IF NOT EXISTS idx_balancing_reserves_type 
ON balancing_reserves(reserve_type);

CREATE INDEX IF NOT EXISTS idx_day_ahead_prices_datetime 
ON day_ahead_prices(datetime_utc);

CREATE INDEX IF NOT EXISTS idx_day_ahead_prices_country 
ON day_ahead_prices(country_code);

-- Comments for documentation
COMMENT ON TABLE balancing_reserves IS 'Balancing reserves data from ENTSOE API';
COMMENT ON TABLE day_ahead_prices IS 'Day-ahead electricity prices from ENTSOE API';
COMMENT ON COLUMN balancing_reserves.country_code IS 'Country code (e.g., DE for Germany)';
COMMENT ON COLUMN balancing_reserves.datetime_utc IS 'Timestamp in UTC';
COMMENT ON COLUMN balancing_reserves.reserve_type IS 'Type of reserve (Primary, Secondary, Tertiary)';
COMMENT ON COLUMN balancing_reserves.amount_mw IS 'Reserve amount in MW';
COMMENT ON COLUMN balancing_reserves.price_eur IS 'Price in EUR per MW';
COMMENT ON COLUMN day_ahead_prices.price_eur_per_mwh IS 'Price in EUR per MWh'; 