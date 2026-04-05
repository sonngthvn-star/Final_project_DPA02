-- --- MEDALLION ARCHITECTURE SETUP ---

-- 1. BRONZE LAYER: Raw Data Landing
-- Purpose: Store the exact, original data from the API. If something goes wrong later, 
-- we can always re-process this raw data without calling the API again.
CREATE TABLE IF NOT EXISTS bronze_air_quality (
    id SERIAL PRIMARY KEY, -- Auto-incrementing ID
    city_name VARCHAR(100),
    raw_data JSONB, -- JSONB is an optimized format for storing JSON in Postgres
    extraction_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- Timestamp when the data was extracted
);

-- 2. SILVER LAYER: Cleaned & Validated Data
-- Purpose: Deduplicated, cleaned, and typed data. This is the "Source of Truth".
CREATE TABLE IF NOT EXISTS silver_air_quality (
    silver_id SERIAL PRIMARY KEY, -- Auto-incrementing ID
    city_name VARCHAR(100),
    country VARCHAR(100),
    aqi INTEGER CHECK (aqi >= 0), -- Validation: AQI cannot be negative
    pm25 NUMERIC(6,2),
    pm10 NUMERIC(6,2),
    temperature NUMERIC(5,2),
    humidity NUMERIC(5,2),
    recorded_at TIMESTAMP NOT NULL,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(city_name, recorded_at) -- Prevent duplicate entries for the same city/time
);

-- 3. GOLD LAYER: Aggregated Analytics
-- Purpose: Pre-calculated views for the Dashboard to ensure the UI loads instantly.
CREATE OR REPLACE VIEW gold_city_daily_summary AS
SELECT 
    city_name,
    country,
    DATE(recorded_at) as obs_date,
    ROUND(AVG(aqi), 2) as avg_aqi,
    MAX(aqi) as peak_aqi
FROM silver_air_quality
GROUP BY city_name, country, obs_date;

-- Professional Serving View for the Dashboard "Latest" status
CREATE OR REPLACE VIEW gold_latest_snapshot AS
WITH RankedData AS (
    SELECT 
        silver_id as id,
        city_name as city,
        aqi,
        pm25,
        pm10,
        temperature,
        humidity,
        recorded_at,
        -- Assigns '1' to the newest record for each city
        ROW_NUMBER() OVER(PARTITION BY city_name ORDER BY recorded_at DESC) as rn
    FROM silver_air_quality
)
SELECT 
    id,
    city,
    aqi,
    pm25,
    pm10,
    temperature,
    humidity,
    to_char(recorded_at, 'YYYY-MM-DD HH24:MI') as "time"
FROM RankedData
WHERE rn = 1 AND recorded_at > NOW() - INTERVAL '24 hours';

-- 4. PIPELINE LOGS
-- Required by DPA: Tracks if ETL jobs succeeded or failed.
CREATE TABLE IF NOT EXISTS pipeline_logs (
    log_id SERIAL PRIMARY KEY,
    dag_run_id VARCHAR(255),
    task_name VARCHAR(100),
    status VARCHAR(50), 
    message TEXT,
    execution_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);