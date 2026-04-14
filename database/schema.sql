-- --- MEDALLION ARCHITECTURE SETUP ---

-- 1. BRONZE LAYER: Raw Data Landing - "Dual-Store" Strategy
-- Purpose: Store the exact, original data from the API + explicit columns for high-speed transformation.
-- we can always re-process this raw data without calling the API again.
CREATE TABLE IF NOT EXISTS bronze_air_quality (
    id SERIAL PRIMARY KEY, -- Auto-incrementing ID
    city_name VARCHAR(100),
    raw_data JSONB, -- Stores the full API response for forensic auditing    
    aqi NUMERIC,        
    pm25 NUMERIC,       
    pm10 NUMERIC,       
    temperature NUMERIC,
    humidity NUMERIC,
    co NUMERIC,
    no2 NUMERIC,
    o3 NUMERIC,
    so2 NUMERIC,
    nh3_raw NUMERIC,
    no_raw NUMERIC,
    rain_1h NUMERIC,
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
    co NUMERIC(8,3),
    no2 NUMERIC(8,3),
    o3 NUMERIC(8,3),
    so2 NUMERIC(8,3),    
    temperature NUMERIC(5,2),
    humidity NUMERIC(5,2),
    rain_1h NUMERIC(6,2),
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
-- This version uses a Common Table Expression (CTE) to calculate the running annual total
CREATE OR REPLACE VIEW gold_latest_snapshot AS
WITH AnnualTotals AS (
    -- Calculate the sum of rain for the current year per city
    -- Note: This looks at ALL records in Silver for the year, 
    -- ignoring the 24h dashboard filter.
    SELECT 
        city_name, 
        ROUND(SUM(rain_1h), 2) as total_annual_rain
    FROM silver_air_quality
    WHERE EXTRACT(YEAR FROM recorded_at) = EXTRACT(YEAR FROM CURRENT_DATE)
    GROUP BY city_name
),
RankedData AS (
    SELECT 
        s.silver_id as id,
        s.city_name as city,
        s.aqi,
        s.pm25,
        s.pm10,
        s.temperature,
        s.humidity,
        s.rain_1h,  -- Current intensity (e.g., last hour)
        s.recorded_at,
        -- Assigns '1' to the newest record for each city
        ROW_NUMBER() OVER(PARTITION BY city_name ORDER BY recorded_at DESC) as rn
    FROM silver_air_quality s
)
SELECT 
    r.id,
    r.city,
    r.aqi,
    r.pm25,
    r.pm10,
    r.temperature,
    r.humidity,
    r.rain_1h as current_rain,
    COALESCE(a.total_annual_rain, 0) as annual_total_rain,
    to_char(r.recorded_at, 'YYYY-MM-DD HH24:MI') as "time"
FROM RankedData r
LEFT JOIN AnnualTotals a ON r.city = a.city_name
WHERE r.rn = 1 AND r.recorded_at > NOW() - INTERVAL '24 hours';

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