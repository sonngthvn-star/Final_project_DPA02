import pandas as pd
import numpy as np
from datetime import datetime
import json
import os
import sys

from db_connection import get_db_connection

# Logic thresholds for smoothing
# Configuration mapping including Country and typical ranges for smoothing
THRESHOLDS = {
    'Hanoi': {'country': 'Vietnam', 'aqi_min': 60, 'aqi_range': (150, 180), 'temp_range': (14.0, 18.0), 'hum_range': (70, 90), 'ratio': 1.6},
    'Saigon': {'country': 'Vietnam', 'aqi_min': 40, 'aqi_range': (80, 110), 'temp_range': (28.0, 33.0), 'hum_range': (60, 85), 'ratio': 1.6},
    'Perth': {'country': 'Australia', 'aqi_min': 5, 'aqi_range': (10, 35), 'temp_range': (15.0, 25.0), 'hum_range': (30, 50), 'ratio': 1.3},
    'Bangkok': {'country': 'Thailand', 'aqi_min': 40, 'aqi_range': (100, 160), 'temp_range': (25.0, 32.0), 'hum_range': (60, 80), 'ratio': 1.3},
    'Singapore': {'country': 'Singapore', 'aqi_min': 20, 'aqi_range': (40, 70), 'temp_range': (26.0, 31.0), 'hum_range': (70, 90), 'ratio': 1.3},
    'Kuala Lumpur': {'country': 'Malaysia', 'aqi_min': 30, 'aqi_range': (50, 90), 'temp_range': (25.0, 32.0), 'hum_range': (70, 90), 'ratio': 1.3},
    'Jakarta': {'country': 'Indonesia', 'aqi_min': 50, 'aqi_range': (120, 170), 'temp_range': (26.0, 33.0), 'hum_range': (70, 95), 'ratio': 1.3},
    'Manila': {'country': 'Philippines', 'aqi_min': 40, 'aqi_range': (70, 120), 'temp_range': (26.0, 32.0), 'hum_range': (70, 90), 'ratio': 1.3},
    'Beijing': {'country': 'China', 'aqi_min': 30, 'aqi_range': (50, 200), 'temp_range': (-5.0, 5.0), 'hum_range': (20, 40), 'ratio': 1.3},
    'Shanghai': {'country': 'China', 'aqi_min': 30, 'aqi_range': (50, 180), 'temp_range': (3.0, 33.0), 'hum_range': (40, 70), 'ratio': 1.3}
}

def clean_value(val):
    """Handles cases where API returns '-' or None"""
    if val is None or val == "-":
        return None
    try:
        return float(val)
    except ValueError:
        return None

def transform_to_silver():
    conn = get_db_connection()
    if not conn: return

    try:
        cur = conn.cursor()
        
        # 1. GET THE "HIGH-WATERMARK"
        # Find the most recent record we've already processed into Silver
        cur.execute("SELECT MAX(recorded_at) FROM silver_air_quality")
        last_processed_ts = cur.fetchone()[0]
        
        # If Silver is empty (first run), we'll take all data from the last 24 hours
        if not last_processed_ts:
            query = """
                SELECT city_name, raw_data, extraction_timestamp 
                FROM bronze_air_quality 
                WHERE extraction_timestamp > NOW() - INTERVAL '24 hours'
            """
            print("Initial run: Fetching last 24h of data.")
        else:
            # 2. FETCH ONLY NEW DATA
            # We pull everything from Bronze that is strictly newer than our last Silver record
            query = """
                SELECT city_name, raw_data, extraction_timestamp 
                FROM bronze_air_quality 
                WHERE extraction_timestamp > %s
            """
            print(f"Incremental run: Fetching data newer than {last_processed_ts}")

        df_bronze = pd.read_sql(query, conn, params=(last_processed_ts,) if last_processed_ts else None)
        
        if df_bronze.empty:
            print("ℹ️ No new data found in Bronze since last run.")
            return

        silver_records = []
        for _, row in df_bronze.iterrows():
            city = row['city_name']
            raw = row['raw_data']
            final_recorded_at = row['extraction_timestamp']
            t = THRESHOLDS.get(city)
            if not t: continue

            # --- DATA EXTRACTION & CLEANING ---
            # Extract and parse raw values
            raw_aqi = clean_value(raw.get('aqi'))
            iaqi = raw.get('iaqi', {})
            raw_pm25 = clean_value(iaqi.get('pm25', {}).get('v'))
            raw_pm10 = clean_value(iaqi.get('pm10', {}).get('v'))
            raw_temp = clean_value(iaqi.get('t', {}).get('v'))
            raw_hum = clean_value(iaqi.get('h', {}).get('v'))

            """ --- SMOOTHING LOGIC FOR NULL VALUES ---
            - Due to using the WAQI API free-account, the user is unable to fetch full air quality data, leading to missing values.`
            - This is a temporary solution to ensure data consistency for downstream processing (running for the purpose of the DPA Project only)
            - This logic applies to AQI, Temperature, Humidity, PM2.5, and PM10.
            - This solution will be removed when a paid API key is acquired for full-fetched real-time data
            """

            # Handle NULL AQI
            final_aqi = int(raw_aqi) if (raw_aqi and raw_aqi >= t['aqi_min']) else np.random.randint(*t['aqi_range'])

            # Handle NULL Temperature        
            final_temp = raw_temp if raw_temp else round(np.random.uniform(*t['temp_range']), 1)
            
            # Handle NULL Humidity
            final_hum = raw_hum if raw_hum else round(np.random.uniform(*t['hum_range']), 1)

            # Fix PM2.5 (must be roughly 75-85% of AQI if missing or low)
            final_pm25 = raw_pm25 if (raw_pm25 and raw_pm25 > (final_aqi * 0.5)) else round(final_aqi * np.random.uniform(0.7, 0.8), 1)
            
            # Fix PM10 (must be larger than PM2.5)
            final_pm10 = raw_pm10 if (raw_pm10 and raw_pm10 > final_pm25) else round(final_pm25 * t['ratio'], 1)

            silver_records.append((
                city, t['country'], final_aqi, final_pm25, final_pm10, 
                final_temp, final_hum, final_recorded_at
            )) 

            # --- THIS SCRIPTS WILL BE REPLACE THE TEMPORARY SOLUTION FOR FETCHING FULL DATA WITH A PAID API KEY ---
            # iaqi = raw.get('iaqi', {})            
            # # FIX: Remove np.random and use real data only
            # # If the API returns None or "-", it stays None in our database
            # silver_records.append((
            #     city, 
            #     t['country'], 
            #     clean_value(raw.get('aqi')), 
            #     clean_value(iaqi.get('pm25', {}).get('v')), 
            #     clean_value(iaqi.get('pm10', {}).get('v')), 
            #     clean_value(iaqi.get('t', {}).get('v')), 
            #     clean_value(iaqi.get('h', {}).get('v')), 
            #     final_recorded_at
            # ))            
            
        # 3. UPSERT TO SILVER
        upsert_sql = """
            INSERT INTO silver_air_quality (city_name, country, aqi, pm25, pm10, temperature, humidity, recorded_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (city_name, recorded_at) DO NOTHING;
        """
        cur.executemany(upsert_sql, silver_records)
        conn.commit()
        print(f"✅ Successfully processed {len(silver_records)} new records.")

    except Exception as e:
        print(f"❌ Silver Transformation Error: {e}")
        conn.rollback()
    finally:
        conn.close()       

if __name__ == "__main__":
    transform_to_silver()