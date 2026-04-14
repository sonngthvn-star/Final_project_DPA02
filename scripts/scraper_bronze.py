'''
This script performs:
1. Fetching real-time Air Quality data from OpenWeatherMap API
2. Saving the raw API response to the PostgreSQL BRONZE LAYER: DUAL-STORE STRATEGY
'''

import requests
import json
from datetime import datetime
import pytz # Added for Timezone support
import os
import sys

from db_connection import get_db_connection

# Define Vietnam Timezone
VN_TZ = pytz.timezone('Asia/Ho_Chi_Minh')

# --- CONFIGURATION & CONSTANTS ---
API_KEY = os.getenv("OPENWEATHER_API_KEY")

# OWM requires Lat/Lon. Coordinates for city list:
CITY_COORDS = {
    "Hanoi": {"lat": 21.0285, "lon": 105.8542},
    "Saigon": {"lat": 10.7626, "lon": 106.6602},
    "Perth": {"lat": -31.9505, "lon": 115.8605},
    "Bangkok": {"lat": 13.7563, "lon": 100.5018},
    "Singapore": {"lat": 1.3521, "lon": 103.8198},
    "Kuala Lumpur": {"lat": 3.1390, "lon": 101.6869},
    "Jakarta": {"lat": -6.2088, "lon": 106.8456},
    "Manila": {"lat": 14.5995, "lon": 120.9842},
    "Beijing": {"lat": 39.9042, "lon": 116.4074},
    "Shanghai": {"lat": 31.2304, "lon": 121.4737}
}

def get_owm_data(city_name, lat, lon):
    """
    Step 1: Dual-Fetch Strategy
    Fetches Air Pollution and Current Weather for gapless data.
    """
    p_url = f"http://api.openweathermap.org/data/2.5/air_pollution?lat={lat}&lon={lon}&appid={API_KEY}"
    w_url = f"http://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API_KEY}&units=metric"
    
    try:
        p_res = requests.get(p_url, timeout=10)
        w_res = requests.get(w_url, timeout=10)
        p_res.raise_for_status()
        w_res.raise_for_status()
        
        p_data = p_res.json()['list'][0]
        w_data = w_res.json()
        
        return {
            "aqi": p_data['main']['aqi'],
            "components": p_data['components'],
            "weather": w_data['main'],
            "raw_payload": {
                "pollution": p_res.json(),
                "weather": w_res.json()
            }
        }
    except Exception as e:
        print(f"❌ OWM Fetch Error for {city_name}: {e}")
        return None

def save_to_bronze_layer(city_name, data_package):
    """
    STEP 2: LOAD TO BRONZE (DUAL-STORE STRATEGY)
    
    This function implements the Medallion Architecture's Bronze Layer requirements:
    1. SOURCE INTEGRITY: The 'raw_data' column (JSONB) stores the 100% original API response.
    2. PERFORMANCE: Key pollutants and weather metrics are extracted into separate columns 
       during the INSERT to allow for faster querying and transformation in the Silver Layer.
    """
    # Enforce Normalization
    if any(alias in city_name for alias in ["Ho Chi Minh", "HCM", "HCMC"]):
        city_name = "Saigon"

    conn = get_db_connection()
    if not conn: return
    
    try:
        cur = conn.cursor()

        # --- PRE-EXTRACTION PARSING ---
        # We navigate the API structure in memory before sending it to the database.
        # This allows us to populate individual columns in a single database transaction.              
        # --- OWM SPECIFIC PARSING ---
        # Note: 'data_package' is the dict returned by get_owm_data
        comp = data_package.get('components', {})
        weather = data_package.get('weather', {}) 
           
        # CORRECTED: Access 'weather' from inside 'raw_payload'
        raw_payload = data_package.get('raw_payload', {})
        raw_weather = raw_payload.get('weather', {})
        rain_data = raw_weather.get('rain', {})                   
        
        # --- DUAL-STORE INSERT QUERY ---
        # This query populates both the JSONB 'blob' and the individual 'atomic' columns.
        insert_query = """
            INSERT INTO bronze_air_quality (
                city_name,
                raw_data,           -- [STORE 1: JSONB] Full integrity record
                aqi,                -- [STORE 2: COLUMN] Individual metric for speed
                pm25,               -- [STORE 2: COLUMN] Individual metric for speed
                pm10,               -- [STORE 2: COLUMN] Individual metric for speed
                temperature,        -- [STORE 2: COLUMN] (Mapped from 't' in API)
                humidity,           -- [STORE 2: COLUMN] (Mapped from 'h' in API)
                co,                 -- [STORE 2: COLUMN] Expansion Pollutant
                no2,                -- [STORE 2: COLUMN] Expansion Pollutant
                o3,                 -- [STORE 2: COLUMN] Expansion Pollutant 
                so2,                -- [STORE 2: COLUMN] Expansion Pollutant
                no_raw,             -- [STORE 2: COLUMN] Expansion Pollutant
                nh3_raw,            -- [STORE 2: COLUMN] Expansion Pollutant
                rain_1h,            -- [STORE 2: COLUMN] Rain volume for the last 1 hour
                extraction_timestamp
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        # --- DATA MAPPING ---
#       # Mapping the API's internal keys to our table column names.        
        values = (
            city_name,
            json.dumps(data_package['raw_payload']),    # Full original API response. Converts the whole dict to a JSON string for JSONB storage
            data_package.get('aqi'),                    # Top-level AQI value
            comp.get('pm2_5'),                          # OWM uses 'pm2_5', not 'pm25'. Nested PM2.5 value
            comp.get('pm10'),                           # Nested PM10 value
            weather.get('temp'),                        # Nested temperature value
            weather.get('humidity'),                    # Nested humidity value
            comp.get('co'),                             # Nested CO value (Carbon Monoxide)
            comp.get('no2'),                            # Nested NO2 value (Nitrogen Dioxide)
            comp.get('o3'),                             # Nested O3 value (Ozone)
            comp.get('so2'),                            # Nested SO2 value (Sulfur Dioxide)
            comp.get('no'),                             # Nested NO value (Nitric Oxide). Stored for future use
            comp.get('nh3'),                            # Nested NH3 value (Ammonia). Stored for future use
            rain_data.get('1h', 0),                     # Rain volume for the last 1 hour
            datetime.now(VN_TZ)                         # Local Vietnam timestamp for auditing
        )

        cur.execute(insert_query, values)
        conn.commit()
        print(f"✅ [Bronze] {city_name}: Data ingested.")
    except Exception as e:
        print(f"❌ Error: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

def run_scraper():
    """THIS IS THE FUNCTION THE PIPELINE CALLS"""
    print("🚀 ETL Phase: Starting OWM Scraper (Bronze Layer)...")    
    
    for city, coords in CITY_COORDS.items():
        print(f"Processing {city}...")
        data = get_owm_data(city, coords['lat'], coords['lon'])
        if data:
            save_to_bronze_layer(city, data)
        else:
            print(f"⚠️ Result: Failed to fetch OWM data for {city}.")

if __name__ == "__main__":
    run_scraper()