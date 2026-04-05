'''
This script performs:
1. Scraping real-time Air Quality data from WAQI API.
2. Saving the raw API response to the PostgreSQL BRONZE LAYER.
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
# Your unique WAQI API token for authentication
# Fetch from .env via the OS environment
TOKEN = os.getenv("WAQI_API_TOKEN")

# The STATIONS_MAP provides backup stations for each city to ensure reliability.
# If the first station fails, the loop will attempt the next one in the list.
STATIONS_MAP = {
    "Hanoi": ["@8667", "@14731", "hanoi"],
    "Saigon": ["@8641", "@9137", "@13540", "saigon"],
    "Perth": ["@10811", "@10813", "perth"],
    "Bangkok": ["@8538", "@10373", "bangkok"],
    "Singapore": ["@5513", "@7109", "singapore"],
    "Kuala Lumpur": ["@10816", "@10817", "kuala lumpur"],
    "Jakarta": ["@10745", "@8674", "jakarta"],
    "Manila": ["@10842", "@10843", "manila"],
    "Beijing": ["@1437", "@3125", "beijing"],
    "Shanghai": ["@1451", "@1438", "shanghai"]
}

def fetch_raw_waqi_data(city_name, sids):
    """
    Step 1: Fetches the most recent available data from a list of 
    potential station IDs for a given city.
    """
    for sid in sids:
        # Construct the specific URL for this station
        url = f"https://api.waqi.info/feed/{sid}/?token={TOKEN}"
        
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status() # Check for HTTP errors (404, 500, etc.)
            data = response.json()
            
            # --- FIX: ELIMINATE SILENT FAILURE ---
            # If the API returns 'error', we must raise an Exception
            if data.get("status") == "error":
                error_msg = data.get("data", "Unknown API error")
                print(f"❌ API returned an error for {city_name}: {error_msg}")
                # Raising this ensures Airflow detects the failure
                raise Exception(f"WAQI API Error for {city_name}: {error_msg}")

            if data.get("status") == "ok":
                print(f"✅ Data fetched successfully for {city_name} (SID: {sid})")
                return data
                
        except requests.exceptions.RequestException as e:
            print(f"⚠️ Connection failed for {city_name} SID {sid}: {e}")
            continue # Try the next station in the list
            
    # If the loop finishes without returning 'ok' data
    raise Exception(f"CRITICAL: All station attempts failed for {city_name}.")

def save_to_bronze_layer(city_name, raw_payload):
    """
    STEP 2: LOAD TO BRONZE (POSTGRESQL)
    This fulfills the Bronze Layer requirement: storing the exact, original data.
    Data is stored in a JSONB column so we don't lose any detail from the original API.
    """
    # Initialize connection using your db_connection.py logic
    conn = get_db_connection()
    if not conn:
        return # Connection error is already printed in db_connection.py
    try:
        cur = conn.cursor()
        # SQL Insert targeting the 'bronze_air_quality' table defined in your schema.sql
        # We use %s placeholders to prevent SQL Injection.
        insert_query = "INSERT INTO bronze_air_quality (city_name, raw_data, extraction_timestamp) VALUES (%s, %s, %s)"

        # FIXED: Use Vietnam local time for the timestamp
        local_now = datetime.now(VN_TZ)

        # Execute the insert:
        # 1. city_name: The string name of the city.
        # 2. json.dumps(raw_payload): Converts the Python dictionary into a JSON string for Postgres.
        # 3. datetime.now(): Records exactly when this scrape happened.
        cur.execute(insert_query, (city_name, json.dumps(raw_payload), local_now)) # Use local_now for timestamp
        # Commit saves the changes to the database permanently
        conn.commit()
        print(f"✅ [Bronze Layer] {city_name}: Raw data pushed to PostgreSQL at {local_now.strftime('%Y-%m-%d %H:%M:%S')}")
    except Exception as e:
        print(f"❌ Bronze-Layer Database Error for {city_name}: {e}")
        conn.rollback() # Roll back the transaction if an error occurs
    finally:
        # Always close the cursor and connection to prevent 'ghost' sessions
        cur.close()
        conn.close()
    
def run_scraper():
    """THIS IS THE FUNCTION THE PIPELINE CALLS"""
    print("🚀 ETL Phase 1: Starting Scraper (Bronze Layer)...")    
    
    for city, sids in STATIONS_MAP.items():
        print(f"Processing {city}...")
        # Call Step 1: Fetch data
        raw_data = fetch_raw_waqi_data(city, sids)
        if raw_data:
            # Call Step 2: Save to Database
            save_to_bronze_layer(city, raw_data)
        else:
            print (f"⚠️ Result: No valid data found for {city} after trying all stations.") 

if __name__ == "__main__":
    run_scraper()