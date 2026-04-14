import pandas as pd
import numpy as np
from datetime import datetime
import json
import os
import sys

from db_connection import get_db_connection
"""
-- STEP 1: Define US EPA Standard Breakpoints --
Each pollutant has a different concentration-to-AQI mapping.
Format: (Min Concentration, Max Concentration, Min AQI, Max AQI)
"""
BREAKPOINTS = {
    'pm25': [(0, 12, 0, 50), (12.1, 35.4, 51, 100), (35.5, 55.4, 101, 150), (55.5, 150.4, 151, 200), (150.5, 250.4, 201, 300), (250.5, 500.4, 301, 500)],
    'pm10': [(0, 54, 0, 50), (55, 154, 51, 100), (155, 254, 101, 150), (255, 354, 151, 200), (355, 424, 201, 300), (425, 604, 301, 500)],
    'co': [(0, 4.4, 0, 50), (4.5, 9.4, 51, 100), (9.5, 12.4, 101, 150), (12.5, 15.4, 151, 200), (15.5, 30.4, 201, 300), (30.5, 50.4, 301, 500)],
    'no2': [(0, 53, 0, 50), (54, 100, 51, 100), (101, 360, 101, 150), (361, 649, 151, 200), (650, 1249, 201, 300), (1250, 2049, 301, 500)],
    'o3': [(0, 54, 0, 50), (55, 70, 51, 100), (71, 85, 101, 150), (86, 105, 151, 200), (106, 200, 201, 300)],
    'so2': [(0, 35, 0, 50), (36, 75, 51, 100), (76, 185, 101, 150), (186, 304, 151, 200), (305, 604, 201, 300), (605, 1004, 301, 500)]
}

def calculate_sub_aqi(conc, pollutant):
    """
    -- STEP 2: The Linear Interpolation Formula --
    This mathematical function finds which breakpoint the current concentration falls into
    and calculates a precise AQI score between 0 and 500.
    """
    if conc is None or conc < 0: return 0
    for low_c, high_c, low_a, high_a in BREAKPOINTS.get(pollutant, []):
        if low_c <= conc <= high_c:
            # The official EPA formula: 
            # AQI = [(AQI_high - AQI_low)/(Conc_high - Conc_low)] * (Current_Conc - Conc_low) + AQI_low
            return ((high_a - low_a) / (high_c - low_c)) * (conc - low_c) + low_a
    return 500 if conc > 0 else 0

def transform_to_silver():
    conn = get_db_connection()
    if not conn: return
    try:
        cur = conn.cursor()
        """
        -- STEP 3: Incremental Loading --        
        We only fetch records from Bronze that haven't been processed into Silver yet.
        This prevents duplicate work and keeps the pipeline fast.
        """
        query = """
            SELECT city_name, pm25, pm10, co, no2, o3, so2, temperature, humidity, rain_1h, extraction_timestamp 
            FROM bronze_air_quality 
            WHERE extraction_timestamp > (SELECT COALESCE(MAX(recorded_at), '1970-01-01') FROM silver_air_quality)
        """
        df = pd.read_sql(query, conn)
        if df.empty:
            print("ℹ️ No new data in Bronze to transform.")
            return

        silver_records = []
        for _, row in df.iterrows():
            """
            -- STEP 4: Unit Conversion & Sub-Index Calculation --
            OWM provides CO in micrograms (ug). EPA requires milligrams (mg).
            Division by 1000 is the correct way to convert this
            """
            co_mg = row['co'] / 1000 if row['co'] else 0
            
            sub_indices = {
                'pm25': calculate_sub_aqi(row['pm25'], 'pm25'),
                'pm10': calculate_sub_aqi(row['pm10'], 'pm10'),
                'co':   calculate_sub_aqi(co_mg, 'co'),
                'no2':  calculate_sub_aqi(row['no2'], 'no2'),
                'o3':   calculate_sub_aqi(row['o3'], 'o3'),
                'so2':  calculate_sub_aqi(row['so2'], 'so2')
            }
            
            """
            -- STEP 5: Highest Pollutant Wins (The EPA Rule) --
            The overall AQI is the maximum value of all individual pollutant indices.
            """
            final_aqi = round(max(sub_indices.values()))
            
            """
            -- STEP 6: Enrichment --
            We add 'Country' data which isn't in the API but is needed for Dashboard filters.
            """
            country = "Australia" if row['city_name'] == "Perth" else "Vietnam" if row['city_name'] in ["Saigon", "Hanoi"] else "Other"

            silver_records.append((
                row['city_name'],
                country,
                final_aqi,
                row['pm25'],
                row['pm10'], 
                row['co'],
                row['no2'],
                row['o3'],
                row['so2'],
                row['temperature'],
                row['humidity'],
                row['rain_1h'],
                row['extraction_timestamp']
            ))

        """
        -- STEP 7: Upsert to Silver Layer --
        'ON CONFLICT' ensures that if we run this twice, we don't get duplicate rows for the same time.
        """
        upsert_sql = """
            INSERT INTO silver_air_quality (
                city_name,
                country,
                aqi,
                pm25,
                pm10,
                co,
                no2,
                o3,
                so2, 
                temperature,
                humidity,
                rain_1h,
                recorded_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (city_name, recorded_at) DO NOTHING;
        """
        cur.executemany(upsert_sql, silver_records)
        conn.commit()
        print(f"✅ Phase 3: {len(silver_records)} records moved to Silver Layer.")
        
    finally:
        conn.close()   

if __name__ == "__main__":
    transform_to_silver()