# Import necessary libraries
from flask import Flask, jsonify, request, render_template
from flask_cors import CORS
import pandas as pd
import sys
import os
from sqlalchemy import text
import subprocess  # Import subprocess for running shell commands
import requests # 
from requests.auth import HTTPBasicAuth

# ==============================================================================
# STEP 1: SYSTEM PATH, DATABASE IMPORT & UTILITIES
# ==============================================================================
# Ensure the 'scripts' directory is in the system path for module imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'scripts'))
from db_connection import get_sqlalchemy_engine, get_db_connection

# Initialize the Flask application
app = Flask(__name__, static_folder='src')
CORS(app, resources={r"/api/*": {"origins": "*"}}, methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"])

# Create a function to standardize various forms of Saigon/HCMC and Kuala Lumpur to a consistency name
def normalize_city_name(city_input):
    # Clean the input: remove extra whitespace and convert to lowercase for comparison
    clean_input = str(city_input).strip().lower()
    
    # Define acceptable variations
    saigon_variants = ["saigon", "ho chi minh", "ho chi minh city", "hcmc", "tphcm", "tp.hcm", "hcm" ]

    Kuala_Lumpur_variants = ["kuala lumpur", "kuala lumpur city", "kl" ]

    # Check if the input is in the list of variants
    if clean_input in saigon_variants:
        return "Saigon"
    
    elif clean_input in Kuala_Lumpur_variants:
        return "Kuala Lumpur"
    
    else:
        # Return the original title-cased name if it's a different city
        return city_input.title()

# Initialize the SQLAlchemy engine for Pandas Read operations
engine = get_sqlalchemy_engine()

# Create API Home page
@app.route('/')
def index():
    """Renders the main dashboard HTML page."""
    return render_template('index.html')

# ==============================================================================
# STEP 2: API ENDPOINTS (CRUD OPERATIONS & DATA RETRIEVAL)
# ==============================================================================

# --- 2.1: READ CURRENT SNAPSHOT (GET) ---
# API Endpoint 'api/current' for current dataset of all cities 
@app.route('/api/current', methods=['GET'])
def get_current_data():
    """
    Fetches the latest snapshot from the GOLD layer for dashboard cards and map.
    """
    try:
        # 1. Included temperature, humidity, and new rain metrics from the view
        query = 'SELECT city, aqi, pm25, pm10, temperature, humidity, current_rain, annual_total_rain, "time" FROM gold_latest_snapshot'
        df = pd.read_sql(query, engine) # Read the data from the database
        
        # 2. Format the 'time' column to a readable string
        if not df.empty and 'time' in df.columns:
            df['time'] = pd.to_datetime(df['time']).dt.strftime("%d/%m/%Y %H:%M")
            
        return jsonify(df.to_dict(orient='records')) # Return the data as JSON 
    except Exception as e:
        return jsonify({"error": str(e)}), 500  
    
# --- 2.2: READ HISTORY (GET) ---
# API Endpoint 'api/history' for history dataset of all cities
@app.route('/api/history', methods=['GET'])
def get_all_history():
    """
    Fetches the full historical dataset from the SILVER layer for the management table.
    """
    try:
        # This feeds the data management table on Dashboard (Frontend)
        query = "SELECT * FROM silver_air_quality ORDER BY recorded_at DESC LIMIT 500"
        df = pd.read_sql(query, engine)
        return jsonify(df.to_dict(orient='records'))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# API Endpoint 'api/history/<city>' for history data of a specific city
@app.route('/api/history/<city>', methods=['GET'])
def get_city_history(city):

    # Normalize the city name (e.g., HCMC -> Saigon)
    normalized_city = normalize_city_name(city)     
    """
    Fetches historical data for a specific city to populate charts.
    """
    try:
        # We use SELECT * to keep all existing data (PM25, temperature, etc.)
        # and add to_char() to create 'time_label' for the Chart.js X-axis.
        query = text("""
            SELECT *, to_char(recorded_at, 'HH24:MI') as time_label 
            FROM silver_air_quality 
            WHERE city_name = :city 
            ORDER BY recorded_at ASC
        """)
               
        # Pass the dictionary to params
        # Execute the query using the established engine
        df = pd.read_sql(query, engine, params={"city": normalized_city})

        # Fill NaN/Null values with 0 so the chart displays a clean baseline
        if not df.empty:
            # List of all numeric columns that need to be clean for Chart.js
            numeric_cols = ['rain_1h', 'pm25', 'pm10', 'co', 'no2', 'o3', 'so2', 'temperature', 'humidity']
            
            # Fill NaN with 0 only for columns that actually exist in the dataframe
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = df[col].fillna(0)     
                    
        return jsonify(df.to_dict(orient='records'))
    except Exception as e:
        return jsonify({"error": str(e)}), 500    

# --- 2.3: CREATE (POST) ---
@app.route('/api/records', methods=['POST'])
def add_record():
    """CREATE Operation: Manually add a new record to the Silver Layer."""
    data = request.json
    conn = get_db_connection()
    if not conn:
        return jsonify({"error": "Database connection failed"}), 500
    try:
        cur = conn.cursor()
        query = """
        INSERT INTO silver_air_quality (city_name, aqi, pm25, pm10, recorded_at)
        VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
        """
        cur.execute(query, (data['city'], data['aqi'], data['pm25'], data['pm10']))
        conn.commit()
        return jsonify({"message": "Record created successfully"}), 201
    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 400
    finally:
        conn.close()

# --- 2.4: UPDATE (PUT) ---
# ==============================================================================
# DATA MANAGEMENT: TARGETED UPDATE (AQI, PM2.5, PM10)
# ==============================================================================

@app.route('/api/update/<int:silver_id>', methods=['PUT'])
def update_air_quality(silver_id):
    """
    Updates targeted parameters in the silver_air_quality table.
    Aligned with UI: AQI, PM2.5, and PM10 only.
    """
    try:
        data = request.get_json()
        
        # Extract the five parameters from the updated popup
        aqi = data.get('aqi')
        pm25 = data.get('pm25')
        pm10 = data.get('pm10')
        temperature = data.get('temperature') # Added
        humidity = data.get('humidity')       # Added

        engine = get_sqlalchemy_engine()
        with engine.begin() as conn:
            # Updating the 5 UI fields + the metadata 'processed_at'
            query = text("""
                UPDATE silver_air_quality 
                SET aqi = :aqi, 
                    pm25 = :pm25, 
                    pm10 = :pm10,
                    temperature = :temp,
                    humidity = :hum,      
                    processed_at = CURRENT_TIMESTAMP
                WHERE silver_id = :id
            """)
            
            result = conn.execute(query, {
                'aqi': aqi,
                'pm25': pm25,
                'pm10': pm10,
                'temp': temperature,
                'hum': humidity,
                'id': silver_id
            })

            if result.rowcount == 0:
                return jsonify({"status": "error", "message": "Record ID not found"}), 404

        return jsonify({
            "status": "success", 
            "message": f"Successfully updated ID {silver_id} (AQI, PM2.5, PM10, Temp, Hum)."
        })

    except Exception as e:
        print(f"Update Error: {str(e)}")
        return jsonify({"status": "error", "message": "Failed to update database"}), 500
 
# --- 2.5: DELETE (DELETE) ---
@app.route('/api/records/<int:record_id>', methods=['DELETE'])
def delete_record(record_id):
    try:
        # Connect to the database
        engine = get_sqlalchemy_engine()
        
        with engine.begin() as conn:
            # We use 'silver_id' to match your table column exactly
            query = text("DELETE FROM silver_air_quality WHERE silver_id = :id")
            
            result = conn.execute(query, {"id": record_id})
            
            # Check if a row was actually deleted
            if result.rowcount == 0:
                return jsonify({"status": "error", "message": "Record not found"}), 404
            
        return jsonify({"status": "success", "message": "Record deleted successfully"})

    except Exception as e:
        print(f"Delete Error: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500 

# --- 2.6: Create an endpoint to run the scraper (scraping dataset) when user press "Refresh Button" from web client (frontend)
@app.route('/api/scrape', methods=['POST'])
def trigger_scrape():
    # --- CONFIGURATION FROM .ENV ---
    airflow_user = os.getenv("AIRFLOW_DB_USER", "airflow")
    airflow_password = os.getenv("AIRFLOW_DB_PASSWORD", "airflow")
    
    # Using the DAG ID verified in your air_quality_dag.py
    dag_id = "air_quality_Medallion_pipeline"
    
    # Using port 8081 based on your docker-compose mapping
    # Using 127.0.0.1 is more stable than 'localhost' on Windows
    airflow_url = f"http://127.0.0.1:8081/api/v1/dags/{dag_id}/dagRuns"

    try:
        print(f"--- Triggering Airflow DAG on port 8081: {dag_id} ---")
        
        # Trigger the DAG via POST request
        response = requests.post(
            airflow_url,
            json={}, 
            auth=HTTPBasicAuth(airflow_user, airflow_password),
            timeout=10
        )

        if response.status_code in [200, 201]:
            return jsonify({
                "status": "success", 
                "message": "Medallion Pipeline Triggered! Data is moving from Bronze to Gold."
            })
        elif response.status_code == 409:
            return jsonify({
                "status": "warning",
                "message": "The pipeline is already running. Please check the Airflow UI at http://localhost:8081."
            })
        elif response.status_code == 401:
            return jsonify({
                "status": "error",
                "message": "Authentication failed. Ensure AIRFLOW_DB_USER/PWD in .env matches Airflow UI login."
            })
        else:
            return jsonify({
                "status": "error", 
                "message": f"Airflow Error: {response.status_code}",
                "details": response.text
            }), response.status_code

    except requests.exceptions.ConnectionError:
        return jsonify({
            "status": "error", 
            "message": "Connection Refused. Ensure Airflow containers are running and port 8081 is open."
        }), 503
    except Exception as e:
        return jsonify({
            "status": "error", 
            "message": f"System Error: {str(e)}"
        }), 500

# Start the Flask server
if __name__ == '__main__':    
    app.run(port=8000, debug=True)