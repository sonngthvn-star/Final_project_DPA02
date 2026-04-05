import os
from datetime import datetime
from db_connection import get_db_connection

def run_validation(dag_run_id="MANUAL_RUN"):
    """
    Robust Silver Layer Validator:
    - Resource Safety: Uses 'with' context managers.
    - Normalization: Enforces 'Saigon' naming standard.
    - DQ Gate: Checks for NULLs across AQI, PM25, Temperature, and Humidity.
    - COMMIT FIX: Explicit connection-level commit for Airflow persistence.
    """
    with get_db_connection() as conn:
        if not conn:
            print("❌ Connection Failed")
            return        
        
        try:
            # We perform our checks inside the cursor block
            with conn.cursor() as cur:
                # --- STEP A: RECONCILIATION ---
                cur.execute("SELECT COUNT(*) FROM bronze_air_quality")
                bronze_count = cur.fetchone()[0]
                
                cur.execute("SELECT COUNT(*) FROM silver_air_quality")
                silver_count = cur.fetchone()[0]

                # --- STEP B: SAIGON NORMALIZATION CHECK ---
                cur.execute("""
                    SELECT COUNT(*) FROM silver_air_quality 
                    WHERE city_name IN ('Ho Chi Minh', 'Ho Chi Minh City')
                """)
                naming_errors = cur.fetchone()[0]

                # --- STEP C: DATA QUALITY GATE (NULL CHECK) ---
                cur.execute("""
                    SELECT COUNT(*) FROM silver_air_quality 
                    WHERE aqi IS NULL 
                       OR pm25 IS NULL 
                       OR temperature IS NULL
                       OR humidity IS NULL
                """)
                dq_null_errors = cur.fetchone()[0]

                # --- STEP D: DETERMINE STATUS ---
                status = "SUCCESS"
                msgs = [f"Counts: B({bronze_count}) vs S({silver_count})"]
                
                if naming_errors > 0:
                    status = "WARNING"
                    msgs.append(f"Naming: {naming_errors} rows not set to 'Saigon'")
                
                if dq_null_errors > 0:
                    status = "FAILED"
                    msgs.append(f"DQ Error: {dq_null_errors} NULL values found")

                final_msg = " | ".join(msgs)

                # --- STEP E: INSERT LOG ---
                cur.execute("""
                    INSERT INTO pipeline_logs (dag_run_id, task_name, status, message, execution_time)
                    VALUES (%s, %s, %s, %s, %s)
                """, (dag_run_id, "Silver_Validation", status, final_msg, datetime.now()))
            
            # CRITICAL FIX: Commit at the CONNECTION level after exiting the cursor block
            conn.commit()
            print(f"✅ Validation Log successfully committed to DB for Run: {dag_run_id}")

        except Exception as e:
            # If anything fails, we roll back to prevent hanging transactions
            conn.rollback()
            print(f"❌ Validation Logic Error: {e}")

if __name__ == "__main__":
    run_id = os.getenv("AIRFLOW_CTX_DAG_RUN_ID", "MANUAL_RUN")
    run_validation(run_id)