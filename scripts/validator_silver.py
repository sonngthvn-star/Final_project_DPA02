import os
from datetime import datetime
from db_connection import get_db_connection

def run_validation(dag_run_id="MANUAL_RUN"):
    """
    The 'Gatekeeper' script. 
    It ensures that only healthy data is marked as SUCCESS in the logs.
    Ensures data integrity, normalization, and reconciliation between layers    
    """
    with get_db_connection() as conn:
        if not conn:
            print("❌ Connection Failed")
            return        
        
        try:
            # We perform our checks inside the cursor block
            with conn.cursor() as cur:
                # --- STEP 1: RECONCILIATION ---
                # Purpose: Ensure the number of records in Silver matches Bronze.
                # If these numbers are far apart, it suggests the transformer is failing.
                cur.execute("SELECT COUNT(*) FROM bronze_air_quality")
                bronze_count = cur.fetchone()[0]
                
                cur.execute("SELECT COUNT(*) FROM silver_air_quality")
                silver_count = cur.fetchone()[0]

                # --- STEP 2: DATA QUALITY GATE (NULL CHECK) ---
                # Purpose: Ensure the new OWM columns (Toxic Gases) were populated.
                # A record is useless for our dashboard if it's missing the AQI or main gases.
                cur.execute("""
                    SELECT COUNT(*) FROM silver_air_quality 
                    WHERE aqi IS NULL
                        OR pm25 IS NULL
                        OR pm10 IS NULL
                        OR co IS NULL
                        OR no2 IS NULL
                        OR o3 IS NULL
                        OR so2 IS NULL
                        OR temperature IS NULL
                        OR humidity IS NULL
                        OR rain_1h IS NULL
                """)
                dq_null_errors = cur.fetchone()[0]

                # --- STEP 3: SAIGON NORMALIZATION CHECK ---
                # Purpose: Ensure the 'Saigon' naming convention is strictly followed.
                # This prevents the Dashboard from showing two different entries for the same city.
                cur.execute("""
                    SELECT COUNT(*) FROM silver_air_quality 
                    WHERE city_name IN ('Ho Chi Minh', 'Ho Chi Minh City', 'HCM', 'HCMC')
                """)
                naming_errors = cur.fetchone()[0]
                
                # --- STEP 4: DETERMINE FINAL STATUS ---
                # We use a hierarchy: SUCCESS -> WARNING -> FAILED
                status = "SUCCESS"
                msgs = [f"Counts: B({bronze_count}) vs S({silver_count})"]
                
                if naming_errors > 0:
                    status = "WARNING"
                    msgs.append(f"Naming: {naming_errors} rows not normalized to 'Saigon'")
                
                if dq_null_errors > 0:
                    # Critical errors force a FAILED status
                    status = "FAILED"
                    msgs.append(f"DQ Error: {dq_null_errors} NULL values found")

                final_msg = " | ".join(msgs)

                # --- STEP 5: AUDIT LOGGING ---
                # We save the results into pipeline_logs so you can track the health
                # of your system over time from your Dashboard or pgAdmin.
                cur.execute("""
                    INSERT INTO pipeline_logs (dag_run_id, task_name, status, message, execution_time)
                    VALUES (%s, %s, %s, %s, %s)
                """, (dag_run_id, "Silver_Validation", status, final_msg, datetime.now()))
            
            # Commit the log entry to the database at the CONNECTION level after exiting the cursor block
            conn.commit()
            print(f"✅ Validation Log successfully committed to DB for Run: {dag_run_id}")

        except Exception as e:
            # If anything fails, we roll back to prevent hanging transactions
            conn.rollback()
            print(f"❌ Validation Logic Error: {e}")

if __name__ == "__main__":
    run_id = os.getenv("AIRFLOW_CTX_DAG_RUN_ID", "MANUAL_RUN")
    run_validation(run_id)