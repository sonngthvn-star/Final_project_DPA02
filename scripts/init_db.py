import psycopg2
from psycopg2 import sql
import os
from pathlib import Path

def run_full_setup():
    """
    Orchestrates the database setup in two distinct phases:
    Step A: Physical Database Creation (System Level)
    Step B: Medallion Table Initialization (Schema Level)
    """
    # Configuration fetched from environment variables injected by Docker
    user = os.getenv("TARGET_POSTGRES_USER")
    password = os.getenv("TARGET_POSTGRES_PASSWORD")
    host = os.getenv("DB_HOST", "postgres_dw")
    port = os.getenv("DB_PORT_INTERNAL", "5432")
    target_db = os.getenv("TARGET_POSTGRES_DB")
    root_db = os.getenv("DB_ROOT_NAME", "postgres")

    # Path to the SQL DDL script inside the container
    schema_path = Path("/opt/airflow/database/schema.sql")

    try:
        # --- STEP A: CREATE THE PHYSICAL DATABASE ---
        # We connect to the 'postgres' default DB because we can't connect 
        # to the target_db before it actually exists.
        print(f"🔄 Step A: Checking if database '{target_db}' exists...")
        conn = psycopg2.connect(dbname=root_db, user=user, password=password, host=host, port=port)
        conn.autocommit = True  # Required for CREATE DATABASE commands
        cur = conn.cursor()
        
        cur.execute(sql.SQL("SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s"), [target_db])
        if not cur.fetchone():
            print(f"✨ Creating physical database: {target_db}")
            cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(target_db)))
        else:
            print(f"ℹ️ Database '{target_db}' already exists. Skipping creation.")
        
        cur.close()
        conn.close()

        # --- STEP B: CREATE THE MEDALLION TABLES ---
        # Now we connect directly to our new target_db to run the schema script.
        print(f"🔄 Step B: Initializing Medallion tables in '{target_db}'...")
        conn = psycopg2.connect(dbname=target_db, user=user, password=password, host=host, port=port)
        cur = conn.cursor()

        if schema_path.exists():
            with open(schema_path, "r", encoding='utf-8') as f:
                sql_script = f.read()
            cur.execute(sql_script)
            conn.commit()
            print("✅ Success: Bronze, Silver, and Gold structures initialized.")
        else:
            raise FileNotFoundError(f"❌ Critical Error: Schema file NOT FOUND at {schema_path}")

    except Exception as e:
        print(f"❌ Database Initialization Failed: {e}")
        raise e 
    finally:
        if 'conn' in locals(): conn.close()

if __name__ == "__main__":
    run_full_setup()