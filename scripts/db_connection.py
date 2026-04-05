import psycopg2
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

def get_db_connection():
    """Works for Airflow (Internal)"""
    try:
        # Airflow needs these defaults
        host = os.getenv("DB_HOST", "postgres_dw") 
        port = os.getenv("DB_PORT_INTERNAL", "5432") 
        
        return psycopg2.connect(
            host=host,
            port=port,
            database=os.getenv("TARGET_POSTGRES_DB"),
            user=os.getenv("TARGET_POSTGRES_USER"),
            password=os.getenv("TARGET_POSTGRES_PASSWORD"),
            connect_timeout=3
        )
    except Exception as e:
        print(f"❌ Psycopg2 Error: {e}")
        return None

def get_sqlalchemy_engine():
    """Modified to detect if it's running on Windows or Docker"""
    user = os.getenv("TARGET_POSTGRES_USER")
    pw = os.getenv("TARGET_POSTGRES_PASSWORD")
    db = os.getenv("TARGET_POSTGRES_DB")

    # 1: Try Internal (Docker) address first
    try:
        internal_uri = f"postgresql://{user}:{pw}@postgres_dw:5432/{db}"
        engine = create_engine(internal_uri, connect_args={'connect_timeout': 1})
        with engine.connect() as conn:
            return engine
    except:
        # 2: If Internal fails, use External (Windows/Localhost)
        # This is what the Dashboard needs!
        external_uri = f"postgresql://{user}:{pw}@127.0.0.1:5433/{db}"
        return create_engine(external_uri)

if __name__ == "__main__":
    print("Testing PostgreSQL Connection...")
    connection = get_db_connection()
    if connection:
        print("✅ Connection Successful!")
        connection.close()
    else:
        print("❌ Connection Failed.")