import time
# from loguru import logger # Professional logging (included in your docker-compose)
from logger_config import pipeline_logger as logger # Use our new config
# Import your specialized modules
import init_db
import scraper_bronze
import transformer_silver
import validator_silver

def run_bronze_layer():
    """Delegates to scraper_bronze.py"""
    logger.info("--- Phase 1: Scraping Raw Data (Bronze) ---")
    start = time.time()
    scraper_bronze.run_scraper()
    logger.success(f"Bronze Phase completed in {round(time.time() - start, 2)}s")

def run_silver_layer():
    """Delegates to transformer_silver.py"""
    logger.info("--- Phase 2: Transforming to Silver Layer ---")
    start = time.time()
    transformer_silver.transform_to_silver()
    logger.success(f"Silver Phase completed in {round(time.time() - start, 2)}s")

def run_validation(dag_run_id="MANUAL_RUN", **kwargs):
    """Delegates to validator_silver.py with Airflow Context"""
    logger.info(f"--- Phase 3: Executing Validation Checks (Run: {dag_run_id}) ---")
    start = time.time()
    # Pass the ID into your validator script
    validator_silver.run_validation(dag_run_id)
    logger.success(f"Validation Phase completed in {round(time.time() - start, 2)}s")

def main():
    """
    Main entry point for manual (non-Airflow) execution.
    In Airflow, we call the individual functions above for better visibility.
    """
    start_total = time.time()
    logger.info("🚀 STARTING DPA PIEPLINE MANUAL RUN")
    
    try:
        # Step 0: Database Setup (Physical DB + Tables)
        init_db.run_full_setup()
        
        # Step 1-3: Medallion Flow
        run_bronze_layer()
        run_silver_layer()
        run_validation()
        
        duration = round(time.time() - start_total, 2)
        logger.info(f"🏁 PIPELINE COMPLETED SUCCESSFULLY IN {duration}s")
        
    except Exception as e:
        logger.critical(f"❌ PIPELINE CRITICAL FAILURE: {e}")
        raise e # Re-raising ensures Airflow sees the task as 'Failed'

if __name__ == "__main__":
    main()