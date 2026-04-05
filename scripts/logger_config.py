# scripts/logger_config.py
import os
from loguru import logger
import sys

# Standard directory for Airflow logs or a custom project volume
LOG_DIR = "/opt/airflow/logs/custom_pipeline"
os.makedirs(LOG_DIR, exist_ok=True)

def setup_app_logger():
    # Remove the default logger to avoid double-printing
    logger.remove()
    
    # 1. Console Sink (Standard Output)
    logger.add(sys.stdout, format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{message}</cyan>", level="INFO")

    # 2. File Sink (Persistence)
    # Rotation: Creates a new file when it hits 10MB
    # Retention: Keeps logs for 10 days only to save space
    logger.add(
        f"{LOG_DIR}/etl_execution_{{time:YYYY-MM-DD}}.log",
        rotation="10 MB",
        retention="10 days",
        level="DEBUG",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}"
    )
    return logger

# Initialize the global logger instance
pipeline_logger = setup_app_logger()