# scheduler.py
import json
import time
import schedule
import os
import logging
import sys
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine

# Import functions from your modules
from data_extraction import schedule_etl_job, connect_to_raw_db
from mapping import DatasetMapper
from transformations_code import (
    extract_all_tables, transform_all_tables, load_all_tables, list_tables_in_raw_db
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("etl_scheduler.log"),
        logging.StreamHandler()
    ]
)

def run_extraction():
    """
    Run the extraction process based on extraction.json config
    """
    logging.info("Starting scheduled extraction process")
    try:
        # Use absolute path for more reliable file detection
        current_dir = os.path.dirname(os.path.abspath(__file__))
        extraction_path = os.path.join(current_dir, 'extraction.json')
        
        logging.info(f"Looking for extraction config at: {extraction_path}")
        if os.path.exists(extraction_path):
            result = schedule_etl_job(extraction_path)
            logging.info(f"Extraction completed: {result}")
            return True
        else:
            logging.error("extraction.json not found, cannot run extraction process")
            return False
    except Exception as e:
        logging.error(f"Error during extraction: {e}")
        return False

def run_mapping():
    """
    Run the mapping process based on mapping_status.json config
    """
    logging.info("Starting scheduled mapping process")
    try:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        mapping_path = os.path.join(current_dir, 'mapping_status.json')
        
        logging.info(f"Looking for mapping config at: {mapping_path}")
        if os.path.exists(mapping_path):
            with open(mapping_path, 'r') as f:
                mapping_config = json.load(f)
            
            if mapping_config.get("mapping", True):
                # Run automated mapping
                mapper = DatasetMapper()
                mapper.merge_tables()
                logging.info("Automated mapping completed successfully")
            else:
                # Move data directly to silver_db_mapping
                logging.info("Skipping mapping as per configuration, moving data directly")
                # Connect to raw_db
                config = {
                    "host": "localhost",
                    "user": "root",
                    "password": "root",
                    "database": "raw_db"
                }
                conn = create_engine(f"mysql+mysqlconnector://{config['user']}:{config['password']}@{config['host']}/{config['database']}")
                
                # Connect to silver_db_mapping
                silver_config = {
                    "host": "localhost",
                    "user": "root",
                    "password": "root",
                    "database": "silver_db_mapping"
                }
                silver_engine = create_engine(f"mysql+mysqlconnector://{silver_config['user']}:{silver_config['password']}@{silver_config['host']}/{silver_config['database']}")
                
                # Get tables from raw_db
                tables = list_tables_in_raw_db()
                
                # Move tables to silver_db_mapping
                for table in tables:
                    try:
                        # Read data from raw_db
                        df = pd.read_sql(f"SELECT * FROM {table}", conn)
                        # Write to silver_db_mapping
                        df.to_sql(table, silver_engine, if_exists='replace', index=False)
                        logging.info(f"Moved table {table} to silver_db_mapping")
                    except Exception as e:
                        logging.error(f"Error moving table {table}: {e}")
            
            return True
        else:
            logging.error("mapping_status.json not found, cannot run mapping process")
            return False
    except Exception as e:
        logging.error(f"Error during mapping: {e}")
        return False

def run_transformation():
    """
    Run the transformation process based on selected_transformations.json config
    """
    logging.info("Starting scheduled transformation process")
    try:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        transform_path = os.path.join(current_dir, 'selected_transformations.json')
        
        logging.info(f"Looking for transformation config at: {transform_path}")
        if os.path.exists(transform_path):
            with open(transform_path, 'r') as f:
                transform_config = json.load(f)
            
            selected_transforms = transform_config.get("selected_transformations", [])
            
            # Extract data
            raw_data = extract_all_tables()
            if not raw_data:
                logging.error("No data found to transform")
                return False
            
            # Apply transformations
            transformed_data = transform_all_tables(raw_data, selected_transforms)
            
            # Load to silver_db
            load_all_tables(transformed_data, prefix="transformed")
            logging.info("Transformed data loaded to silver_db")
            
            # Check for aggregations
            agg_path = os.path.join(current_dir, 'selected_aggregation_parameters.json')
            if os.path.exists(agg_path):
                with open(agg_path, 'r') as f:
                    agg_params = json.load(f)
                
                # Process each table's aggregation
                agg_results = {}
                for table, params in agg_params.items():
                    if table in transformed_data:
                        df = transformed_data[table]
                        groupby_cols = params.get("groupby_columns", [])
                        agg_cols = params.get("aggregation_columns", [])
                        agg_funcs = params.get("aggregation_functions", ["sum"])
                        
                        if groupby_cols and agg_cols and agg_funcs:
                            try:
                                aggregator_dict = {col: agg_funcs for col in agg_cols}
                                aggregated_df = df.groupby(groupby_cols).agg(aggregator_dict)
                                aggregated_df.columns = ["_".join(x) for x in aggregated_df.columns.ravel()]
                                aggregated_df = aggregated_df.reset_index()
                                agg_results[table] = aggregated_df
                                logging.info(f"Aggregated data for table '{table}'")
                            except Exception as e:
                                logging.error(f"Error aggregating table '{table}': {e}")
                
                # Load aggregated data
                if agg_results:
                    load_all_tables(agg_results, prefix="agg")
                    logging.info("Aggregated data loaded to silver_db")
            
            # Mark transformation as complete
            with open(os.path.join(current_dir, "transformation_status.json"), "w") as f:
                json.dump({"transformation_complete": True}, f)
            
            return True
        else:
            logging.error("selected_transformations.json not found, cannot run transformation process")
            return False
    except Exception as e:
        logging.error(f"Error during transformation: {e}")
        return False

def run_etl_pipeline():
    """
    Run the complete ETL pipeline in sequence
    """
    logging.info("Starting complete ETL pipeline")
    
    # Run extraction
    extraction_success = run_extraction()
    if not extraction_success:
        logging.error("Extraction failed, stopping pipeline")
        return
    
    # Run mapping
    mapping_success = run_mapping()
    if not mapping_success:
        logging.error("Mapping failed, stopping pipeline")
        return
    
    # Run transformation
    transformation_success = run_transformation()
    if not transformation_success:
        logging.error("Transformation failed")
        return
    
    logging.info("Complete ETL pipeline executed successfully")

def setup_schedule():
    """
    Set up the schedule based on extraction.json config
    """
    current_dir = os.path.dirname(os.path.abspath(__file__))
    extraction_path = os.path.join(current_dir, 'extraction.json')
    
    logging.info(f"Looking for extraction config for scheduling at: {extraction_path}")
    if not os.path.exists(extraction_path):
        logging.error("extraction.json not found, cannot set up schedule")
        return
    
    with open(extraction_path, 'r') as f:
        config = json.load(f)
    
    frequency = config.get('frequency', None)
    
    if frequency == "Once":
        # Schedule a one-time run
        schedule_year = config.get('schedule_year', datetime.now().year)
        schedule_month = config.get('schedule_month', datetime.now().month)
        schedule_day = config.get('schedule_day', datetime.now().day)
        schedule_hour = config.get('schedule_hour', datetime.now().hour)
        schedule_minute = config.get('schedule_minute', datetime.now().minute)
        
        # Calculate time difference to schedule the job
        schedule_time = datetime(schedule_year, schedule_month, schedule_day, 
                                schedule_hour, schedule_minute)
        now = datetime.now()
        
        if schedule_time > now:
            time_diff = (schedule_time - now).total_seconds()
            logging.info(f"Scheduling one-time ETL job at {schedule_time}")
            
            # Schedule the job after the delay
            time.sleep(time_diff)
            run_etl_pipeline()
        else:
            logging.warning("Scheduled time is in the past, running immediately")
            run_etl_pipeline()
    
    elif frequency.startswith("Every"):
        # Parse frequency for recurring schedule
        try:
            minutes = int(frequency.split()[1])
            logging.info(f"Setting up recurring ETL job every {minutes} minutes")
            
            # Schedule recurring job
            schedule.every(minutes).minutes.do(run_etl_pipeline)
            
            # Keep the scheduler running
            while True:
                schedule.run_pending()
                time.sleep(1)
        except (ValueError, IndexError):
            logging.error(f"Invalid frequency format: {frequency}")
            return
    else:
        logging.error(f"Unsupported frequency: {frequency}")

def check_config_files():
    """
    Debug function to check the existence of config files
    """
    current_dir = os.path.dirname(os.path.abspath(__file__))
    files_to_check = [
        'extraction.json', 
        'mapping_status.json', 
        'selected_transformations.json', 
        'transformation_status.json'
    ]
    
    logging.info(f"Current working directory: {os.getcwd()}")
    logging.info(f"Script directory: {current_dir}")
    
    found_files = []
    for file_name in files_to_check:
        file_path = os.path.join(current_dir, file_name)
        exists = os.path.exists(file_path)
        logging.info(f"Looking for {file_path}: {'FOUND' if exists else 'NOT FOUND'}")
        if exists:
            found_files.append(file_name)
    
    return len(found_files) > 0

if __name__ == "__main__":
    logging.info("ETL Scheduler started")
    
    # Debug file existence
    files_exist = check_config_files()
    
    if files_exist:
        # Check if it's a scheduled run or an immediate run
        if len(sys.argv) > 1 and sys.argv[1] == "--now":
            logging.info("Running ETL pipeline immediately")
            run_etl_pipeline()
        else:
            # Set up the schedule
            setup_schedule()
    else:
        logging.error("No configuration files found. Please run the frontend application first to create them.")