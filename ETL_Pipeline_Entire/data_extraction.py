import pandas as pd
import mysql.connector
from sqlalchemy import create_engine
from datetime import datetime
import logging
from sqlalchemy import text
import json

# Hardcoded connection details for raw_db (target system)
RAW_DB_HOST = 'localhost'
RAW_DB_USER = 'root'
RAW_DB_PASSWORD = 'root'
RAW_DB_NAME = 'raw_db'

# Function to connect to the raw_db (target MySQL database)
def connect_to_raw_db():
    try:
        engine = create_engine(f"mysql+mysqlconnector://{RAW_DB_USER}:{RAW_DB_PASSWORD}@{RAW_DB_HOST}/{RAW_DB_NAME}")
        return engine
    except Exception as e:
        logging.error(f"Error connecting to raw_db: {e}")
        return None

# Total Refresh Function
def total_refresh(src_engine, raw_db_engine, table_name):
    try:
        with src_engine.begin() as src_conn, raw_db_engine.begin() as raw_db_conn:
            src_df = pd.read_sql(f"SELECT * FROM {table_name}", src_conn)
            if src_df.empty:
                return f"⚠ No data in '{table_name}' for refresh.", None
            
            # Drop and recreate table in raw_db
            raw_db_conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
            schema_query = f"SHOW CREATE TABLE {table_name}"
            schema_result = src_conn.execute(text(schema_query)).fetchone()
            schema_sql = schema_result[1].replace("DEFINER=root@localhost", "")
            raw_db_conn.execute(text(schema_sql))
            
            # Insert full data
            src_df.to_sql(table_name, raw_db_engine, if_exists="append", index=False)
            
            return f"✅ Full Refresh completed for '{table_name}'.", datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    except Exception as e:
        logging.error(f"Total Refresh Error: {e}")
        return f"❌ Error: {e}", None

# Incremental Load Function
def incremental_load(src_engine, raw_db_engine, table_name):
    try:
        with src_engine.begin() as src_conn, raw_db_engine.begin() as raw_db_conn:
            # Ensure raw_db table exists
            result = raw_db_conn.execute(text(f"SHOW TABLES LIKE '{table_name}'"))
            if result.fetchone() is None:
                schema_query = f"SHOW CREATE TABLE {table_name}"
                schema_result = src_conn.execute(text(schema_query)).fetchone()
                schema_sql = schema_result[1].replace("DEFINER=root@localhost", "")
                raw_db_conn.execute(text(schema_sql))

            # Get last recorded ID in raw_db table
            last_id_result = raw_db_conn.execute(text(f"SELECT MAX(id) FROM {table_name}"))
            last_id = last_id_result.fetchone()[0]
            last_id = last_id if last_id is not None else 0  # Default to 0 if no data

            # Fetch only new records
            query = f"SELECT * FROM {table_name} WHERE id > {last_id}"
            src_df = pd.read_sql(query, src_conn)

            last_extracted_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            # Ensure ID column exists
            if 'id' not in src_df.columns:
                return f"❌ Error: 'id' column missing in '{table_name}'.", None, last_extracted_time

            # Insert only new records
            src_df.to_sql(table_name, raw_db_engine, if_exists="append", index=False)

            return f"✅ Incremental data loaded for '{table_name}'.", src_df, last_extracted_time

    except Exception as e:
        logging.error(f"Error loading '{table_name}': {e}")
        return f"❌ Error: {e}", None, datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# Scheduler Logic to run at specific time
def schedule_etl_job(config_file='extraction.json'):
    try:
        # Read configuration file
        with open(config_file, 'r') as config_file:
            config = json.load(config_file)

        # Connect to source database
        src_conn = mysql.connector.connect(
            host=config['source_db']['host'],
            user=config['source_db']['user'],
            password=config['source_db']['password'],
            database=config['source_db']['db']
        )
        src_engine = create_engine(f"mysql+mysqlconnector://{config['source_db']['user']}:{config['source_db']['password']}@{config['source_db']['host']}/{config['source_db']['db']}")

        # Connect to raw_db
        raw_db_engine = connect_to_raw_db()
        if raw_db_engine is None:
            return "Error connecting to raw_db."

        # Loop through selected tables and perform the appropriate extraction
        for table in config['tables']:
            extraction_type = config['extraction_type']
            if extraction_type == "Full Refresh":
                msg, _ = total_refresh(src_engine, raw_db_engine, table)
            else:
                msg, _, _ = incremental_load(src_engine, raw_db_engine, table)

            print(msg)
        return "ETL job completed."
    except Exception as e:
        logging.error(f"Error during scheduled ETL job: {e}")
        return f"Error during scheduled ETL job: {e}"

# Call this scheduler in a specific interval based on the saved configuration
