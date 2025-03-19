import pandas as pd
import mysql.connector
from sqlalchemy import create_engine
from datetime import datetime
import logging
from sqlalchemy import text
import json

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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
        with src_engine.connect() as src_conn, raw_db_engine.connect() as raw_db_conn:
            # First, get the table creation SQL
            schema_query = f"SHOW CREATE TABLE {table_name}"
            schema_result = src_conn.execute(text(schema_query)).fetchone()
            if not schema_result:
                return f"❌ Error: Table '{table_name}' not found in source database.", None
                
            schema_sql = schema_result[1].replace("DEFINER=root@localhost", "")
            
            # Drop the table if it exists in raw_db
            raw_db_conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
            
            # Create the table in raw_db with the same schema
            raw_db_conn.execute(text(schema_sql))
            
            # Now fetch all data from source
            src_df = pd.read_sql(f"SELECT * FROM {table_name}", src_conn)
            if src_df.empty:
                return f"⚠ No data in '{table_name}' for refresh, but table structure created.", datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # Insert all data into raw_db
            src_df.to_sql(table_name, raw_db_engine, if_exists="replace", index=False, method="multi", chunksize=1000)
            
            return f"✅ Full Refresh completed for '{table_name}'. {len(src_df)} rows transferred.", datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    except Exception as e:
        logging.error(f"Total Refresh Error for '{table_name}': {e}")
        return f"❌ Error: {e}", None

# Incremental Load Function
def incremental_load(src_engine, raw_db_engine, table_name):
    try:
        with src_engine.connect() as src_conn, raw_db_engine.connect() as raw_db_conn:
            # Check if table exists in raw_db
            check_table = raw_db_conn.execute(text(f"SHOW TABLES LIKE '{table_name}'"))
            table_exists = check_table.fetchone() is not None
            
            # If table doesn't exist, create it with the same schema as source
            if not table_exists:
                schema_query = f"SHOW CREATE TABLE {table_name}"
                schema_result = src_conn.execute(text(schema_query)).fetchone()
                if not schema_result:
                    return f"❌ Error: Table '{table_name}' not found in source database.", None, datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                schema_sql = schema_result[1].replace("DEFINER=root@localhost", "")
                raw_db_conn.execute(text(schema_sql))
                logging.info(f"Created table '{table_name}' in raw_db")
                
                # For new tables, perform a full load
                src_df = pd.read_sql(f"SELECT * FROM {table_name}", src_conn)
                if not src_df.empty:
                    src_df.to_sql(table_name, raw_db_engine, if_exists="append", index=False, method="multi", chunksize=1000)
                    return f"✅ New table '{table_name}' created and loaded with {len(src_df)} rows.", src_df, datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                else:
                    return f"⚠ Table '{table_name}' created but no data to load.", None, datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # If table exists, get last recorded ID
            last_id_result = raw_db_conn.execute(text(f"SELECT MAX(id) FROM {table_name}"))
            last_id = last_id_result.fetchone()[0]
            last_id = last_id if last_id is not None else 0  # Default to 0 if no data
            
            # Fetch only new records
            query = f"SELECT * FROM {table_name} WHERE id > {last_id}"
            src_df = pd.read_sql(query, src_conn)
            
            if src_df.empty:
                return f"ℹ️ No new data to load for '{table_name}'.", None, datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # Insert only new records
            src_df.to_sql(table_name, raw_db_engine, if_exists="append", index=False, method="multi", chunksize=1000)
            
            return f"✅ Incremental data loaded for '{table_name}'. {len(src_df)} new rows transferred.", src_df, datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    except Exception as e:
        logging.error(f"Incremental load error for '{table_name}': {e}")
        return f"❌ Error: {e}", None, datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# Scheduler Logic to run at specific time
def schedule_etl_job(config_file='extraction.json'):
    try:
        # Read configuration file
        with open(config_file, 'r') as file:
            config = json.load(file)
        
        logging.info(f"Starting ETL job with config: {config_file}")
        
        # Connect to source database
        src_engine = create_engine(
            f"mysql+mysqlconnector://{config['source_db']['user']}:{config['source_db']['password']}@{config['source_db']['host']}/{config['source_db']['db']}"
        )
        
        # Connect to raw_db
        raw_db_engine = connect_to_raw_db()
        if raw_db_engine is None:
            return "Error connecting to raw_db."
        
        results = []
        
        # Loop through selected tables and perform the appropriate extraction
        for table in config['tables']:
            extraction_type = config['extraction_type']
            if extraction_type == "Full Refresh":
                msg, timestamp = total_refresh(src_engine, raw_db_engine, table)
            else:
                msg, df, timestamp = incremental_load(src_engine, raw_db_engine, table)
            
            logging.info(msg)
            results.append({"table": table, "message": msg, "timestamp": timestamp})
        
        return {"status": "ETL job completed.", "results": results}
    except Exception as e:
        logging.error(f"Error during scheduled ETL job: {e}")
        return {"status": f"Error during scheduled ETL job: {e}", "results": []}

# Execute the ETL job
if __name__ == "__main__":
    result = schedule_etl_job()
    print(result)