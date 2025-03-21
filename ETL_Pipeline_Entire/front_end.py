import streamlit as st
import json
import sys
import io
import mysql.connector
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import os
import glob
import time
    
# Import functions and classes from modules
from transformations_code import (
    list_tables_in_raw_db, extract_all_tables, TRANSFORMATIONS,
    transform_all_tables, load_all_tables
)
from mapping import DatasetMapper  # from mapping.py
from data_extraction import total_refresh, incremental_load, connect_to_raw_db

# Initialize session state for tracking progress
if 'current_page' not in st.session_state:
    st.session_state.current_page = "Text to SQL"  # Set a default page
if 'extraction_complete' not in st.session_state:
    st.session_state.extraction_complete = False
if 'mapping_complete' not in st.session_state:
    st.session_state.mapping_complete = False
if 'transformation_complete' not in st.session_state:
    st.session_state.transformation_complete = False
if 'connection_established' not in st.session_state:
    st.session_state.connection_established = False
if 'tables' not in st.session_state:
    st.session_state.tables = []
if 'show_only_query' not in st.session_state:
    st.session_state.show_only_query = False
if 'query_page' not in st.session_state:
    st.session_state.query_page = "Text to SQL"


# Check if files exist to determine completion status
if os.path.exists('extraction.json'):
    st.session_state.extraction_complete = True
if os.path.exists('mapping_status.json'):
    with open('mapping_status.json', 'r') as f:
        mapping_status = json.load(f)
        st.session_state.mapping_complete = True
if os.path.exists('transformation_status.json'):
    st.session_state.transformation_complete = True
    st.session_state.show_only_query = True

# -----------------------------
# Data Extraction Functions
# -----------------------------
def connect_to_source_db(host, user, password, db):
    try:
        conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=db
        )
        return conn
    except mysql.connector.Error as err:
        st.error(f"Error: {err}")
        return None

def get_table_names(src_conn):
    cursor = src_conn.cursor()
    cursor.execute("SHOW TABLES")
    tables = cursor.fetchall()
    return [table[0] for table in tables]

def load_data_to_raw_zone(config):
    try:
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
        
        # Set extraction complete flag
        st.session_state.extraction_complete = True
        return "ETL job completed. Data extracted successfully."
    except Exception as e:
        st.error(f"Error during data loading: {e}")
        return f"Error during data loading: {e}"

# -----------------------------
# Data Mapping Functions
# -----------------------------
def list_tables_in_mapping_db():
    config = {
        "host": "localhost",
        "user": "root",
        "password": "root",
        "database": "silver_db_mapping"
    }
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()
    cursor.execute("SHOW TABLES;")
    tables = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    return tables

def preview_mapping_table(table):
    config = {
        "host": "localhost",
        "user": "root",
        "password": "root",
        "database": "silver_db_mapping"
    }
    conn = mysql.connector.connect(**config)
    try:
        df = pd.read_sql(f"SELECT * FROM {table} LIMIT 5", conn)
    except Exception as e:
        df = pd.DataFrame({"Error": [str(e)]})
    conn.close()
    return df

def run_automated_mapping():
    mapper = DatasetMapper()
    buffer = io.StringIO()
    old_stdout = sys.stdout
    sys.stdout = buffer  # Redirect stdout to capture mapping logs
    mapper.merge_tables()
    sys.stdout = old_stdout
    logs = buffer.getvalue()
    return logs

def move_data_to_silver_db():
    config = {
        "host": "localhost",
        "user": "root",
        "password": "root",
        "database": "raw_db"
    }
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()
    cursor.execute("SHOW TABLES;")
    tables = [row[0] for row in cursor.fetchall()]
    for table in tables:
        try:
            df = pd.read_sql(f"SELECT * FROM {table}", conn)
            silver_config = {
                "host": "localhost",
                "user": "root",
                "password": "root",
                "database": "silver_db_mapping"
            }
            conn_str = f"mysql+mysqlconnector://{silver_config['user']}:{silver_config['password']}@{silver_config['host']}/{silver_config['database']}"
            engine = create_engine(conn_str)
            df.to_sql(table, con=engine, if_exists='replace', index=False)
            print(f"Moved table {table} to silver_db_mapping.")
        except Exception as e:
            print(f"Error moving table {table}: {e}")
    cursor.close()
    conn.close()

# -----------------------------
# Silver DB Functions
# -----------------------------
def connect_to_silver_db():
    try:
        conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="root",
            database="silver_db"
        )
        return conn
    except mysql.connector.Error as err:
        st.error(f"Error connecting to silver_db: {err}")
        return None

def list_tables_in_silver_db():
    conn = connect_to_silver_db()
    if conn:
        cursor = conn.cursor()
        cursor.execute("SHOW TABLES;")
        tables = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        return tables
    return []

def get_table_schema(table_name):
    conn = connect_to_silver_db()
    if conn:
        cursor = conn.cursor()
        cursor.execute(f"DESCRIBE {table_name};")
        schema = cursor.fetchall()
        cursor.close()
        conn.close()
        return schema
    return []

def execute_query(query):
    conn = connect_to_silver_db()
    if conn:
        try:
            df = pd.read_sql(query, conn)
            conn.close()
            return df
        except Exception as e:
            conn.close()
            return pd.DataFrame({"Error": [str(e)]})
    return pd.DataFrame({"Error": ["Could not connect to silver_db"]})

# -----------------------------
# Delete ETL Pipeline Function
# -----------------------------
def delete_etl_pipeline():
    json_files = glob.glob("*.json")
    if not json_files:
        return "No ETL files found to delete."
    
    try:
        for file in json_files:
            os.remove(file)
        
        # Reset session state
        st.session_state.extraction_complete = False
        st.session_state.mapping_complete = False
        st.session_state.transformation_complete = False
        st.session_state.show_only_query = False
        
        return f"Successfully deleted {len(json_files)} ETL pipeline files."
    except Exception as e:
        return f"Error deleting ETL pipeline: {e}"

# -----------------------------
# Page Functions
# -----------------------------
def data_extraction_page():
    st.title('Step 1: Data Extraction')
    
    if st.session_state.extraction_complete:
        st.success("✅ Data extraction completed successfully!")
        st.info("You can now proceed to Data Mapping.")
        return
    
    st.subheader('Source Database Connection')
    host = st.text_input('Source DB Host')
    user = st.text_input('Source DB User')
    password = st.text_input('Source DB Password', type="password")
    db = st.text_input('Source DB Name')

    if st.button('Connect to Source DB'):
        src_conn = connect_to_source_db(host, user, password, db)
        if src_conn:
            st.session_state.connection_established = True
            st.session_state.src_conn = src_conn  # Save the connection in session state
            tables = get_table_names(src_conn)
            st.session_state.tables = tables  # Save the tables in session state
            st.success(f"Connected to {db} successfully!")

    if st.session_state.connection_established:
        table_selection = st.multiselect('Select Tables for Extraction', st.session_state.tables)

        extraction_type = st.selectbox("Select Extraction Type", ["Full Refresh", "Incremental Load"])

        st.subheader("Scheduling Details")
        schedule_type = st.selectbox("Select Type of Scheduling", ["Specific Time", "Interval (Every N Minutes)"])

        if schedule_type == "Specific Time":
            schedule_day = st.number_input("Day of the Month", min_value=1, max_value=31, value=1)
            schedule_month = st.number_input("Month", min_value=1, max_value=12, value=1)
            schedule_year = st.number_input("Year", min_value=datetime.now().year, value=datetime.now().year)
            schedule_hour = st.number_input("Hour", min_value=0, max_value=23, value=datetime.now().hour)
            schedule_minute = st.number_input("Minute", min_value=0, max_value=59, value=datetime.now().minute)
            schedule_second = st.number_input("Second", min_value=0, max_value=59, value=datetime.now().second)
            frequency = "Once"

        elif schedule_type == "Interval (Every N Minutes)":
            frequency_value = st.number_input("Enter Frequency (Minutes)", min_value=1, value=1)
            schedule_day = schedule_month = schedule_year = schedule_hour = schedule_minute = schedule_second = None
            frequency = f"Every {frequency_value} minute(s)"

        config = {
            'source_db': {
                'host': host,
                'user': user,
                'password': password,
                'db': db
            },
            'tables': table_selection,
            'extraction_type': extraction_type,
            'schedule_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'schedule_day': schedule_day,
            'schedule_month': schedule_month,
            'schedule_year': schedule_year,
            'schedule_hour': schedule_hour,
            'schedule_minute': schedule_minute,
            'schedule_second': schedule_second,
            'frequency': frequency
        }

        # Button to Load Data into Raw Zone
        if st.button('Load Data into Raw Zone'):
            if not table_selection:
                st.error("Please select at least one table for extraction.")
            else:
                load_message = load_data_to_raw_zone(config)
                st.success(load_message)
                with open('extraction.json', 'w') as config_file:
                    json.dump(config, config_file, indent=4)
                st.rerun()  # Refresh the page to show completion status

def data_mapping_page():
    st.title("Step 2: Data Mapping")
    
    # Check if extraction is complete
    if not st.session_state.extraction_complete:
        st.warning("⚠️ Please complete Data Extraction before proceeding to Data Mapping.")
        return
    
    # Check if mapping is already complete
    if st.session_state.mapping_complete:
        st.success("✅ Data mapping completed successfully!")
        st.info("You can now proceed to Data Transformations.")
        return
    
    st.header("Automated Data Mapping")

    # Option to choose whether to perform data mapping or not
    perform_mapping = st.radio("Do you want to perform data mapping?", ("Yes", "No"))

    # If the user chooses "Yes", run the mapping process
    if perform_mapping == "Yes":
        if "mapping_logs" not in st.session_state:
            st.session_state["mapping_logs"] = ""

        # Button to run the automated data mapping process.
        if st.button("Run Automated Data Mapping"):
            with st.spinner("Running automated data mapping..."):
                logs = run_automated_mapping()
                st.session_state["mapping_logs"] = logs
                st.success("Automated data mapping completed. See preview below.")

        if st.session_state["mapping_logs"]:
            st.subheader("Mapping Logs:")
            st.code(st.session_state["mapping_logs"], language="python")

        # Button to load (and preview) the mapped data into silver_db_mapping.
        if st.button("Load Mapped Data into Silver DB"):
            tables = list_tables_in_mapping_db()
            if tables:
                st.write("Mapped Tables:")
                for table in tables:
                    with st.expander(f"Table: {table}"):
                        df_preview = preview_mapping_table(table)
                        st.dataframe(df_preview)
                st.success("Mapped data loaded into silver_db_mapping.")
                
                # Save JSON file stating that mapping is true
                with open('mapping_status.json', 'w') as f:
                    json.dump({"mapping": True}, f)
                st.session_state.mapping_complete = True
                st.rerun()  # Refresh the page to show completion status
            else:
                st.error("No tables found in silver_db_mapping.")
    
    # If the user chooses "No", ask for confirmation and move data to silver_db_mapping
    elif perform_mapping == "No":
        st.write("You have chosen to skip data mapping.")
        if st.button("Confirm and Move Data to Silver DB"):
            with st.spinner("Moving data to Silver DB..."):
                move_data_to_silver_db()
                st.success("Data successfully moved to Silver DB.")
            
            # Save JSON file stating that mapping is false
            with open('mapping_status.json', 'w') as f:
                json.dump({"mapping": False}, f)
            st.session_state.mapping_complete = True
            st.rerun()  # Refresh the page to show completion status

def data_transformations_page():
    st.title("Step 3: Data Transformations & Aggregations")
    
    # Check if extraction and mapping are complete
    if not st.session_state.extraction_complete:
        st.warning("⚠️ Please complete Data Extraction before proceeding to Data Transformations.")
        return
    
    if not st.session_state.mapping_complete:
        st.warning("⚠️ Please complete Data Mapping before proceeding to Data Transformations.")
        return

    # Initialize session state dictionaries.
    if "raw_data" not in st.session_state:
        st.session_state["raw_data"] = {}
    if "df_transformed" not in st.session_state:
        st.session_state["df_transformed"] = {}
    if "agg_results" not in st.session_state:
        st.session_state["agg_results"] = {}

    # --- Step 1: Extraction from raw_db (All Tables) ---
    st.header("1. Extraction from MySQL raw_db (All Tables)")
    tables = list_tables_in_raw_db()
    if not tables:
        st.write("No tables found in raw_db. Please load some data into MySQL.")
    else:
        st.write("Found tables:", tables)
    if st.button("Extract All Tables", key="extract_all"):
        raw_data = extract_all_tables()
        st.session_state["raw_data"] = raw_data
        st.write("Raw Data Previews:")
        for table, df in raw_data.items():
            with st.expander(f"Table: {table}"):
                st.dataframe(df.head())

    # --- Step 2: Transformations (Applied to All Tables) ---
    if st.session_state["raw_data"]:
        st.header("2. Transformations (Apply to All Tables)")
        with st.form("transformation_form", clear_on_submit=False):
            selected_transformations = st.multiselect(
                "Select transformations to apply:",
                options=list(TRANSFORMATIONS.keys()),
                default=[],
                key="transform_select"
            )
            transform_submitted = st.form_submit_button("Apply Transformations")
            if transform_submitted:
                with open("selected_transformations.json", "w") as f:
                    json.dump({"selected_transformations": selected_transformations}, f, indent=4)
                st.write("Selected transformations saved to selected_transformations.json")
                transformed_data = transform_all_tables(st.session_state["raw_data"], selected_transformations)
                st.session_state["df_transformed"] = transformed_data
                st.write("Transformed Data Previews:")
                for table, df in transformed_data.items():
                    with st.expander(f"Table: {table}"):
                        st.dataframe(df.head())

    # --- Step 3: Aggregation (Optional) for Selected Tables ---
    if st.session_state["df_transformed"]:
        st.header("3. Aggregation & Summarization (Optional)")
        st.write("For each table you wish to aggregate, enter aggregation parameters.")
        agg_tables = st.multiselect(
            "Select tables for aggregation:",
            options=list(st.session_state["df_transformed"].keys()),
            key="agg_table_selection"
        )
        table_agg_params = {}
        for table in agg_tables:
            df = st.session_state["df_transformed"][table]
            st.subheader(f"Aggregation Parameters for Table: {table}")
            groupby_options = list(df.columns)
            numeric_options = list(df.select_dtypes(include=['int', 'float']).columns)
            groupby_selected = st.multiselect(f"Group-by columns for {table}:", options=groupby_options, key=f"agg_groupby_{table}")
            numeric_selected = st.multiselect(f"Numeric columns to aggregate for {table}:", options=numeric_options, key=f"agg_numeric_{table}")
            
            # Dropdown for aggregation functions selection
            agg_func = st.multiselect(f"Select aggregation functions for {table} (choose multiple)", 
                                     options=["sum", "mean", "min", "max", "count"], 
                                     key=f"agg_func_{table}")
            
            table_agg_params[table] = {
                "groupby_columns": groupby_selected,
                "aggregation_columns": numeric_selected,
                "aggregation_functions": agg_func
            }

        if st.button("Apply Aggregation to Selected Tables", key="apply_agg_selected"):
            agg_results = {}
            for table, params in table_agg_params.items():
                if table in st.session_state["df_transformed"]:
                    df = st.session_state["df_transformed"][table]
                    if params["groupby_columns"] and params["aggregation_columns"] and params["aggregation_functions"]:
                        aggregator_dict = {col: params["aggregation_functions"] for col in params["aggregation_columns"]}
                        try:
                            aggregated_df = df.groupby(params["groupby_columns"]).agg(aggregator_dict)
                            aggregated_df.columns = ["_".join(x) for x in aggregated_df.columns.ravel()]
                            aggregated_df = aggregated_df.reset_index()
                            agg_results[table] = aggregated_df
                            st.write(f"Aggregated Data for table '{table}':")
                            st.dataframe(aggregated_df.head())
                        except Exception as e:
                            st.error(f"Error aggregating table '{table}': {e}")
            st.session_state["agg_results"] = agg_results
            with open("selected_aggregation_parameters.json", "w") as f:
                json.dump(table_agg_params, f, indent=4)
            st.write("Aggregation parameters saved to selected_aggregation_parameters.json")
            for table, params in table_agg_params.items():
                if table in agg_results:
                    filename = f"{table}_agg.json"
                    with open(filename, "w") as f:
                        json.dump({table: params}, f, indent=4)
                    st.write(f"Aggregation parameters for table '{table}' saved to {filename}")

    # --- Step 4: Load All Transformed Data to Silver Layer ---
    if st.session_state["df_transformed"]:
        st.header("4. Load All Transformed Tables to Silver Layer")
        if st.button("Load All Transformed Tables", key="load_transformed_all"):
            load_all_tables(st.session_state["df_transformed"], prefix="transformed")
            st.success("All transformed tables loaded into silver_db.")
            # Mark transformation as complete
            with open("transformation_status.json", "w") as f:
                json.dump({"transformation_complete": True}, f)
            st.session_state.transformation_complete = True
            st.session_state.show_only_query = True
            st.rerun()  # Refresh the page to show only query page

    # --- Step 5: Load All Aggregated Data to Silver Layer ---
    if st.session_state["agg_results"]:
        st.header("5. Load All Aggregated Tables to Silver Layer")
        if st.button("Load All Aggregated Tables", key="load_agg_all"):
            load_all_tables(st.session_state["agg_results"], prefix="agg")
            st.success("All aggregated tables loaded into silver_db.")
            # Mark transformation as complete if not already done
            if not st.session_state.transformation_complete:
                with open("transformation_status.json", "w") as f:
                    json.dump({"transformation_complete": True}, f)
                st.session_state.transformation_complete = True
                st.session_state.show_only_query = True
                st.rerun()  # Refresh the page to show only query page

# -----------------------------
# Data Exploration Pages
# -----------------------------
def text_to_sql_page():
    st.title("Text to SQL")
    
    # Get all tables from silver_db
    silver_tables = list_tables_in_silver_db()
    
    if not silver_tables:
        st.warning("No tables found in silver_db. Please ensure data was loaded properly.")
        return
    
    # Display table information
    table_info = {}
    with st.expander("Table Information", expanded=False):
        for table in silver_tables:
            st.subheader(f"Table: {table}")
            # Get schema
            schema = get_table_schema(table)
            schema_df = pd.DataFrame(schema, columns=["Field", "Type", "Null", "Key", "Default", "Extra"])
            st.write("Schema:")
            st.dataframe(schema_df)
            
            # Get sample data
            sample_query = f"SELECT * FROM {table} LIMIT 5"
            sample_df = execute_query(sample_query)
            st.write("Sample Data:")
            st.dataframe(sample_df)
            
            # Store table info for AI assistant
            table_info[table] = {
                "schema": schema_df.to_dict(orient="records"),
                "sample": sample_df.head().to_dict(orient="records")
            }
    
    # Input area for AI queries
    st.header("Query Your Data with Natural Language")
    
    # Create a text area for the query
    user_query = st.text_area(
        "Ask a question about your data:", 
        height=100,
        placeholder="Example: Show me the top 5 products by revenue, Which customers have spent the most in the last quarter, etc."
    )
    
    # Initialize session state for storing generated SQL and results
    if 'generated_sql' not in st.session_state:
        st.session_state.generated_sql = ""
    if 'sql_results' not in st.session_state:
        st.session_state.sql_results = None
    if 'golden_db_saved' not in st.session_state:
        st.session_state.golden_db_saved = False
    if 'show_success' not in st.session_state:
        st.session_state.show_success = False
    
    # Generate SQL button
    if st.button("Generate SQL Query"):
        if user_query:
            with st.spinner("Generating SQL query..."):
                # Import the process_query function from text_to_sql.py
                from text_to_sql import process_query
                
                # Generate SQL using the model
                st.session_state.generated_sql = process_query(user_query)
                st.session_state.sql_results = None  # Reset results when generating new SQL
                st.session_state.show_success = True
    
    # Show success message for generation
    if st.session_state.show_success and st.session_state.generated_sql:
        st.success("✅ SQL query successfully generated!")
        st.session_state.show_success = False  # Reset so it only shows once
    
    # Display and allow editing of the generated SQL
    if st.session_state.generated_sql:
        st.subheader("Generated SQL Query")
        
        # Allow editing the generated SQL
        edited_sql = st.text_area(
            "Edit SQL if needed:", 
            value=st.session_state.generated_sql,
            height=100
        )
        
        # Run SQL button (separate from generation)
        if st.button("Run SQL Query"):
            with st.spinner("Executing query..."):
                try:
                    result_df = execute_query(edited_sql)
                    st.session_state.sql_results = result_df
                    st.session_state.current_sql = edited_sql
                except Exception as e:
                    st.error(f"Error executing query: {e}")
    
    # Display results if available
    if st.session_state.sql_results is not None:
        st.subheader("Query Results")
        st.dataframe(st.session_state.sql_results)
        
        # Option to save to golden_db (only if we have results)
        if not st.session_state.sql_results.empty:
            st.subheader("Save to Golden Database")
            
            # Use columns for layout
            col1, col2 = st.columns([1, 2])
            
            with col1:
                save_to_golden = st.checkbox("Save to golden_db")
            
            with col2:
                if save_to_golden:
                    # Get existing golden_db tables for reference
                    try:
                        conn = mysql.connector.connect(
                            host="localhost",
                            user="root",
                            password="root",
                            database="golden_db"
                        )
                        cursor = conn.cursor()
                        cursor.execute("SHOW TABLES")
                        existing_tables = [table[0] for table in cursor.fetchall()]
                        cursor.close()
                        conn.close()
                    except:
                        existing_tables = []
                    
                    # Show existing tables if there are any
                    if existing_tables:
                        st.write("Existing tables in golden_db:")
                        st.write(", ".join(existing_tables))
                    
                    # More descriptive table name with default value
                    default_name = f"golden_{datetime.now().strftime('%Y%m%d_%H%M')}"
                    table_name = st.text_input(
                        "Enter a meaningful name for this table:", 
                        value=default_name,
                        help="Choose a descriptive name for your results table"
                    )
            
            # Save button with form to prevent page refresh
            if save_to_golden:
                # Use a form to prevent page refresh
                with st.form(key="save_to_golden_form"):
                    st.write("Click to confirm saving data to golden_db")
                    submit_button = st.form_submit_button(label="Save to Golden DB")
                    
                    if submit_button:
                        try:
                            # Connect to MySQL without database to create if needed
                            conn = mysql.connector.connect(
                                host="localhost",
                                user="root",
                                password="root"
                            )
                            cursor = conn.cursor()
                            cursor.execute("CREATE DATABASE IF NOT EXISTS golden_db")
                            cursor.close()
                            conn.close()
                            
                            # Now connect to the golden_db and save the data
                            engine = create_engine(f"mysql+mysqlconnector://root:root@localhost/golden_db")
                            st.session_state.sql_results.to_sql(table_name, con=engine, if_exists='replace', index=False)
                            
                            st.session_state.golden_db_saved = True
                            # Store the table name that was used
                            st.session_state.saved_table_name = table_name
                            return True
                        except Exception as e:
                            st.error(f"Error saving to golden_db: {e}")
                            return False
                
                # Show success message outside the form if data was saved
                if st.session_state.golden_db_saved:
                    st.success(f"✅ Data successfully saved to golden_db.{st.session_state.saved_table_name}")
                    st.info("You can now visualize this data in the Data Visualization tab.")
                    # Reset the flag after showing the message
                    st.session_state.golden_db_saved = False
    
    # Provide some example queries for users
    with st.expander("Example Queries"):
        st.markdown("""
        Here are some example queries you can try:
        
        - Show me the total sales by product category
        - Which customers made the most purchases last month?
        - What's the average transaction value by day of week?
        - Show me trends in customer acquisition over time
        - Identify products frequently purchased together
        """)
        
def data_visualization_page():
    st.title("Data Visualization")
    
    # Get all tables from golden_db
    try:
        # Connect to golden_db instead of silver_db
        conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="root",
            database="golden_db"
        )
        cursor = conn.cursor()
        cursor.execute("SHOW TABLES")
        golden_tables = [table[0] for table in cursor.fetchall()]
        cursor.close()
        conn.close()
    except Exception as e:
        st.error(f"Error connecting to golden_db: {e}")
        golden_tables = []
    
    if not golden_tables:
        st.warning("No tables found in golden_db. Please ensure data was loaded properly from your queries.")
        return
    
    # Table selection
    selected_table = st.selectbox("Select a table to visualize", golden_tables)
    
    # Get the data
    if selected_table:
        try:
            # Execute query on golden_db
            conn = mysql.connector.connect(
                host="localhost",
                user="root",
                password="root",
                database="golden_db"
            )
            df = pd.read_sql(f"SELECT * FROM {selected_table}", conn)
            conn.close()
            
            st.write(f"Data preview for {selected_table}:")
            st.dataframe(df.head())
            
            # Only show visualization options if we have data
            if not df.empty:
                st.header("Create Visualization")
                
                # Select chart type
                chart_type = st.selectbox(
                    "Select chart type",
                    ["Bar Chart", "Line Chart", "Scatter Plot", "Histogram", "Pie Chart"]
                )
                
                # Get column names for x and y axis selection
                numeric_columns = df.select_dtypes(include=['int', 'float']).columns.tolist()
                all_columns = df.columns.tolist()
                
                if chart_type in ["Bar Chart", "Line Chart", "Scatter Plot"]:
                    x_axis = st.selectbox("Select X-axis", all_columns)
                    y_axis = st.selectbox("Select Y-axis", numeric_columns)
                    
                    if st.button("Generate Chart"):
                        st.subheader(f"{chart_type}: {y_axis} by {x_axis}")
                        
                        if chart_type == "Bar Chart":
                            st.bar_chart(df.set_index(x_axis)[y_axis])
                        elif chart_type == "Line Chart":
                            st.line_chart(df.set_index(x_axis)[y_axis])
                        elif chart_type == "Scatter Plot":
                            st.scatter_chart(df.set_index(x_axis)[y_axis])
                
                elif chart_type == "Histogram":
                    column = st.selectbox("Select column for histogram", numeric_columns)
                    bins = st.slider("Number of bins", min_value=5, max_value=100, value=20)
                    
                    if st.button("Generate Histogram"):
                        st.subheader(f"Histogram of {column}")
                        hist_values = df[column].dropna()
                        if not hist_values.empty:
                            st.bar_chart(hist_values.value_counts(bins=bins).sort_index())
                        else:
                            st.error("No valid data for histogram.")
                
                elif chart_type == "Pie Chart":
                    st.info("For pie charts, the data will be aggregated by the selected category column")
                    category_column = st.selectbox("Select category column", all_columns)
                    value_column = st.selectbox("Select value column", numeric_columns)
                    
                    if st.button("Generate Pie Chart"):
                        st.subheader(f"Pie Chart: {value_column} by {category_column}")
                        # This is a placeholder as Streamlit doesn't have native pie chart
                        pie_data = df.groupby(category_column)[value_column].sum()
                        st.write("Pie Chart Data:")
                        st.dataframe(pie_data)
                        st.info("Note: Streamlit doesn't have a native pie chart. In a production app, you would use Plotly or other libraries.")
        
        except Exception as e:
            st.error(f"Error loading data: {e}")

# -----------------------------
# Delete ETL Pipeline Function
# -----------------------------
def delete_etl_pipeline():
    # First delete JSON files
    json_files = glob.glob("*.json")
    files_deleted = 0
    
    try:
        for file in json_files:
            os.remove(file)
            files_deleted += 1
        
        # Now drop and recreate databases
        try:
            # Connect to MySQL without specifying a database
            conn = mysql.connector.connect(
                host="localhost",
                user="root",
                password="root"
            )
            cursor = conn.cursor()
            
            # Drop databases if they exist
            cursor.execute("DROP DATABASE IF EXISTS raw_db")
            cursor.execute("DROP DATABASE IF EXISTS silver_db_mapping")
            cursor.execute("DROP DATABASE IF EXISTS silver_db")
            cursor.execute("DROP DATABASE IF EXISTS golden_db")
            
            # Recreate empty databases
            cursor.execute("CREATE DATABASE raw_db")
            cursor.execute("CREATE DATABASE silver_db_mapping")
            cursor.execute("CREATE DATABASE silver_db")
            cursor.execute("CREATE DATABASE golden_db")
            
            cursor.close()
            conn.close()
            
            # Reset session state
            st.session_state.extraction_complete = False
            st.session_state.mapping_complete = False
            st.session_state.transformation_complete = False
            st.session_state.show_only_query = False
            
            return f"Successfully deleted {files_deleted} ETL pipeline files and reset all databases."
        
        except mysql.connector.Error as err:
            return f"Files deleted but database reset failed: {err}"
            
    except Exception as e:
        return f"Error deleting ETL pipeline: {e}"

def logging_page():
    st.title("ETL Pipeline Logs")
    
    # Check if any log files exist
    log_files = []
    if os.path.exists('extraction.json'):
        log_files.append('extraction.json')
    if os.path.exists('mapping_status.json'):
        log_files.append('mapping_status.json')
    if os.path.exists('transformation_status.json'):
        log_files.append('transformation_status.json')
    if os.path.exists('selected_transformations.json'):
        log_files.append('selected_transformations.json')
    if os.path.exists('selected_aggregation_parameters.json'):
        log_files.append('selected_aggregation_parameters.json')
    
    # Add any table-specific aggregation logs
    table_agg_logs = glob.glob("*_agg.json")
    log_files.extend(table_agg_logs)
    
    if not log_files:
        st.warning("No log files found. The ETL pipeline hasn't been run yet.")
        return
    # Main app navigation
# Main app navigation
if st.session_state.transformation_complete:
    # Add top navigation menu instead of sidebar
    st.title("Data Analytics Platform")
    
    # Create a horizontal navigation bar using columns
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        if st.button("Text to SQL", use_container_width=True):
            st.session_state.current_page = "Text to SQL"
    
    with col2:
        if st.button("Data Visualization", use_container_width=True):
            st.session_state.current_page = "Data Visualization"
    
    with col3:
        if st.button("Logs", use_container_width=True):
            st.session_state.current_page = "Logs"
    
    with col4:
        if st.button("Reset ETL", use_container_width=True):
            st.session_state.current_page = "Reset"
    
    st.divider()  # Add a divider below the navigation
    
    # Display the selected page
    if st.session_state.current_page == "Text to SQL":
        text_to_sql_page()
    elif st.session_state.current_page == "Data Visualization":
        data_visualization_page()
    elif st.session_state.current_page == "Logs":
        logging_page()
    elif st.session_state.current_page == "Reset":
        st.header("Reset ETL Pipeline")
        st.warning("⚠️ Warning: This will delete all ETL pipeline files and reset all databases. This action cannot be undone.")
        
        # Add confirmation checkbox
        confirm_delete = st.checkbox("I understand that this will reset the entire ETL pipeline and all databases")
        
        # Single button that's disabled until checkbox is checked
        if st.button("Delete ETL Pipeline Files & Reset Databases", disabled=not confirm_delete):
            result = delete_etl_pipeline()
            st.success(result)
            st.info("Redirecting to ETL Pipeline setup...")
            # Add a slight delay for user to see the message
            import time
            time.sleep(2)
            st.rerun()
else:
    # Create tabs only for the ETL pipeline stages
    tab1, tab2, tab3 = st.tabs([
        "Data Extraction", 
        "Data Mapping", 
        "Data Transformations"
    ])
    
    with tab1:
        data_extraction_page()
    
    with tab2:
        data_mapping_page()
    
    with tab3:
        data_transformations_page()