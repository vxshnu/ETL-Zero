import streamlit as st
import json
import sys
import io
import mysql.connector
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import os

# Import functions and classes from modules
from transformations_code import (
    list_tables_in_raw_db, extract_all_tables, TRANSFORMATIONS,
    transform_all_tables, load_all_tables
)
from mapping import DatasetMapper  # from mapping.py
from data_extraction import total_refresh, incremental_load, connect_to_raw_db

# Initialize session state for tracking progress
if 'extraction_complete' not in st.session_state:
    st.session_state.extraction_complete = False
if 'mapping_complete' not in st.session_state:
    st.session_state.mapping_complete = False
if 'connection_established' not in st.session_state:
    st.session_state.connection_established = False
if 'tables' not in st.session_state:
    st.session_state.tables = []

# Check if files exist to determine completion status
if os.path.exists('extraction.json'):
    st.session_state.extraction_complete = True
if os.path.exists('mapping_status.json'):
    with open('mapping_status.json', 'r') as f:
        mapping_status = json.load(f)
        st.session_state.mapping_complete = True

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

    # --- Step 5: Load All Aggregated Data to Silver Layer ---
    if st.session_state["agg_results"]:
        st.header("5. Load All Aggregated Tables to Silver Layer")
        if st.button("Load All Aggregated Tables", key="load_agg_all"):
            load_all_tables(st.session_state["agg_results"], prefix="agg")
            st.success("All aggregated tables loaded into silver_db.")

# -----------------------------
# Progress Indicator
# -----------------------------
def display_progress():
    steps = [
        {"name": "Data Extraction", "complete": st.session_state.extraction_complete},
        {"name": "Data Mapping", "complete": st.session_state.mapping_complete},
        {"name": "Data Transformations", "complete": False}  # Always false as it's the final step
    ]
    
    col1, col2, col3 = st.sidebar.columns(3)
    
    for i, step in enumerate(steps):
        col = [col1, col2, col3][i]
        if step["complete"]:
            col.markdown(f"#### ✅ {step['name']}")
        else:
            col.markdown(f"#### ⏳ {step['name']}")

# -----------------------------
# Main App
# -----------------------------
def main():
    st.sidebar.title("ETL Pipeline")
    st.sidebar.subheader("Progress")
    display_progress()
    
    st.sidebar.subheader("Navigation")
    
    # Determine which pages are accessible
    pages = {
        "1. Data Extraction": data_extraction_page,
        "2. Data Mapping": data_mapping_page if st.session_state.extraction_complete else None,
        "3. Data Transformations": data_transformations_page if st.session_state.mapping_complete else None
    }
    
    # Filter out None values for disabled pages
    available_pages = {k: v for k, v in pages.items() if v is not None}
    
    # Create the radio buttons for navigation
    page = st.sidebar.radio("Select Step", list(available_pages.keys()))
    
    # Run the selected page function
    available_pages[page]()
    
    # Add explanation for disabled pages
    if "2. Data Mapping" not in available_pages:
        st.sidebar.info("Complete Data Extraction to unlock Data Mapping")
    if "3. Data Transformations" not in available_pages:
        st.sidebar.info("Complete Data Mapping to unlock Data Transformations")

if __name__ == "__main__":
    main()