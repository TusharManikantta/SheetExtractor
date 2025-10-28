import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import numpy as np
from sqlalchemy import create_engine, text

# --- Configuration for Database and Streamlit ---

# ⚠️ IMPORTANT: Set up your database credentials securely in a .streamlit/secrets.toml file:
#
# # .streamlit/secrets.toml
# [connections.sqlite]
# url = "sqlite:///analytics.db"
#
# This file will be created in your app directory. Streamlit handles the connection.
SQLITE_DB_NAME = 'analytics.db'
EVENT_TABLE_NAME = 'events_data'
METRICS_TABLE_NAME = 'infra_metrics'

# Page configuration
st.set_page_config(page_title="SQLite Analytics Dashboard", layout="wide", page_icon="📊")

# Custom CSS and Header
st.markdown("""
<style>
.main-header { font-size: 2.5rem; font-weight: bold; color: #1f77b4; text-align: center; margin-bottom: 2rem; }
.section-header { font-size: 1.5rem; font-weight: bold; color: #2c3e50; margin-top: 2rem; margin-bottom: 1rem; }
</style>
""", unsafe_allow_html=True)
st.markdown('<p class="main-header">📊 SQLite Analytics Dashboard</p>', unsafe_allow_html=True)

# Initialize session state (updated to track DB connection and table)
if 'conn' not in st.session_state:
    st.session_state.conn = None
if 'df' not in st.session_state:
    st.session_state.df = None
if 'data_loaded_db' not in st.session_state:
    st.session_state.data_loaded_db = False

# Aggregation functions mapping
AGG_FUNCTIONS = {
    'Sum': 'sum',
    'Average': 'mean',
    'Max': 'max',
    'Min': 'min',
    'Count': 'count',
    'Product': 'prod',
    'Standard Deviation': 'std',
    'Variance': 'var'
}

# --- Database Functions ---

@st.cache_resource
def get_db_connection():
    """Establish and cache the SQLite database connection."""
    try:
        # Connects using the URL specified in secrets.toml: sqlite:///analytics.db
        conn = st.connection("sqlite", type="sql")
        st.session_state.conn = conn
        st.success(f"✅ SQLite connection established to **{SQLITE_DB_NAME}**.")
        return conn
    except Exception as e:
        st.error(f"❌ Failed to connect to SQLite. Please check .streamlit/secrets.toml. Error: {e}")
        return None

def ingest_excel_to_db(uploaded_file, conn, table_name, if_exists_policy='replace'):
    """Load Excel to Pandas, transform, and write to SQLite."""
    try:
        st.info(f"Reading file and preparing for ingestion into **{table_name}**...")
        # Use openpyxl engine for reading .xlsx files
        df = pd.read_excel(uploaded_file, engine='openpyxl')
        
        # Standardize column names for database (Important for case-insensitivity/consistency)
        df.columns = [col.upper().replace('-', '_').strip() for col in df.columns]

        # Basic type conversion for known columns
        if 'TXN_DATE' in df.columns:
            # Convert date column to datetime, then to date objects for easier SQLite indexing
            df['TXN_DATE'] = pd.to_datetime(df['TXN_DATE'], errors='coerce').dt.date
        if 'HOUR' in df.columns:
            df['HOUR'] = pd.to_numeric(df['HOUR'], errors='coerce')
        if 'EVENTS' in df.columns:
            df['EVENTS'] = pd.to_numeric(df['EVENTS'], errors='coerce')
            
        # Use SQLAlchemy engine for efficient bulk insert
        engine = create_engine(conn.url)
        df.to_sql(table_name, engine, if_exists=if_exists_policy, index=False)
        st.success(f"🎉 Successfully ingested {len(df)} rows into the SQLite table: **{table_name}**")
        return True
    except Exception as e:
        st.error(f"Error during data ingestion: {e}")
        return False

def load_data_from_db(conn, table_name):
    """Fetch all data from the database table."""
    try:
        st.info(f"Fetching data from database table: **{table_name}**...")
        # Use st.connection.query for Streamlit's caching mechanism
        # SQLite queries are case-sensitive on table/column names, ensure they match the case used in ingestion
        df = conn.query(f'SELECT * FROM "{table_name}"', ttl=3600) # Cache for 1 hour
        st.success(f"Data fetched successfully! ({len(df)} rows)")
        return df
    except Exception as e:
        st.error(f"Error fetching data from database: {e}")
        return pd.DataFrame()

# --- Utility Functions (Kept from previous version, only minor date-handling for SQLite) ---

def detect_columns(df):
    """Auto-detect relevant columns in the DataFrame"""
    columns = df.columns.tolist()
    source_col = next((col for col in columns if 'SOURCE' in col), 'SOURCE')
    date_col = next((col for col in columns if 'DATE' in col), 'TXN_DATE')
    hour_col = next((col for col in columns if 'HOUR' in col), None)
    events_col = next((col for col in columns if 'EVENTS' in col), 'EVENTS')
    return source_col, date_col, hour_col, events_col

def create_pivot_table(df, rows, columns, values, agg_func='sum'):
    """Create pivot table from DataFrame"""
    try:
        pivot = pd.pivot_table(
            df, 
            values=values, 
            index=rows, 
            columns=columns, 
            aggfunc=agg_func, 
            fill_value=0
        )
        return pivot
    except Exception as e:
        st.error(f"Error creating pivot table: {str(e)}")
        return None

def get_time_grouping_options(df, date_column):
    """Determine available time grouping options"""
    options = ['Daily']
    try:
        # Ensure date_column is datetime
        df[date_column] = pd.to_datetime(df[date_column])
        date_range = (df[date_column].max() - df[date_column].min()).days
        if date_range >= 7: options.append('Weekly')
        if date_range >= 28: options.append('Monthly')
        if date_range >= 84: options.append('Quarterly')
        if date_range >= 365: options.append('Yearly')
    except: 
        pass
    return options

def apply_time_grouping(df, date_column, grouping, hour_column=None):
    """Apply time-based grouping to DataFrame"""
    df_copy = df.copy()
    # Convert back to datetime for grouping operations
    df_copy[date_column] = pd.to_datetime(df_copy[date_column], errors='coerce')
    
    if grouping == 'Daily':
        df_copy['Time_Period'] = df_copy[date_column].dt.date
    elif grouping == 'Weekly':
        df_copy['Time_Period'] = df_copy[date_column].dt.to_period('W').astype(str)
    elif grouping == 'Monthly':
        df_copy['Time_Period'] = df_copy[date_column].dt.to_period('M').astype(str)
    elif grouping == 'Quarterly':
        df_copy['Time_Period'] = df_copy[date_column].dt.to_period('Q').astype(str)
    elif grouping == 'Yearly':
        df_copy['Time_Period'] = df_copy[date_column].dt.to_period('Y').astype(str)
    
    # Include hourly if selected and available
    if hour_column and hour_column in df_copy.columns and grouping == 'Hourly':
        df_copy['Time_Period'] = df_copy[date_column].dt.strftime('%Y-%m-%d') + ' H' + df_copy[hour_column].astype(str)

    return df_copy


# --- Streamlit UI and Logic ---

# Get DB Connection first
conn = get_db_connection()

if conn:
    st.markdown('<p class="section-header">📁 Data Management</p>', unsafe_allow_html=True)
    
    # Select or Upload Data Source
    source_option = st.radio(
        "Choose Data Source",
        ('Upload Excel to DB', 'Query Existing DB Data'),
        key='source_option'
    )
    
    if source_option == 'Upload Excel to DB':
        uploaded_file = st.file_uploader(
            f"Choose an Excel file (will be ingested into {SQLITE_DB_NAME})", 
            type=['xlsx', 'xls'], 
            help="Upload your middleware reports or metrics Excel file"
        )
        
        if uploaded_file is not None:
            col_ingest, col_policy = st.columns([1, 1])
            with col_ingest:
                if st.button("🚀 Ingest Data to Database", type="primary", use_container_width=True):
                    # For simplicity, we assume the uploaded file is always the 'EVENTS' format
                    ingestion_success = ingest_excel_to_db(
                        uploaded_file, 
                        conn, 
                        EVENT_TABLE_NAME, 
                        if_exists_policy='replace' # Replaces existing table content
                    )
                    if ingestion_success:
                        st.session_state.data_loaded_db = True
                        st.session_state.df = load_data_from_db(conn, EVENT_TABLE_NAME)

    
    elif source_option == 'Query Existing DB Data':
        # Simple button to load data from the designated table
        if st.button(f"⬇️ Load Data from '{EVENT_TABLE_NAME}' Table", type="primary", use_container_width=True):
            st.session_state.df = load_data_from_db(conn, EVENT_TABLE_NAME)
            if not st.session_state.df.empty:
                st.session_state.data_loaded_db = True
            else:
                st.warning(f"The table '{EVENT_TABLE_NAME}' might be empty or not exist.")

# Display basic info and data preview if data is loaded
if st.session_state.get('data_loaded_db') and not st.session_state.df.empty:
    df = st.session_state.df.copy()
    
    st.markdown("---")
    st.markdown('<p class="section-header">📋 Data Preview & Metadata</p>', unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns(3)
    with col1: st.metric("Source Table", EVENT_TABLE_NAME)
    with col2: st.metric("Total Rows", len(df))
    with col3: st.metric("Total Columns", len(df.columns))

    with st.expander("📋 View Data Preview (First 20 Rows)"):
        st.dataframe(df.head(20), use_container_width=True)

    # --- Analysis Section (Starts here, similar to original) ---
    
    st.markdown("---")
    st.markdown('<p class="section-header">⚙️ EVENTS Analysis Configuration</p>', unsafe_allow_html=True)
    
    # Find relevant columns
    columns = df.columns.tolist() 
    source_col, date_col, hour_col, events_col = detect_columns(df)

    col1, col2 = st.columns(2)
    with col1:
        source_column = st.selectbox("Source Column", columns, index=columns.index(source_col) if source_col in columns else 0)
        date_column = st.selectbox("Date Column", columns, index=columns.index(date_col) if date_col in columns else 0)
    with col2:
        hour_column_options = ['None'] + [col for col in columns if col.upper() == 'HOUR'] # Simple hour detection
        default_hour_index = hour_column_options.index(hour_col) if hour_col in hour_column_options else 0
        hour_column_sel = st.selectbox("Hour Column (Optional)", hour_column_options, index=default_hour_index)
        events_column = st.selectbox("Events Column", columns, index=columns.index(events_col) if events_col in columns else 0)
    
    hour_column = None if hour_column_sel == 'None' else hour_column_sel

    # Service selection
    try:
        services = df[source_column].unique().tolist()
        selected_services = st.multiselect(
            "Select Service(s)", services, default=services, help="Select one or more services to analyze"
        )
    except KeyError:
        st.error(f"Column '{source_column}' not found or invalid in data.")
        st.stop()
        
    # Time grouping
    time_grouping_options = get_time_grouping_options(df, date_column)
    if hour_column:
        time_grouping_options = ['Hourly'] + [opt for opt in time_grouping_options if opt != 'Daily'] 
    
    time_grouping = st.selectbox("Time Grouping", time_grouping_options)
    
    # Aggregation function
    agg_function = st.selectbox("Aggregation Function", list(AGG_FUNCTIONS.keys()))

    # Chart Configuration
    st.markdown("### 📈 Chart Configuration")
    col1_c, col2_c, col3_c = st.columns(3)
    with col1_c:
        chart_type = st.selectbox("Chart Type", ['Bar', 'Line', 'Pie', 'Area'])

    # Filter data based on selection
    filtered_df = df[df[source_column].isin(selected_services)].copy()

    # Apply time grouping and aggregate data
    if time_grouping == 'Hourly' and hour_column:
        filtered_df['Time_Period'] = filtered_df[date_column].astype(str) + ' H' + filtered_df[hour_column].astype(str)
        group_cols = ['Time_Period', source_column]
    else:
        filtered_df = apply_time_grouping(filtered_df, date_column, time_grouping, hour_column)
        group_cols = ['Time_Period', source_column]
        
    # Aggregate data
    agg_func = AGG_FUNCTIONS[agg_function]
    aggregated_df = filtered_df.groupby(group_cols)[events_column].agg(agg_func).reset_index()

    with col2_c:
        x_axis = st.selectbox("X-Axis", group_cols + [events_column], index=0)
    with col3_c:
        y_axis_options = [col for col in group_cols + [events_column] if col != x_axis]
        y_axis_default_index = y_axis_options.index(events_column) if events_column in y_axis_options else 0
        y_axis = st.selectbox("Y-Axis", y_axis_options, index=y_axis_default_index)

    # Generate Chart
    if st.button("📊 Generate Chart", type="primary", use_container_width=True):
        st.markdown("### 📊 Visualization Results")

        # Display summary statistics
        col1_m, col2_m, col3_m, col4_m = st.columns(4)
        with col1_m: st.metric("Total Events (Aggregated)", f"{aggregated_df[events_column].sum():,.0f}")
        with col2_m: st.metric("Average", f"{aggregated_df[events_column].mean():,.2f}")
        with col3_m: st.metric("Maximum", f"{aggregated_df[events_column].max():,.0f}")
        with col4_m: st.metric("Minimum", f"{aggregated_df[events_column].min():,.0f}")

        # Create chart based on type
        if chart_type == 'Bar':
            fig = px.bar(aggregated_df, x=x_axis, y=y_axis, color=source_column, title=f"{agg_function} of {events_column} by {x_axis}", barmode='group')
        elif chart_type == 'Line':
            fig = px.line(aggregated_df, x=x_axis, y=y_axis, color=source_column, title=f"{agg_function} of {events_column} by {x_axis}", markers=True)
        elif chart_type == 'Pie':
            pie_df = aggregated_df.groupby(source_column)[events_column].sum().reset_index()
            fig = px.pie(pie_df, values=events_column, names=source_column, title=f"Distribution of {events_column} by Service")
        elif chart_type == 'Area':
            fig = px.area(aggregated_df, x=x_axis, y=y_axis, color=source_column, title=f"{agg_function} of {events_column} by {x_axis}")

        fig.update_layout(height=600, template='plotly_white')
        st.plotly_chart(fig, use_container_width=True)

        # Display pivot table
        with st.expander("📋 View Pivot Table"):
            # Pivot table index columns
            pivot_index = [date_column] if time_grouping == 'Daily' and 'Time_Period' not in filtered_df.columns else ['Time_Period']
            
            # Pivot table column setup
            pivot_columns = [source_column]
            if time_grouping == 'Hourly' and hour_column:
                pivot_columns.append(hour_column)

            pivot = create_pivot_table(
                filtered_df, 
                rows=pivot_index, 
                columns=pivot_columns, 
                values=events_column, 
                agg_func=agg_func
            )
            
            if pivot is not None:
                st.dataframe(pivot, use_container_width=True)
    
else:
    if not conn:
        st.error("Please resolve the database connection issue to proceed. Ensure your `.streamlit/secrets.toml` file is set up.")
    else:
        st.info("👆 Please select an option above to load data from an Excel file (and ingest to DB) or from existing DB data.")
