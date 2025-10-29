import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import datetime 
import numpy as np
from sqlalchemy import create_engine, text, inspect 
import os
import io
import xlsxwriter
import tempfile 

# IMPORT THE NEW REPORT UTILS FUNCTIONS (Assumes report_utils.py is in place)
from report_utils import (
    generate_pdf_report, 
    generate_excel_report, 
    save_plotly_figure_to_png
)

# --- Configuration and Page Setup ---

# Database and Table Names 
SQLITE_DB_NAME = 'analytics.db'
EVT_TABLE_NAME = 'evt_data'
CPU_MEM_TABLE_NAME = 'cpu_mem_data'

st.set_page_config(page_title="SQLite Analytics Dashboard", layout="wide", page_icon="📊")

st.markdown("""
<style>
.main-header { font-size: 2.5rem; font-weight: bold; color: #1f77b4; text-align: center; margin-bottom: 2rem; }
.section-header { font-size: 1.5rem; font-weight: bold; color: #2c3e50; margin-top: 2rem; margin-bottom: 1rem; }
</style>
""", unsafe_allow_html=True)
st.markdown('<p class="main-header">📊 DB Analytics Dashboard (SQLite)</p>', unsafe_allow_html=True)

# Initialize session state
if 'conn' not in st.session_state:
    st.session_state.conn = None
if 'df' not in st.session_state:
    st.session_state.df = pd.DataFrame() 
if 'data_loaded_db' not in st.session_state:
    st.session_state.data_loaded_db = False
if 'is_single_file' not in st.session_state:
    st.session_state.is_single_file = True 
if 'files_to_ingest' not in st.session_state:
    st.session_state.files_to_ingest = []
if 'duplicates_to_confirm' not in st.session_state:
    st.session_state.duplicates_to_confirm = []


AGG_FUNCTIONS = {
    'SUM': 'sum',
    'AVERAGE': 'mean',
    'COUNT': 'count',
    'MAX': 'max',
    'MIN': 'min',
    'MEDIAN': 'median'
}

# --- New: DB Metrics Function ---
def get_db_metrics(db_file_name, conn, table_name):
    """Calculates the size of the SQLite DB file and total record count."""
    
    # 1. Get File Size
    size_metric = "0 MB"
    try:
        if os.path.exists(db_file_name):
            file_size_bytes = os.path.getsize(db_file_name)
            file_size_mb = file_size_bytes / (1024 * 1024)
            size_metric = f"{file_size_mb:.2f} MB"
    except Exception:
        pass 

    # 2. Get Total Record Count
    record_count = 0
    try:
        engine = create_engine(conn._instance.url)
        inspector = inspect(engine)
        if inspector.has_table(table_name):
            with engine.connect() as connection:
                result = connection.execute(text(f'SELECT COUNT(*) FROM "{table_name}"')).scalar()
                record_count = int(result)
    except Exception:
        pass 
    
    return size_metric, record_count

# --- Utility Functions ---

@st.cache_resource
def get_db_connection():
    """Establish and cache the SQLite database connection using Streamlit secrets."""
    try:
        conn = st.connection("sqlite", type="sql")
        st.session_state.conn = conn
        st.success(f"✅ SQLite connection established to **{SQLITE_DB_NAME}**.")
        return conn
    except Exception as e:
        st.error(f"❌ Failed to connect to SQLite. Check your **.streamlit/secrets.toml**. Error: {e}")
        return None

def load_data_from_db(conn, table_name):
    """Fetch all data from the database table."""
    try:
        st.info(f"Fetching data from database table: **{table_name}**...")
        df = conn.query(f'SELECT * FROM "{table_name}"', ttl=3600) 
        if 'TXN_DATE' in df.columns:
            df['TXN_DATE'] = pd.to_datetime(df['TXN_DATE'], errors='coerce')
        if 'DATE' in df.columns:
             df['DATE'] = pd.to_datetime(df['DATE'], errors='coerce')

        st.success(f"Data fetched successfully! ({len(df)} rows)")
        return df
    except Exception as e:
        st.error(f"Error fetching data from database. Error: {e}")
        return pd.DataFrame()

def to_excel(df, sheet_name="Export"):
    """Convert DataFrame to an Excel (xlsx) file in memory (bytes)."""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, sheet_name=sheet_name, index=False)
    processed_data = output.getvalue()
    return processed_data

# --- File Parsing/Ingestion Utility Functions ---

def parse_excel_files(file_list, conn, table_name, file_type):
    """Reads excel files and performs duplication checks."""
    engine = create_engine(conn._instance.url)
    existing_files = []
    if inspect(engine).has_table(table_name):
        try:
            existing_files = pd.read_sql(f'SELECT DISTINCT "Original_File" FROM "{table_name}"', engine)['Original_File'].tolist()
        except Exception:
            existing_files = [] 

    new_files_to_ingest = []
    duplicate_files = []

    for f in file_list:
        file_name = f.name if hasattr(f, 'name') else os.path.basename(f)
        
        if file_name in existing_files:
            duplicate_files.append((f, file_name))
        else:
            new_files_to_ingest.append((f, file_name))
            
    st.session_state.files_to_ingest = new_files_to_ingest
    return duplicate_files

def read_and_process_evt(file_list_tuples):
    """Processes EVT files into a single DataFrame ready for ingestion."""
    df_list = []
    def read_evt_sheets(file, file_name):
        xls = pd.ExcelFile(file)
        evt_sheets = [sheet for sheet in xls.sheet_names if sheet.upper().startswith("EVT")]
        parsed_dfs = []
        for sheet in evt_sheets:
            sheet_df = xls.parse(sheet)
            sheet_df['Original_File'] = file_name 
            parsed_dfs.append(sheet_df)
        return parsed_dfs

    for f_obj, file_name in file_list_tuples:
        try:
            evt_sheets = read_evt_sheets(f_obj, file_name)
            if evt_sheets:
                df_list.extend(evt_sheets)
            else:
                st.warning(f"File {file_name} ignored (no EVT sheets found)")
        except Exception as e:
            st.error(f"Could not read {file_name}: {e}")
            
    if not df_list: return pd.DataFrame()
    
    df_combined = pd.concat(df_list, ignore_index=True)
    df_combined['TXN_DATE'] = pd.to_datetime(df_combined['TXN_DATE'], format="%m/%d/%Y", errors='coerce').dt.date
    return df_combined

def read_and_process_cpu_mem(file_list_tuples):
    """Processes CPU/Mem files into a single DataFrame ready for ingestion."""
    df_list = []

    def read_cpu_mem_sheets(file):
        xls = pd.ExcelFile(file)
        cpu_mem_sheets = [sheet for sheet in xls.sheet_names if not sheet.upper().startswith('EVT')]
        sheet_dfs = []
        for sheet in cpu_mem_sheets:
            df_sheet = None
            for header_row in range(10):
                try:
                    df_try = pd.read_excel(file, sheet_name=sheet, header=header_row)
                    required_cols = ["Row Labels", "Max of EVENTS", "CPU"]
                    if all(col in df_try.columns for col in required_cols):
                        df_sheet = df_try[required_cols].copy()
                        df_sheet.rename(columns={"Row Labels": "DATE", "Max of EVENTS": 'MAX_EVENTS', 'CPU': 'CPU'}, inplace=True)
                        break
                except Exception:
                    continue
            if df_sheet is not None:
                sheet_dfs.append(df_sheet)
        return sheet_dfs

    for f_obj, file_name in file_list_tuples:
        try:
            cpu_mem_sheets = read_cpu_mem_sheets(f_obj)
            if cpu_mem_sheets:
                for sheet_df in cpu_mem_sheets:
                    sheet_df['Original_File'] = file_name # NEW: Store filename
                    df_list.append(sheet_df)
            else:
                st.warning(f"File {file_name} ignored (no CPU/Memory Utilization sheets found)")
        except Exception as e:
            st.error(f"Could not read {file_name}: {e}")

    if not df_list: return pd.DataFrame()
    
    df_combined = pd.concat(df_list, ignore_index=True)
    df_combined = df_combined[df_combined['DATE'].notna()].copy()
    df_combined['DATE'] = pd.to_datetime(df_combined['DATE'], errors='coerce').dt.date
    df_combined = df_combined[df_combined["DATE"].notna()].copy()
    
    return df_combined

def ingest_data_to_db(df, conn, table_name, file_names_to_delete=None):
    """Deletes old records (if replacing) and appends new data."""
    engine = create_engine(conn._instance.url)
    
    with engine.begin() as connection:
        if file_names_to_delete:
            # 1. Delete old versions of files being replaced
            for file_name in file_names_to_delete:
                connection.execute(text(f'DELETE FROM "{table_name}" WHERE "Original_File" = :file_name'), 
                                   {'file_name': file_name})
        
        # 2. Append new data
        if not df.empty:
            df.to_sql(table_name, connection, if_exists='append', index=False)
            st.success(f"🎉 Successfully ingested {len(df)} new/replaced records.")
        else:
             st.info("No new data to ingest.")
    
    # 3. Reload session state
    st.session_state.df = load_data_from_db(conn, table_name)
    st.session_state.data_loaded_db = True
    st.rerun()

def detect_columns(df):
    columns = df.columns.tolist()
    source_col = next((col for col in columns if 'SOURCE' in col), 'SOURCE')
    date_col = next((col for col in columns if 'DATE' in col), 'TXN_DATE')
    hour_col = next((col for col in columns if 'HOUR' in col), None)
    events_col = next((col for col in columns if 'EVENTS' in col), 'EVENTS')
    return source_col, date_col, hour_col, events_col

def create_pivot_table(df, rows, columns, values, agg_func='sum'):
    try:
        pivot = pd.pivot_table(df, values=values, index=rows, columns=columns, aggfunc=agg_func, fill_value=0)
        if isinstance(pivot.columns, pd.MultiIndex) or len(columns) > 0:
            pivot['Grand Total'] = pivot.sum(axis=1)
        else:
             pivot.loc['Grand Total', :] = pivot.sum(axis=0)
        return pivot
    except Exception as e:
        st.error(f"Error creating pivot table: {str(e)}")
        return None

def apply_time_grouping(df, date_column, grouping, hour_column=None):
    df_copy = df.copy()
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
    
    if hour_column and hour_column in df_copy.columns and grouping == 'Hourly':
        df_copy['Time_Period'] = df_copy[date_column].dt.strftime('%Y-%m-%d') + ' H' + df_copy[hour_column].astype(str)
    
    if 'Time_Period' not in df_copy.columns:
        df_copy['Time_Period'] = df_copy[date_column]

    return df_copy

# --- Main Streamlit Execution Flow ---

# 1. Establish connection
conn = get_db_connection()

if conn:
    st.markdown('<p class="section-header">📁 Data Management and Ingestion</p>', unsafe_allow_html=True)
    
    excel_type = st.selectbox('Select Data/Table Type for Analysis', ['EVT', 'CPU and Memory Utilization'])
    target_table = EVT_TABLE_NAME if excel_type == 'EVT' else CPU_MEM_TABLE_NAME

    st.write("### 📂 Ingest/Load Data")
    
    upload_mode = st.radio(
        "Select Upload Mode:",
        ['Upload Single File', 'Upload Multiple Files / Folder Path'],
        key='upload_mode'
    )
    
    file_list = []
    is_single_file_upload = (upload_mode == 'Upload Single File')

    # --- File/Folder Selection UI ---
    if upload_mode == 'Upload Single File':
        uploaded_file = st.file_uploader("Upload a single Excel file", type=["xlsx", "xls"], accept_multiple_files=False)
        if uploaded_file:
            file_list = [uploaded_file]
            
    else: # Upload Multiple Files / Folder Path
        col_folder, col_multi_upload = st.columns(2)
        with col_folder:
            folder_path = st.text_input("Enter a folder path to load all Excel files:")
        with col_multi_upload:
            excel_files = st.file_uploader("Or upload multiple Excel files", type=["xlsx", "xls"], accept_multiple_files=True)
        
        if folder_path and os.path.isdir(folder_path):
            # Read paths from folder
            file_list = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if (f.endswith('.xlsx') or f.endswith('.xls')) and not f.startswith('~$')]
        elif excel_files:
            # Read uploaded files
            file_list = excel_files
    
    # --- Ingestion Button (START) ---
    
    if file_list and not st.session_state.duplicates_to_confirm:
        if st.button(f"🚀 Prepare {excel_type} Data for Ingestion", type="primary", use_container_width=True):
            with st.spinner(f'Checking {len(file_list)} file(s) for duplicates...'):
                
                duplicates = parse_excel_files(file_list, conn, target_table, excel_type)

                if duplicates:
                    st.session_state.duplicates_to_confirm = duplicates
                    st.warning(f"Found {len(duplicates)} file(s) already present in the database. Scroll down to confirm replacement.")
                    st.rerun()
                elif st.session_state.files_to_ingest:
                    st.session_state.duplicates_to_confirm = []
                    st.session_state.is_single_file = is_single_file_upload
                    st.info("No duplicates found. Proceeding with ingestion...")
                    
                    if excel_type == 'EVT':
                        df_to_ingest = read_and_process_evt(st.session_state.files_to_ingest)
                    else:
                        df_to_ingest = read_and_process_cpu_mem(st.session_state.files_to_ingest)
                        
                    if not df_to_ingest.empty:
                        ingest_data_to_db(df_to_ingest, conn, target_table)

    # --- Duplicate Confirmation UI ---
    if st.session_state.duplicates_to_confirm:
        
        st.markdown('---')
        st.error('⚠️ DUPLICATE FILE CONFIRMATION REQUIRED')
        st.info("The files below are already in the database. Choose whether to replace them.")
        
        duplicate_files = st.session_state.duplicates_to_confirm
        files_to_replace = st.multiselect(
            'Select files to **REPLACE** (delete old version and insert new)',
            [name for _, name in duplicate_files],
            key='files_to_replace'
        )

        if st.button('✅ Confirm Ingestion and Replace', type='primary'):
            files_to_process = st.session_state.files_to_ingest
            files_to_delete_names = []
            
            for f_obj, file_name in duplicate_files:
                if file_name in files_to_replace:
                    files_to_process.append((f_obj, file_name))
                    files_to_delete_names.append(file_name)

            if excel_type == 'EVT':
                df_to_ingest = read_and_process_evt(files_to_process)
            else:
                df_to_ingest = read_and_process_cpu_mem(files_to_process)
                
            if not df_to_ingest.empty or files_to_delete_names:
                ingest_data_to_db(df_to_ingest, conn, target_table, file_names_to_delete=files_to_delete_names)

            st.session_state.duplicates_to_confirm = [] 
            st.rerun()

    # --- Load from DB Button ---
    if st.button(f"⬇️ Load Data from '{target_table}' Table for Analysis", use_container_width=True, key='load_db_btn'):
        st.session_state.df = load_data_from_db(conn, target_table)
        st.session_state.data_loaded_db = not st.session_state.df.empty
        if st.session_state.data_loaded_db:
            if excel_type == 'EVT' and not st.session_state.df.empty:
                 month_count = len(st.session_state.df['TXN_DATE'].dt.to_period('M').unique())
                 st.session_state.is_single_file = (month_count <= 1 and is_single_file_upload)
            else:
                 st.session_state.is_single_file = False
            st.rerun() 
            
    # --- DB Storage Metrics Display ---
    if conn:
        db_size, total_records = get_db_metrics(SQLITE_DB_NAME, conn, target_table)
        
        st.markdown('---')
        st.markdown('##### 📊 Current DB Storage Metrics')
        
        # Display the DB location for clarity
        st.caption(f"DB Location: {os.path.abspath(SQLITE_DB_NAME)}")
        
        col_s1, col_s2 = st.columns(2)
        with col_s1:
            st.metric(label=f"💾 Size of {SQLITE_DB_NAME}", value=db_size)
        with col_s2:
            st.metric(label=f"🔢 Total Records in '{target_table}'", value=f"{total_records:,}")


    # ----------------------------------------
    # --- NEW: VIEW AND DELETE FILES SECTION ---
    # ----------------------------------------
    st.markdown('---')
    st.markdown('##### 🔎 View & Manage Ingested Files')
    
    if 'Original_File' in st.session_state.df.columns:
        all_unique_files = sorted(st.session_state.df['Original_File'].unique())
    else:
        all_unique_files = []

    # 1. View Files Table
    if st.button(f'Show File Inventory for "{target_table}"', key='show_files', use_container_width=True):
        try:
            engine = create_engine(conn._instance.url)
            query = f"""
                SELECT "Original_File", COUNT(*) as "Record_Count"
                FROM "{target_table}"
                GROUP BY "Original_File"
                ORDER BY "Record_Count" DESC
            """
            files_df = pd.read_sql(query, engine)
            st.dataframe(files_df, use_container_width=True)
        except Exception as e:
            st.error(f"Error viewing files: {e}")


    # 2. Delete Files Controls
    if all_unique_files:
        st.markdown('###### 🗑️ Delete Files Permanently')
        
        files_to_delete_select = st.multiselect(
            'Select files to permanently delete from DB:',
            all_unique_files,
            key='files_to_delete_key'
        )

        if st.button('💣 DELETE SELECTED FILES', type='secondary', use_container_width=True):
            if files_to_delete_select:
                engine = create_engine(conn._instance.url)
                
                with st.spinner(f"Deleting {len(files_to_delete_select)} files..."):
                    with engine.begin() as connection:
                        for file_name in files_to_delete_select:
                            connection.execute(
                                text(f'DELETE FROM "{target_table}" WHERE "Original_File" = :file_name'), 
                                {'file_name': file_name}
                            )
                    
                    st.success(f"Successfully deleted {len(files_to_delete_select)} file(s) from '{target_table}'.")
                    
                    # Refresh the data and metrics
                    st.session_state.data_loaded_db = False
                    st.rerun()
            else:
                st.warning("Please select at least one file to delete.")


# --- Analysis Section (Conditionally rendered) ---

df_loaded = st.session_state.df.copy()

if st.session_state.get('data_loaded_db') and not df_loaded.empty:
    
    if excel_type == 'EVT':
        date_col = 'TXN_DATE'
        event_col = 'EVENTS'
        source_col = 'SOURCE'
        hour_col_present = 'HOUR' in df_loaded.columns
        if not hour_col_present:
             st.warning("The 'HOUR' column is missing from the loaded EVT data. Hourly granularity is unavailable.")
    else: 
        date_col = 'DATE'
        event_col = 'MAX_EVENTS' 
        source_col = None 
        hour_col_present = False 

    st.markdown("---")
    st.subheader(f"Data Analysis: {excel_type} Data")
    
    # -----------------------------------------------
    # --- INDIVIDUAL FILE DOWNLOAD SECTION ---
    # -----------------------------------------------
    st.markdown('### 📥 Download Original Files')
    
    if excel_type == 'EVT' and 'Original_File' in df_loaded.columns:
        original_files = sorted(df_loaded['Original_File'].unique())
        
        col_file_select, col_file_download = st.columns([2, 1])
        with col_file_select:
            selected_download_file = st.selectbox(
                'Select Original File to Download',
                original_files,
                help="Select a file to download the exact data set that was uploaded from that file."
            )

        if selected_download_file:
            df_to_download = df_loaded[df_loaded['Original_File'] == selected_download_file].copy()
            df_to_download.drop(columns=['Original_File'], inplace=True, errors='ignore')
            
            excel_data = to_excel(df_to_download, sheet_name=target_table)
            
            with col_file_download:
                st.download_button(
                    label=f"Download {selected_download_file}",
                    data=excel_data,
                    file_name=selected_download_file,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    type="primary",
                    use_container_width=True
                )
    else:
        st.info("The individual file download option is available only for EVT data with multiple file uploads.")
    
    st.markdown('---')


    # ------------------------------------
    # --- EVT Analysis (Traffic Volume) ---
    # ------------------------------------
    if excel_type == 'EVT':
        
        all_sources = sorted(df_loaded[source_col].dropna().unique())
        df_loaded['TXN_DATE_Month'] = df_loaded[date_col].dt.to_period('M').astype(str)
        all_months = sorted(df_loaded['TXN_DATE_Month'].unique())

        st.subheader("Traffic Volume (EVT) Configuration")
        col_f1, col_f2 = st.columns(2)
        with col_f1:
            selected_sources = st.multiselect('Filter by Source', all_sources, default=all_sources)
        with col_f2:
            selected_months = st.multiselect('Filter by Month', all_months, default=all_months)

        # GRANULARITY CHANGE: Conditional Granularity
        granularity_options = ['Day', 'Week', 'Month']
        if hour_col_present:
             granularity_options.insert(0, 'Hourly')
             
        if not st.session_state.is_single_file:
            granularity_options.append('Quarterly')
            
        granularity = st.selectbox('Graph Granularity', granularity_options)
        
        # HOURLY FILTER: Show specific day/hour selectors 
        if granularity == 'Hourly' and st.session_state.is_single_file and hour_col_present:
            st.markdown('##### Pinpoint Day & Hour Filter')
            
            all_days = sorted(df_loaded[date_col].dt.date.unique())
            all_hours = sorted(df_loaded['HOUR'].dropna().unique())
            
            col_dh1, col_dh2 = st.columns(2)
            with col_dh1:
                selected_day = st.selectbox('Select Specific Day', ['All'] + all_days) 
            with col_dh2:
                selected_hour = st.selectbox('Select Specific Hour', ['All'] + all_hours)
                
            if selected_day != 'All':
                df_loaded = df_loaded[df_loaded[date_col].dt.date == selected_day] 
            if selected_hour != 'All':
                df_loaded = df_loaded[df_loaded['HOUR'] == selected_hour]
                
            if df_loaded.empty:
                st.warning("No data found for the selected Day and Hour combination. Please adjust the filters.")
                st.stop()


        agg_options = list(AGG_FUNCTIONS.keys())
        selected_aggs = st.multiselect("Select aggregation(s) for graph", agg_options, default=["SUM"])
        
        df_filtered = df_loaded[
            df_loaded[source_col].isin(selected_sources) & 
            df_loaded['TXN_DATE_Month'].isin(selected_months)
        ].copy()

        # Grouping logic adjusted for new granularity options
        if granularity == 'Hourly':
            df_filtered['Group_Date'] = df_filtered[date_col].dt.strftime('%Y-%m-%d') + ' H' + df_filtered['HOUR'].astype(str)
            group_cols = ['Group_Date', source_col]
            xlabel = 'Date & Hour'
        elif granularity == 'Day':
            df_filtered['Group_Date'] = df_filtered[date_col].dt.date
            group_cols = ['Group_Date', source_col]
            xlabel = 'Date'
        elif granularity == 'Week':
            df_filtered['Group_Date'] = df_filtered[date_col].dt.to_period('W').astype(str)
            group_cols = ['Group_Date', source_col]
            xlabel = 'Week'
        elif granularity == 'Month':
            df_filtered['Group_Date'] = df_filtered['TXN_DATE_Month']
            group_cols = ['Group_Date', source_col]
            xlabel = 'Month'
        else: # Quarterly
            df_filtered['Group_Date'] = df_filtered[date_col].dt.to_period('Q').astype(str)
            group_cols = ['Group_Date', source_col]
            xlabel = 'Quarter'

        agg_funcs = AGG_FUNCTIONS
        
        graph_types = ['Bar', 'Line', 'Scatter', 'Area', 'Step Line', "Horizontal Bar", "Pie"]
        graph_type = st.selectbox('Select graph type', graph_types)

        # Plotting Loop 
        last_fig = None
        for agg in selected_aggs:
            grouped_df = df_filtered.groupby(group_cols)[event_col].agg(agg_funcs[agg.upper()]).unstack(fill_value=0)
            data = []
            
            for source in grouped_df.columns:
                if graph_type == 'Bar':
                    trace = go.Bar(x=grouped_df.index.astype(str), y=grouped_df[source], name=source, hoverinfo='x+y+name')
                elif graph_type == 'Line' or graph_type == 'Step Line':
                    trace = go.Scatter(x=grouped_df.index.astype(str), y=grouped_df[source], name=source, 
                                       mode='lines+markers', line_shape='hv' if graph_type == 'Step Line' else 'linear', hoverinfo='x+y+name')
                elif graph_type == 'Scatter':
                    trace = go.Scatter(x=grouped_df.index.astype(str), y=grouped_df[source], name=source, mode='markers', hoverinfo='x+y+name')
                elif graph_type == 'Area':
                    trace = go.Scatter(x=grouped_df.index.astype(str), y=grouped_df[source], name=source, mode='lines', fill='tozeroy', hoverinfo='x+y+name')
                elif graph_type == 'Horizontal Bar':
                    trace = go.Bar(y=grouped_df.index.astype(str), x=grouped_df[source], name=source, orientation='h', hoverinfo='x+y+name')
                elif graph_type == 'Pie':
                    pie_values = grouped_df[source].sum()
                    trace = go.Pie(labels=['Total'], values=[pie_values], name=source, hoverinfo='label+value+name', title=source)
                
                data.append(trace)

            layout = go.Layout(
                barmode='group' if graph_type == 'Bar' else 'stack',
                title=f"{agg.title()} of Transaction Volume per {xlabel} by Source",
                xaxis={'title': xlabel} if graph_type not in ['Horizontal Bar', 'Pie'] else None,
                yaxis={'title': f"{agg.title()} of Events"} if graph_type not in ['Horizontal Bar', 'Pie'] else None,
                legend={'title': 'Source'}
            )
            fig = go.Figure(data=data, layout=layout)
            last_fig = fig 
            st.plotly_chart(fig, use_container_width=True)
            
        # --- PDF/Excel Report Generation UI for EVT ---
        st.markdown('---')
        st.subheader("Download Professional Reports")
        
        report_sources = st.multiselect('Select Source(s) for Report', all_sources, default=all_sources, key='report_src')
        report_months = st.multiselect('Select Month(s) for Report', all_months, default=all_months, key='report_month')
        
        col_pdf, col_excel = st.columns(2)

        # 1. Generate PDF Report
        with col_pdf:
            if st.button('📄 Generate PDF Report', type="primary", use_container_width=True):
                if last_fig is not None:
                    with st.spinner('Generating PDF report, please wait...'):
                        report_df = df_loaded[
                            df_loaded[source_col].isin(report_sources) & 
                            df_loaded['TXN_DATE_Month'].isin(report_months)
                        ].copy()

                        summary_text = f"""
**EVT Data Analysis Report**
Date Range: {report_df[date_col].min().strftime('%Y-%m-%d')} to {report_df[date_col].max().strftime('%Y-%m-%d')}
Services Analyzed: {', '.join(report_sources) if report_sources else 'All'}
Granularity: {granularity}
Total Events (SUM): {report_df[event_col].sum():,.0f}
"""
                        table1 = report_df.pivot_table(index=report_df[date_col].dt.day, columns='TXN_DATE_Month', values=event_col, aggfunc='sum', fill_value=0)
                        table1.index.name = "Day"

                        tables = {
                            "Total Daily Volume": table1,
                            "Raw Data Sample (First 100)": report_df.head(100)
                        }

                        fig_path = save_plotly_figure_to_png(last_fig)
                        figures = {"Volume Trend": fig_path}
                        
                        pdf_filename = f"EVT_Report_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
                        pdf_path = os.path.join(tempfile.gettempdir(), pdf_filename)

                        generate_pdf_report(summary_text, tables, figures, pdf_path)

                        with open(pdf_path, 'rb') as f:
                            st.download_button(
                                label="Download PDF Report", data=f.read(), file_name=pdf_filename, mime="application/pdf", key='download_pdf_evt'
                            )
                        if os.path.exists(fig_path):
                            os.remove(fig_path)
                else:
                     st.warning("Please generate a chart first.")

        # 2. Generate Excel Report
        with col_excel:
            if st.button('XLSX Generate Excel Report', use_container_width=True):
                with st.spinner('Generating Excel report...'):
                    report_df = df_loaded[
                        df_loaded[source_col].isin(report_sources) & 
                        df_loaded['TXN_DATE_Month'].isin(report_months)
                    ].copy()

                    excel_tables = {
                        "Filtered_Raw_Data": report_df,
                        "Pivot_Daily_Volume": grouped_df
                    }
                    excel_filename = f"EVT_Data_Export_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
                    excel_path = os.path.join(tempfile.gettempdir(), excel_filename)
                    
                    generate_excel_report(excel_tables, excel_path)
                    
                    with open(excel_path, 'rb') as f:
                        st.download_button(
                            label="Download Excel Report", data=f.read(), file_name=excel_filename, mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key='download_excel_evt'
                        )

    # ----------------------------------------
    # --- CPU/Mem Analysis (Dual-Axis Plot) ---
    # ----------------------------------------
    else: # excel_type == 'CPU and Memory Utilization'

        if 'MAX_EVENTS' not in df_loaded.columns or 'CPU' not in df_loaded.columns:
            st.error("Missing expected columns (MAX_EVENTS, CPU) for this analysis type. Ensure files were ingested correctly.")
            st.stop()
            
        date_col = 'DATE' 

        df_loaded['Month'] = df_loaded[date_col].dt.to_period('M').astype(str)
        all_months = sorted(df_loaded['Month'].unique())
        
        st.subheader("CPU and Memory Utilization Analysis")
        selected_months = st.multiselect('Filter by Month', all_months, default=all_months)
        granularity = st.selectbox('Graph Granularity', ['Day', 'Month'])

        df_filtered = df_loaded[df_loaded['Month'].isin(selected_months)].copy()

        if granularity == 'Day':
            group_cols = [df_filtered[date_col].dt.date]
            x_label = 'Date'
        else: # Month
            group_cols = [df_filtered[date_col].dt.to_period('M').astype(str)]
            x_label = 'Month'

        grouped_df = df_filtered.groupby(group_cols).agg({'MAX_EVENTS': 'sum', 'CPU': 'mean'})

        bar_trace = go.Bar(
            x=grouped_df.index.astype(str), y=grouped_df['MAX_EVENTS'], yaxis='y1',
            name='Max Events (SUM)', marker_color='rgba(55, 83, 109, 0.7)', hoverinfo='x+y+name'
        )
        line_trace = go.Scatter(
            x=grouped_df.index.astype(str), y=grouped_df['CPU'], name='CPU (MEAN)', yaxis='y2', 
            mode='lines+markers', line={'color': 'red', 'width': 3}, hoverinfo='x+y+name'
        )
        
        layout = go.Layout(
            title=f'Transaction Volume and CPU by {x_label}',
            xaxis={'title': x_label},
            yaxis=dict(title='Max Events (SUM)', side='left', showgrid=False),
            yaxis2=dict(title='CPU (MEAN)', overlaying='y', side='right', showgrid=False),
            legend={'title': 'Legend'}, barmode='group',
            margin=dict(l=60, r=60, t=60, b=60)
        )
        fig = go.Figure(data=[bar_trace, line_trace], layout=layout)
        st.plotly_chart(fig, use_container_width=True)
        
        # --- PDF/Excel Report Generation UI for CPU/Mem ---
        st.markdown('---')
        st.subheader('Download Report')

        col_pdf_cpu, col_excel_cpu = st.columns(2)

        # 1. Generate PDF Report (CPU/MEM)
        with col_pdf_cpu:
            if st.button('📄 Generate PDF Report', type="primary", use_container_width=True, key='pdf_cpu'):
                with st.spinner('Generating PDF report...'):
                    summary_text = f"""
**CPU/Memory Utilization Data Analysis Report**
Date Range: {df_loaded[date_col].min().strftime('%Y-%m-%d')} to {df_loaded[date_col].max().strftime('%Y-%m-%d')}
Granularity: {granularity}
Key Metrics: Avg Events {df_loaded[event_col].mean():.2f} | Avg CPU {df_loaded['CPU'].mean():.2f}
"""
                    tables = {'Aggregated Data': grouped_df, 'Raw Data Sample': df_loaded.head(100)}
                    
                    fig_path = save_plotly_figure_to_png(fig) 
                    figures = {"Volume & CPU Trend": fig_path}
                    
                    pdf_filename = f"CPUMEM_Report_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
                    pdf_path = os.path.join(tempfile.gettempdir(), pdf_filename)

                    generate_pdf_report(summary_text, tables, figures, pdf_path)

                    with open(pdf_path, 'rb') as f:
                        st.download_button(
                            label="Download PDF Report", data=f.read(), file_name=pdf_filename, mime="application/pdf", key='download_pdf_cpu'
                        )
                    if os.path.exists(fig_path):
                        os.remove(fig_path)

        # 2. Generate Excel Report (CPU/MEM)
        with col_excel_cpu:
            if st.button('XLSX Generate Excel Report', use_container_width=True, key='excel_cpu'):
                with st.spinner('Generating Excel report...'):
                    excel_tables = {
                        "Aggregated_Data": grouped_df,
                        "Raw_Data_Sample": df_loaded.head(100)
                    }
                    excel_filename = f"CPUMEM_Data_Export_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
                    excel_path = os.path.join(tempfile.gettempdir(), excel_filename)
                    
                    generate_excel_report(excel_tables, excel_path)
                    
                    with open(excel_path, 'rb') as f:
                        st.download_button(
                            label="Download Excel Report", data=f.read(), file_name=excel_filename, mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key='download_excel_cpu'
                        )
