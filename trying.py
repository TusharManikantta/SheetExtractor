import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import datetime 
from datetime import timedelta 
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

# Custom Mapping: SOURCE (EVT) to HostName (CPU/Mem)
RPI_HOST_MAPPING = {
    'RPI': 'LRCHS0N01',
    # Add other SOURCE-Hostname mappings here if needed
}
HOST_TO_SOURCE = {v: k for k, v in RPI_HOST_MAPPING.items()}


st.set_page_config(page_title="SQLite Analytics Dashboard", layout="wide", page_icon="📊")

st.markdown("""
<style>
.main-header { font-size: 2.5rem; font-weight: bold; color: #1f77b4; text-align: center; margin-bottom: 2rem; }
.section-header { font-size: 1.5rem; font-weight: bold; color: #2c3e50; margin-top: 2rem; margin-bottom: 1rem; }
</style>
""", unsafe_allow_html=True)
st.markdown('<p class="main-header">📊 DB Analytics Dashboard (SQLite)</p>', unsafe_allow_html=True)

# Initialize session state
if 'conn' not in st.session_state: st.session_state.conn = None
if 'df' not in st.session_state: st.session_state.df = pd.DataFrame() 
if 'data_loaded_db' not in st.session_state: st.session_state.data_loaded_db = False
if 'is_single_file' not in st.session_state: st.session_state.is_single_file = True 
if 'files_to_ingest' not in st.session_state: st.session_state.files_to_ingest = []
if 'duplicates_to_confirm' not in st.session_state: st.session_state.duplicates_to_confirm = []


AGG_FUNCTIONS = {
    'SUM': 'sum', 'AVERAGE': 'mean', 'COUNT': 'count',
    'MAX': 'max', 'MIN': 'min', 'MEDIAN': 'median'
}

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

def load_data_from_db(conn, table_name):
    """Fetch all data from the database table."""
    try:
        st.info(f"Fetching data from database table: **{table_name}**...")
        df = conn.query(f'SELECT * FROM "{table_name}"', ttl=3600) 
        if 'TXN_DATE' in df.columns:
            df['TXN_DATE'] = pd.to_datetime(df['TXN_DATE'], errors='coerce')
        if 'DATE' in df.columns:
             df['DATE'] = pd.to_datetime(df['DATE'], errors='coerce')
        if 'Date_Time' in df.columns:
            df['Date_Time'] = pd.to_datetime(df['Date_Time'], errors='coerce')

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
    """Processes EVT files, combining TXN_DATE and Hour into a single datetime column."""
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
    
    # 1. CLEAN UP DATES/HOURS
    df_combined['TXN_DATE'] = pd.to_datetime(df_combined['TXN_DATE'], format="%m/%d/%Y", errors='coerce')
    
    # 2. CREATE THE CANONICAL JOIN KEY (Date_Time)
    df_combined['Hour'] = pd.to_numeric(df_combined['Hour'], errors='coerce').fillna(0).astype(int)
    df_combined['Date_Time'] = df_combined.apply(
        lambda row: row['TXN_DATE'] + timedelta(hours=row['Hour']), axis=1
    )
    
    # Drop original columns used for creation
    df_combined.drop(columns=['TXN_DATE', 'Hour'], inplace=True, errors='ignore')
    
    return df_combined

def read_and_process_cpu_mem(file_list_tuples):
    """Processes CPU/Mem files, using 'date' as the canonical join key (Date_Time)."""
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
                    required_cols = ["hostName", "date", "cpu_total_pct"] 
                    if all(col in df_try.columns for col in required_cols): 
                        df_sheet = df_try.copy()
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
                    sheet_df['Original_File'] = file_name 
                    df_list.append(sheet_df)
            else:
                st.warning(f"File {file_name} ignored (no CPU/Memory Utilization sheets found)")
        except Exception as e:
            st.error(f"Could not read {file_name}: {e}")

    if not df_list: return pd.DataFrame()
    
    df_combined = pd.concat(df_list, ignore_index=True)
    
    # 1. CLEAN UP DATES/TIMES (Assuming the 'date' column contains the full timestamp)
    df_combined = df_combined[df_combined['date'].notna()].copy()
    # 2. CREATE THE CANONICAL JOIN KEY (Date_Time)
    df_combined['Date_Time'] = pd.to_datetime(df_combined['date'], errors='coerce')
    
    df_combined = df_combined[df_combined['Date_Time'].notna()]

    # Drop original date column
    df_combined.drop(columns=['date'], inplace=True, errors='ignore')
    
    # Ensure cpu_total_pct is numeric for aggregation
    df_combined['cpu_total_pct'] = pd.to_numeric(df_combined['cpu_total_pct'], errors='coerce')
    
    return df_combined

def ingest_data_to_db(df, conn, table_name, file_names_to_delete=None):
    """Deletes old records (if replacing) and appends new data."""
    engine = create_engine(conn._instance.url)
    
    with engine.begin() as connection:
        if file_names_to_delete:
            for file_name in file_names_to_delete:
                connection.execute(text(f'DELETE FROM "{table_name}" WHERE "Original_File" = :file_name'), 
                                   {'file_name': file_name})
        
        if not df.empty:
            # FIX: Ensure all columns exist in the target table on first insertion
            df.to_sql(table_name, connection, if_exists='append', index=False)
            st.success(f"🎉 Successfully ingested {len(df)} new/replaced records.")
        else:
             st.info("No new data to ingest.")
    
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

# --- Analysis Mode Selector (Placed higher up) ---
ANALYSIS_MODES = ['Cross-Table: Volume vs. CPU', 'Standard EVT Analysis', 'Standard CPU/Mem Analysis']
analysis_mode = st.sidebar.selectbox('Select Analysis Mode', ANALYSIS_MODES)


if conn:
    st.markdown('<p class="section-header">📁 Data Management and Ingestion</p>', unsafe_allow_html=True)
    
    if analysis_mode == 'Cross-Table: Volume vs. CPU':
        excel_type = st.selectbox('Select Data/Table Type to Ingest', ['EVT', 'CPU and Memory Utilization'])
        target_table = EVT_TABLE_NAME if excel_type == 'EVT' else CPU_MEM_TABLE_NAME
        
    elif analysis_mode == 'Standard EVT Analysis':
        excel_type = 'EVT'
        target_table = EVT_TABLE_NAME
    else:
        excel_type = 'CPU and Memory Utilization'
        target_table = CPU_MEM_TABLE_NAME


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
            file_list = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if (f.endswith('.xlsx') or f.endswith('.xls')) and not f.startswith('~$')]
        elif excel_files:
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
                 month_count = len(st.session_state.df['Date_Time'].dt.to_period('M').unique())
                 st.session_state.is_single_file = (month_count <= 1 and is_single_file_upload)
            else:
                 st.session_state.is_single_file = False
            st.rerun() 
            
    # --- DB Storage Metrics Display ---
    if conn:
        db_size, total_records = get_db_metrics(SQLITE_DB_NAME, conn, target_table)
        
        st.markdown('---')
        st.markdown('##### 📊 Current DB Storage Metrics')
        
        st.caption(f"DB Location: {os.path.abspath(SQLITE_DB_NAME)}")
        
        col_s1, col_s2 = st.columns(2)
        with col_s1:
            st.metric(label=f"💾 Size of {SQLITE_DB_NAME}", value=db_size)
        with col_s2:
            st.metric(label=f"🔢 Total Records in '{target_table}'", value=f"{total_records:,}")


    # --- View and Delete Files Section ---
    st.markdown('---')
    st.markdown('##### 🔎 View & Manage Ingested Files')
    
    if st.session_state.df is not None and 'Original_File' in st.session_state.df.columns:
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
                    
                    st.session_state.data_loaded_db = False
                    st.rerun()
            else:
                st.warning("Please select at least one file to delete.")


# --- Analysis Section (Conditionally rendered) ---

df_loaded = st.session_state.df.copy()

if st.session_state.get('data_loaded_db') and not df_loaded.empty:
    
    # Set canonical columns for filtering and plotting
    date_time_col = 'Date_Time'
    
    if analysis_mode == 'Cross-Table: Volume vs. CPU':
        pass 
    elif analysis_mode == 'Standard EVT Analysis':
        event_col = 'EVENTS'
        source_col = 'SOURCE'
    else: # Standard CPU/Mem Analysis
        event_col = 'MAX_EVENTS' 
        source_col = None 
        
    st.markdown("---")
    st.subheader(f"Data Analysis: {analysis_mode}")

    # --- Cross-Table Analysis ---
    if analysis_mode == 'Cross-Table: Volume vs. CPU':
        
        # --- Data Filtering and Pre-checks ---
        try:
            # Reload both tables directly for the cross-join consistency
            evt_df = load_data_from_db(conn, EVT_TABLE_NAME)
            cpu_df = load_data_from_db(conn, CPU_MEM_TABLE_NAME)
        except Exception:
            st.error("Could not load both EVT and CPU/Mem tables. Ensure data is ingested in both.")
            st.stop()
            
        if evt_df.empty or cpu_df.empty or 'hostName' not in cpu_df.columns or 'SOURCE' not in evt_df.columns:
            st.warning("Both EVT and CPU/Mem data must be complete for cross-analysis.")
            st.stop()
            
        # Get available hostnames from CPU data
        all_hostnames = sorted(cpu_df['hostName'].dropna().unique().tolist())
        
        st.markdown("##### 1. Select Host and Time")
        
        # 1. Hostname Selection (Filter CPU data)
        col_host, col_source_map = st.columns(2)
        with col_host:
            selected_hostname = st.selectbox('Select Target Hostname (from CPU data)', all_hostnames)
        with col_source_map:
            mapped_source = HOST_TO_SOURCE.get(selected_hostname, None)
            st.info(f"Mapped EVT Source: **{mapped_source if mapped_source else 'N/A'}**")
            
        # 2. Time Filtering
        cpu_df_filtered = cpu_df[cpu_df['hostName'] == selected_hostname].copy()
        cpu_df_filtered['Month_Period'] = cpu_df_filtered[date_time_col].dt.to_period('M').astype(str)
        all_months = sorted(cpu_df_filtered['Month_Period'].unique())

        col_month, col_day = st.columns(2)
        with col_month:
            selected_month = st.selectbox('Select Month', all_months)
        
        cpu_df_filtered = cpu_df_filtered[cpu_df_filtered['Month_Period'] == selected_month]
        all_days = sorted(cpu_df_filtered[date_time_col].dt.date.unique().tolist())
        
        with col_day:
            selected_day = st.selectbox('Select Day', all_days)

        # Final filter by selected day
        cpu_df_final = cpu_df_filtered[cpu_df_filtered[date_time_col].dt.date == selected_day].copy()
        
        if cpu_df_final.empty or mapped_source is None:
            st.warning("No CPU data found for the selected day/host, or host not mapped to a SOURCE.")
            st.stop()

        # --- Join and Plot ---
        st.markdown("##### 2. Visualization")

        # 1. Filter EVT data by mapped source and time frame
        evt_df_final = evt_df[
            (evt_df['SOURCE'] == mapped_source) &
            (evt_df[date_time_col].dt.date == selected_day)
        ].copy()

        # Aggregate EVT data (Sum of EVENTS per hour)
        evt_agg = evt_df_final.groupby(date_time_col).agg(
            Total_Volume=('EVENTS', 'sum')
        ).reset_index()

        # Aggregate CPU data (Mean of CPU Total % per hour)
        cpu_agg = cpu_df_final.groupby(date_time_col).agg(
            Avg_CPU_Total=('cpu_total_pct', 'mean')
        ).reset_index()

        # Perform the JOIN on Date_Time
        merged_df = pd.merge(
            evt_agg,
            cpu_agg,
            on=date_time_col,
            how='inner' 
        )
        
        if merged_df.empty:
            st.error(f"No matching hourly data found between SOURCE '{mapped_source}' and Host '{selected_hostname}' on {selected_day}.")
            st.stop()

        # Extract hour for X-axis
        merged_df['Hour'] = merged_df[date_time_col].dt.hour
        
        st.dataframe(merged_df.sort_values('Hour'), use_container_width=True)

        # --- Dual Axis Plot ---
        fig = go.Figure()

        # Add Volume Trace (Bar, Y1)
        fig.add_trace(go.Bar(
            x=merged_df['Hour'], y=merged_df['Total_Volume'], name='Transaction Volume', yaxis='y1',
            marker_color='rgba(150, 150, 250, 0.7)'
        ))

        # Add CPU Trace (Line, Y2)
        fig.add_trace(go.Scatter(
            x=merged_df['Hour'], y=merged_df['Avg_CPU_Total'], name='Avg CPU Total %', yaxis='y2',
            mode='lines+markers', line=dict(color='red', width=3), marker=dict(size=8)
        ))

        # Configure Layout
        fig.update_layout(
            title=f"Transaction Volume vs. CPU Utilization for {selected_hostname} on {selected_day}",
            xaxis_title="Hour of Day (0-23)",
            yaxis=dict(
                title='Transaction Volume (Sum of EVENTS)', side='left', showgrid=False,
                titlefont=dict(color='rgba(150, 150, 250, 1)')
            ),
            yaxis2=dict(
                title='Average CPU Total %', overlaying='y', side='right', showgrid=True, zeroline=False,
                range=[0, merged_df['Avg_CPU_Total'].max() * 1.2 if not merged_df.empty else 100],
                titlefont=dict(color='red')
            ),
            legend=dict(x=0.01, y=1.1, orientation="h")
        )

        st.plotly_chart(fig, use_container_width=True)
        last_fig = fig 

        # --- PDF Report Generation for Cross-Table ---
        st.markdown('---')
        st.subheader("Download Professional Reports")
        
        col_pdf, col_spacer = st.columns([1, 1])

        with col_pdf:
            if st.button('📄 Generate PDF Report (Cross-Table)', type="primary", use_container_width=True):
                if last_fig is not None:
                    with st.spinner('Generating PDF report, please wait...'):
                        summary_text = f"""
**Cross-Analysis Report: {selected_hostname}**
Date: {selected_day} | Mapped Source: {mapped_source}

Transaction Metrics: 
- Total Volume (Day): {merged_df['Total_Volume'].sum():,.0f}
- Max Hourly Volume: {merged_df['Total_Volume'].max():,.0f}

CPU Metrics:
- Average CPU Total % (Day): {merged_df['Avg_CPU_Total'].mean():.2f}%
- Peak Hourly CPU %: {merged_df['Avg_CPU_Total'].max():.2f}%
"""
                        tables = {
                            "Hourly Joined Data": merged_df.set_index('Hour').drop(columns=['Date_Time']),
                        }

                        fig_path = save_plotly_figure_to_png(last_fig)
                        figures = {"Volume vs. CPU Trend": fig_path}
                        
                        pdf_filename = f"Cross_Report_{selected_hostname}_{selected_day}.pdf"
                        pdf_path = os.path.join(tempfile.gettempdir(), pdf_filename)

                        generate_pdf_report(summary_text, tables, figures, pdf_path)

                        with open(pdf_path, 'rb') as f:
                            st.download_button(
                                label="Download PDF Report", data=f.read(), file_name=pdf_filename, mime="application/pdf", key='download_pdf_cross'
                            )
                        if os.path.exists(fig_path):
                            os.remove(fig_path)
                else:
                    st.warning("Please run the visualization step first.")
        
# --- END Cross-Table Analysis ---


# --- Standard EVT Analysis (Placeholder) ---
    elif analysis_mode == 'Standard EVT Analysis':
        st.info("Standard EVT Analysis not implemented yet. Switch to another mode or refer to the full EVT analysis block.")

# --- Standard CPU/Mem Analysis (Placeholder) ---
    elif analysis_mode == 'Standard CPU/Mem Analysis':
        st.info("Standard CPU/Mem Analysis not implemented yet. Switch to another mode or refer to the full CPU/Mem analysis block.")
