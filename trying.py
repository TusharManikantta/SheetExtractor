import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import numpy as np

# Page configuration
st.set_page_config(page_title="Excel Analytics Dashboard", layout="wide", page_icon="📊")

# Custom CSS
st.markdown("""
    <style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .section-header {
        font-size: 1.5rem;
        font-weight: bold;
        color: #2c3e50;
        margin-top: 2rem;
        margin-bottom: 1rem;
    }
    </style>
    """, unsafe_allow_html=True)

st.markdown('<p class="main-header">📊 Excel Analytics Dashboard</p>', unsafe_allow_html=True)

# Initialize session state
if 'uploaded_file' not in st.session_state:
    st.session_state.uploaded_file = None
if 'df' not in st.session_state:
    st.session_state.df = None

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

def load_excel_file(uploaded_file):
    """Load Excel file and return DataFrame"""
    try:
        df = pd.read_excel(uploaded_file)
        return df
    except Exception as e:
        st.error(f"Error loading file: {str(e)}")
        return None

def detect_columns(df):
    """Auto-detect relevant columns in the DataFrame"""
    columns = df.columns.tolist()
    columns_lower = [col.lower() for col in columns]
    
    source_col = next((col for col in columns if 'source' in col.lower()), columns[0])
    date_col = next((col for col in columns if 'date' in col.lower()), columns[1] if len(columns) > 1 else columns[0])
    hour_col = next((col for col in columns if 'hour' in col.lower()), None)
    events_col = next((col for col in columns if 'events' in col.lower()), columns[-1])
    
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
        df[date_column] = pd.to_datetime(df[date_column])
        date_range = (df[date_column].max() - df[date_column].min()).days
        
        if date_range >= 7:
            options.append('Weekly')
        if date_range >= 28:
            options.append('Monthly')
        if date_range >= 84:
            options.append('Quarterly')
        if date_range >= 365:
            options.append('Yearly')
    except:
        pass
    
    return options

def apply_time_grouping(df, date_column, grouping, hour_column=None):
    """Apply time-based grouping to DataFrame"""
    df_copy = df.copy()
    df_copy[date_column] = pd.to_datetime(df_copy[date_column])
    
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
        df_copy['Time_Period'] = df_copy[date_column].astype(str) + ' H' + df_copy[hour_column].astype(str)
    
    return df_copy

# File Upload Section
st.markdown('<p class="section-header">📁 Upload Excel File</p>', unsafe_allow_html=True)

uploaded_file = st.file_uploader(
    "Choose an Excel file",
    type=['xlsx', 'xls'],
    help="Upload your middleware reports or metrics Excel file"
)

if uploaded_file is not None:
    st.session_state.uploaded_file = uploaded_file
    st.session_state.df = load_excel_file(uploaded_file)
    
    if st.session_state.df is not None:
        st.success(f"✅ File loaded successfully! ({len(st.session_state.df)} rows)")
        
        # Display basic info
        with st.expander("📋 View Data Preview"):
            st.dataframe(st.session_state.df.head(20), use_container_width=True)
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Rows", len(st.session_state.df))
            with col2:
                st.metric("Total Columns", len(st.session_state.df.columns))
            with col3:
                st.metric("Data Type", "EVENTS")

# Main Analysis Section
if st.session_state.df is not None:
    df = st.session_state.df
    
    st.markdown('<p class="section-header">⚙️ Configuration</p>', unsafe_allow_html=True)
    
    # Data Type Selection
    st.info("📊 Analyzing EVENTS data")
    data_type = 'EVENTS'
    
    if data_type == 'EVENTS':
        st.markdown("### 📊 Events Analysis Configuration")
        
        # Find relevant columns
        columns = df.columns.tolist()
        
        # Try to auto-detect columns
        source_col, date_col, hour_col, events_col = detect_columns(df)
        
        col1, col2 = st.columns(2)
        
        with col1:
            source_column = st.selectbox("Source Column", columns, index=columns.index(source_col))
            date_column = st.selectbox("Date Column", columns, index=columns.index(date_col))
            
        with col2:
            hour_column = st.selectbox("Hour Column (Optional)", ['None'] + columns, 
                                      index=columns.index(hour_col) + 1 if hour_col else 0)
            events_column = st.selectbox("Events Column", columns, index=columns.index(events_col))
        
        hour_column = None if hour_column == 'None' else hour_column
        
        # Service selection
        services = df[source_column].unique().tolist()
        selected_services = st.multiselect(
            "Select Service(s)",
            services,
            default=services,
            help="Select one or more services to analyze"
        )
        
        # Time grouping
        time_grouping_options = get_time_grouping_options(df, date_column)
        if hour_column:
            time_grouping_options = ['Hourly'] + time_grouping_options
        
        time_grouping = st.selectbox("Time Grouping", time_grouping_options)
        
        # Aggregation function
        agg_function = st.selectbox("Aggregation Function", list(AGG_FUNCTIONS.keys()))
        
        # Chart Configuration
        st.markdown("### 📈 Chart Configuration")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            chart_type = st.selectbox("Chart Type", ['Bar', 'Line', 'Pie', 'Area'])
        
        # Filter data based on selection
        filtered_df = df[df[source_column].isin(selected_services)].copy()
        
        # Apply time grouping
        if time_grouping == 'Hourly' and hour_column:
            # Group by date, hour, and source
            filtered_df[date_column] = pd.to_datetime(filtered_df[date_column])
            filtered_df['Time_Period'] = filtered_df[date_column].dt.strftime('%Y-%m-%d') + ' H' + filtered_df[hour_column].astype(str)
            group_cols = ['Time_Period', source_column]
        else:
            filtered_df = apply_time_grouping(filtered_df, date_column, time_grouping, hour_column)
            group_cols = ['Time_Period', source_column]
        
        # Aggregate data
        agg_func = AGG_FUNCTIONS[agg_function]
        aggregated_df = filtered_df.groupby(group_cols)[events_column].agg(agg_func).reset_index()
        
        with col2:
            x_axis = st.selectbox("X-Axis", group_cols + [events_column])
        
        with col3:
            y_axis_options = [col for col in group_cols + [events_column] if col != x_axis]
            y_axis = st.selectbox("Y-Axis", y_axis_options)
        
        # Generate Chart
        if st.button("📊 Generate Chart", type="primary", use_container_width=True):
            st.markdown("### 📊 Visualization Results")
            
            # Display summary statistics
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Total Events", f"{aggregated_df[events_column].sum():,.0f}")
            with col2:
                st.metric("Average", f"{aggregated_df[events_column].mean():,.2f}")
            with col3:
                st.metric("Maximum", f"{aggregated_df[events_column].max():,.0f}")
            with col4:
                st.metric("Minimum", f"{aggregated_df[events_column].min():,.0f}")
            
            # Create chart based on type
            if chart_type == 'Bar':
                fig = px.bar(aggregated_df, x=x_axis, y=y_axis, color=source_column,
                            title=f"{agg_function} of {events_column} by {x_axis}",
                            barmode='group')
            elif chart_type == 'Line':
                fig = px.line(aggregated_df, x=x_axis, y=y_axis, color=source_column,
                             title=f"{agg_function} of {events_column} by {x_axis}",
                             markers=True)
            elif chart_type == 'Pie':
                pie_df = aggregated_df.groupby(source_column)[events_column].sum().reset_index()
                fig = px.pie(pie_df, values=events_column, names=source_column,
                            title=f"Distribution of {events_column} by Service")
            elif chart_type == 'Area':
                fig = px.area(aggregated_df, x=x_axis, y=y_axis, color=source_column,
                             title=f"{agg_function} of {events_column} by {x_axis}")
            
            fig.update_layout(height=600, template='plotly_white')
            st.plotly_chart(fig, use_container_width=True)
            
            # Display pivot table
            with st.expander("📋 View Pivot Table"):
                if time_grouping == 'Hourly' and hour_column:
                    pivot = create_pivot_table(filtered_df, date_column, 
                                             [source_column, hour_column], 
                                             events_column, agg_func)
                else:
                    pivot = create_pivot_table(filtered_df, 'Time_Period', 
                                             source_column, events_column, agg_func)
                
                if pivot is not None:
                    st.dataframe(pivot, use_container_width=True)
    
    elif data_type == 'INFRA-METRICS':
        st.markdown("### 🖥️ Infrastructure Metrics Configuration")
        
        columns = df.columns.tolist()
        
        # Auto-detect columns
        date_col = next((col for col in columns if 'date' in col.lower() or any(str(df[col].dtype).startswith(t) for t in ['datetime', 'object'])), columns[0])
        cpu_col = next((col for col in columns if 'cpu' in col.lower()), None)
        events_col = next((col for col in columns if 'events' in col.lower()), None)
        
        col1, col2 = st.columns(2)
        
        with col1:
            date_column = st.selectbox("Date/Row Labels Column", columns, index=columns.index(date_col))
            metric_columns = st.multiselect("Metric Columns", 
                                           [col for col in columns if col != date_column],
                                           default=[col for col in columns if col != date_column][:2])
        
        with col2:
            agg_function = st.selectbox("Aggregation Function", list(AGG_FUNCTIONS.keys()))
            time_grouping = st.selectbox("Time Grouping", 
                                        get_time_grouping_options(df, date_column))
        
        # Chart Configuration
        st.markdown("### 📈 Chart Configuration")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            chart_type = st.selectbox("Chart Type", ['Line', 'Bar', 'Area', 'Scatter'])
        
        with col2:
            x_axis = st.selectbox("X-Axis", [date_column] + metric_columns)
        
        with col3:
            y_axis_options = [col for col in metric_columns if col != x_axis]
            y_axis = st.multiselect("Y-Axis (Multiple)", y_axis_options, default=y_axis_options[:1])
        
        # Generate Chart
        if st.button("📊 Generate Chart", type="primary", use_container_width=True):
            st.markdown("### 📊 Visualization Results")
            
            # Apply time grouping if date column
            try:
                plot_df = apply_time_grouping(df, date_column, time_grouping)
                group_col = 'Time_Period'
            except:
                plot_df = df.copy()
                group_col = date_column
            
            # Aggregate data
            agg_func = AGG_FUNCTIONS[agg_function]
            agg_dict = {col: agg_func for col in metric_columns}
            aggregated_df = plot_df.groupby(group_col).agg(agg_dict).reset_index()
            
            # Display metrics
            cols = st.columns(len(metric_columns))
            for idx, col in enumerate(metric_columns):
                with cols[idx]:
                    st.metric(f"{col} ({agg_function})", f"{aggregated_df[col].mean():,.2f}")
            
            # Create chart
            if chart_type == 'Line':
                fig = go.Figure()
                for col in y_axis:
                    fig.add_trace(go.Scatter(x=aggregated_df[x_axis], y=aggregated_df[col],
                                           mode='lines+markers', name=col))
                fig.update_layout(title=f"{agg_function} of Metrics over {x_axis}")
            
            elif chart_type == 'Bar':
                fig = go.Figure()
                for col in y_axis:
                    fig.add_trace(go.Bar(x=aggregated_df[x_axis], y=aggregated_df[col], name=col))
                fig.update_layout(title=f"{agg_function} of Metrics by {x_axis}", barmode='group')
            
            elif chart_type == 'Area':
                fig = go.Figure()
                for col in y_axis:
                    fig.add_trace(go.Scatter(x=aggregated_df[x_axis], y=aggregated_df[col],
                                           fill='tonexty', name=col))
                fig.update_layout(title=f"{agg_function} of Metrics over {x_axis}")
            
            elif chart_type == 'Scatter':
                if len(y_axis) >= 1:
                    fig = px.scatter(aggregated_df, x=x_axis, y=y_axis[0],
                                   title=f"{agg_function} - {x_axis} vs {y_axis[0]}")
            
            fig.update_layout(height=600, template='plotly_white')
            st.plotly_chart(fig, use_container_width=True)
            
            # Display data table
            with st.expander("📋 View Aggregated Data"):
                st.dataframe(aggregated_df, use_container_width=True)
    
    else:  # CUSTOM
        st.markdown("### 🔧 Custom Analysis")
        st.info("Select columns and configure your custom analysis")
        
        columns = df.columns.tolist()
        
        col1, col2 = st.columns(2)
        
        with col1:
            x_column = st.selectbox("X-Axis Column", columns)
            chart_type = st.selectbox("Chart Type", ['Bar', 'Line', 'Pie', 'Scatter', 'Box'])
        
        with col2:
            y_column = st.selectbox("Y-Axis Column", [col for col in columns if col != x_column])
            agg_function = st.selectbox("Aggregation", list(AGG_FUNCTIONS.keys()))
        
        if st.button("📊 Generate Chart", type="primary", use_container_width=True):
            agg_func = AGG_FUNCTIONS[agg_function]
            
            # Aggregate data
            agg_df = df.groupby(x_column)[y_column].agg(agg_func).reset_index()
            
            # Create chart
            if chart_type == 'Bar':
                fig = px.bar(agg_df, x=x_column, y=y_column,
                           title=f"{agg_function} of {y_column} by {x_column}")
            elif chart_type == 'Line':
                fig = px.line(agg_df, x=x_column, y=y_column,
                            title=f"{agg_function} of {y_column} by {x_column}", markers=True)
            elif chart_type == 'Pie':
                fig = px.pie(agg_df, values=y_column, names=x_column,
                           title=f"Distribution of {y_column}")
            elif chart_type == 'Scatter':
                fig = px.scatter(df, x=x_column, y=y_column,
                               title=f"{x_column} vs {y_column}")
            elif chart_type == 'Box':
                fig = px.box(df, x=x_column, y=y_column,
                           title=f"Distribution of {y_column} by {x_column}")
            
            fig.update_layout(height=600, template='plotly_white')
            st.plotly_chart(fig, use_container_width=True)

else:
    st.info("👆 Please upload an Excel file to begin analysis")
    
    # Show example format
    with st.expander("📖 Expected File Format"):
        st.markdown("""
        ### EVENTS File Format:
        - **SOURCE**: Service names (IRP, MRP, RPI, TIE, RTM-GPO, etc.)
        - **TXN_DATE**: Transaction date (e.g., 6/1/2025, 7/1/2025)
        - **HOUR**: Hour of the day (0-23)
        - **EVENTS**: Event count (numeric values)
        
        ### Example:
        ```
        SOURCE    TXN_DATE    HOUR    EVENTS
        IRP       6/1/2025    0       231034
        MRP       6/1/2025    0       58005
        RPI       6/1/2025    0       125430
        ...
        ```
        """)

# Footer
st.markdown("---")
st.markdown("""
    <div style='text-align: center; color: #7f8c8d; padding: 20px;'>
        <p>📊 Excel Analytics Dashboard | Built with Streamlit</p>
    </div>
    """, unsafe_allow_html=True)