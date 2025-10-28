# --- Save this as report_utils.py ---

import pandas as pd
import plotly.io as pio
from fpdf import FPDF
import tempfile
import os
import numpy as np
import io

# Set Plotly to use the 'png' engine for image export
pio.kaleido.scope.default_format = "png"

# Save a plotly figure to a PNG file and return the file path
def save_plotly_figure_to_png(fig, filename=None):
    if filename is None:
        # Create a temporary file path
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix='.png')
        filename = tmp.name
        tmp.close()
    
    # Use kaleido engine to write the image
    try:
        fig.write_image(filename, width=900, height=500)
    except ValueError:
        # Fallback if the figure has no data (e.g., filtered to empty)
        return None
        
    return filename

# Generate a professional PDF report with summary, tables, and images
def generate_pdf_report(summary_text, tables, figures, output_path):

    class PDF(FPDF):
        def header(self):
            self.set_font('Arial', 'B', 15)
            self.cell(0, 10, 'Data Analysis Report', 0, 1, 'C')
            self.line(10, 20, 200, 20)
            self.ln(5)

        def footer(self):
            self.set_y(-15)
            self.set_font('Arial', 'I', 8)
            self.cell(0, 10, f'Page {self.page_no()}/{{nb}}', 0, 0, 'C')

    pdf = PDF()
    pdf.alias_nb_pages()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()
    
    # 1. Summary
    pdf.set_font('Arial', 'B', 14)
    pdf.cell(0, 10, '1. Summary and Key Metrics', ln=True)
    pdf.set_font('Arial', '', 11)
    # Ensure text is treated as a string for multi_cell
    pdf.multi_cell(0, 6, summary_text.replace('***', '').strip())
    pdf.ln(5)

    # 2. Tables
    for table_title, df in tables.items():
        pdf.add_page()
        pdf.set_font('Arial', 'B', 12)
        pdf.set_fill_color(245, 245, 245)
        pdf.cell(0, 12, f'2. Table: {table_title}', ln=True, align='L', fill=True)
        pdf.ln(1)

        pdf.set_font('Arial', '', 9)
        row_height = 8
        
        # Define maximum width for the whole table
        PAGE_WIDTH = 190 
        
        # Calculate proportional column widths
        num_cols = len(df.columns)
        index_width = min(40, max(25, len(str(df.index.name or '')) * 3))
        data_col_width = (PAGE_WIDTH - index_width) / num_cols
        
        # Header Row
        pdf.set_fill_color(200, 220, 255)
        pdf.set_text_color(0)
        pdf.set_font('Arial', 'B', 9)
        
        # Index Name
        pdf.cell(index_width, row_height, str(df.index.name) if df.index.name else '', border=1, align='C', fill=True)
        # Column Headers
        for col in df.columns:
            pdf.cell(data_col_width, row_height, str(col), border=1, align='C', fill=True)
        pdf.ln(row_height)

        # Data Rows
        pdf.set_font('Arial', '', 9)
        pdf.set_fill_color(255, 255, 255)
        
        for idx, row in df.iterrows():
            highlight = len(df.columns) > 1
            
            # Index Cell
            pdf.cell(index_width, row_height, str(idx), border=1, align='C')
            
            # Find max value in row for highlighting (only works for numeric columns)
            max_val = None
            max_indices = set()
            if highlight:
                try:
                    float_vals = [float(row[col]) for col in df.columns]
                    max_val = max(float_vals)
                    max_indices = {i for i, v in enumerate(float_vals) if v == max_val}
                except Exception:
                    max_val = None
            
            # Data Cells
            for i, col in enumerate(df.columns):
                cell_val = str(row[col])
                
                if highlight and max_val is not None and i in max_indices:
                    pdf.set_text_color(255, 0, 0) # Red text for highlight
                
                pdf.cell(data_col_width, row_height, cell_val, border=1, align='C')
                
                if highlight and max_val is not None and i in max_indices:
                    pdf.set_text_color(0) # Reset text color
            
            pdf.ln(row_height)
        pdf.ln(6)

    # 3. Figures
    color_palette = [
        (31, 119, 180), (255, 127, 14), (44, 160, 44), (214, 39, 40), 
        (148, 103, 189), (140, 86, 75), (227, 119, 194), (127, 127, 127)
    ]
    
    for i, (fig_title, fig_path) in enumerate(figures.items()):
        if fig_path and os.path.exists(fig_path):
            pdf.add_page()
            pdf.set_font('Arial', 'B', 12)
            pdf.cell(0, 10, f'3. Figure: {fig_title}', ln=True)

            # Add a colored border above the chart for distinction
            r, g, b = color_palette[i % len(color_palette)]
            y = pdf.get_y()
            pdf.set_fill_color(r, g, b)
            pdf.rect(x=10, y=y, w=190, h=3, style='F')
            pdf.ln(4)
            
            # Image path and width (adjust w=180 as needed)
            pdf.image(fig_path, w=180)
            pdf.ln(7)

    pdf.output(output_path, "F")
    return output_path

# Generate a professional Excel report with multiple sheets
def generate_excel_report(tables, output_path):
    # Uses the tables dictionary of DataFrames to create a multi-sheet Excel file
    with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
        for sheet_name, df in tables.items():
            # Sanitize sheet name for Excel (max 31 chars, no invalid chars)
            clean_sheet_name = sheet_name.replace(':', '_').replace('/', '_')[:31]
            df.to_excel(writer, sheet_name=clean_sheet_name)
    return output_path