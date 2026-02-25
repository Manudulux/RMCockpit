import streamlit as st
import pandas as pd
import sqlite3
import plotly.express as px

# Set Streamlit page configuration
st.set_page_config(page_title="Inventory Dashboard", layout="wide")
st.title("Inventory & PO Reporting Dashboard")

# Connect to SQLite database (creates a local file named 'database.db')
@st.cache_resource
def get_database_connection():
    return sqlite3.connect('database.db', check_same_thread=False)

conn = get_database_connection()

# Sidebar: File Uploaders (Drag and Drop)
st.sidebar.header("Upload Data Files")
st.sidebar.markdown("Upload new versions to update the database.")

rm_file = st.sidebar.file_uploader("1. Upload RM Extract (Data by Month)", type=['csv', 'xlsx'])
po_file = st.sidebar.file_uploader("2. Upload PO History", type=['csv', 'xlsx'])

# Function to load data into SQLite and return as DataFrame
def load_data_to_db(file, table_name):
    if file is not None:
        # Read the file based on extension
        if file.name.endswith('.csv'):
            df = pd.read_csv(file)
        else:
            df = pd.read_excel(file)
            
        # Load into SQLite database
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        st.sidebar.success(f"✅ {table_name} loaded into database!")
        return df
    else:
        # Try to retrieve from database if file is not uploaded
        try:
            return pd.read_sql(f"SELECT * FROM {table_name}", conn)
        except:
            return pd.DataFrame()

# Load files into DB
rm_df = load_data_to_db(rm_file, 'rm_data')
po_df = load_data_to_db(po_file, 'po_data')

# ---------------------------------------------------------
# REPORTING SECTION
# ---------------------------------------------------------

if not rm_df.empty:
    st.header("📉 Blocked Stock Qty Evolution")
    
    # Check if necessary columns exist to avoid errors
    required_cols = ['Month/Year', 'Plant', 'Plant ID', 'Material ID', 'Material Desc', 'Material Group Desc', 'Blocked Stock Qty']
    missing_cols = [col for col in required_cols if col not in rm_df.columns]
    
    if missing_cols:
        st.error(f"The uploaded RM data is missing the following required columns: {missing_cols}")
    else:
        # Clean up date column
        rm_df['Month/Year'] = pd.to_datetime(rm_df['Month/Year'], errors='coerce')
        
        # Sidebar: Filters
        st.sidebar.header("Filter Report")
        
        # Define filters
        plants = st.sidebar.multiselect("Plant", options=sorted(rm_df['Plant'].dropna().unique()))
        plant_ids = st.sidebar.multiselect("Plant ID", options=sorted(rm_df['Plant ID'].dropna().unique()))
        materials = st.sidebar.multiselect("Material ID", options=sorted(rm_df['Material ID'].dropna().unique()))
        material_descs = st.sidebar.multiselect("Material Desc", options=sorted(rm_df['Material Desc'].dropna().unique()))
        material_groups = st.sidebar.multiselect("Material Group Desc", options=sorted(rm_df['Material Group Desc'].dropna().unique()))
        
        # Apply filters
        filtered_df = rm_df.copy()
        if plants:
            filtered_df = filtered_df[filtered_df['Plant'].isin(plants)]
        if plant_ids:
            filtered_df = filtered_df[filtered_df['Plant ID'].isin(plant_ids)]
        if materials:
            filtered_df = filtered_df[filtered_df['Material ID'].isin(materials)]
        if material_descs:
            filtered_df = filtered_df[filtered_df['Material Desc'].isin(material_descs)]
        if material_groups:
            filtered_df = filtered_df[filtered_df['Material Group Desc'].isin(material_groups)]
        
        # Aggregate data for the chart
        if not filtered_df.empty:
            agg_df = filtered_df.groupby('Month/Year', as_index=False)['Blocked Stock Qty'].sum()
            agg_df = agg_df.sort_values('Month/Year')
            
            # Plot the evolution
            fig = px.line(
                agg_df, 
                x='Month/Year', 
                y='Blocked Stock Qty', 
                markers=True,
                title="Evolution of Blocked Stock Quantity over Time",
                labels={"Month/Year": "Date", "Blocked Stock Qty": "Total Blocked Stock"}
            )
            st.plotly_chart(fig, use_container_width=True)
            
            # Display underlying filtered data
            st.subheader("Filtered Data Preview")
            st.dataframe(filtered_df[required_cols])
        else:
            st.warning("No data available for the selected filters.")
else:
    st.info("👈 Please upload the 'RM Extract' file in the sidebar to view the reporting.")

# Optional: display PO data if needed
if not po_df.empty:
    with st.expander("View Purchase Order (PO) Data"):
        st.dataframe(po_df.head(100))



