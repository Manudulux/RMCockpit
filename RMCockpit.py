import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go

# ==========================================
# 1. APP CONFIG
# ==========================================
st.set_page_config(page_title="Supply Chain Cockpit", layout="wide")
st.title("📊 Supply Chain Cockpit")

# ==========================================
# 2. DATA LOADING & CLEANING HELPER
# ==========================================
def clean_numeric(series):
    """Removes commas and converts to numeric safely."""
    return pd.to_numeric(series.astype(str).str.replace(',', ''), errors='coerce').fillna(0)

@st.cache_data
def load_and_process(po_file, plan_file):
    # Load
    df_po = pd.read_csv(po_file, low_memory=False)
    df_plan = pd.read_csv(plan_file, low_memory=False)

    # 1. Process Lead Times (PO History)
    df_po['real leadtime'] = pd.to_numeric(df_po['real leadtime'], errors='coerce')
    df_po = df_po.dropna(subset=['real leadtime'])
    
    # Standardize column names for merging
    df_po = df_po.rename(columns={'Material': 'Material ID', 'Plnt': 'Plant ID'})
    
    lt_stats = df_po.groupby(['Material ID', 'Plant ID'])['real leadtime'].agg(
        Avg_LT='mean', 
        Std_LT='std'
    ).reset_index().fillna(0) # Fill std with 0 if only 1 data point

    # 2. Process Planning Book
    # Clean numeric columns immediately
    num_cols = [
        'Unrestricted Stock Qty', 
        'Avg Last 12 Mths Factory Usage Qty', 
        'Std Dev Last 12 Mths Factory Usage Qty',
        'MRP Total Forecast Qty'
    ]
    for col in num_cols:
        if col in df_plan.columns:
            df_plan[col] = clean_numeric(df_plan[col])

    # Get latest snapshot
    df_plan['Report_Date'] = pd.to_datetime(df_plan['Report_Date'], errors='coerce')
    latest_date = df_plan['Report_Date'].max()
    df_latest = df_plan[df_plan['Report_Date'] == latest_date].copy()

    # 3. Merge
    df_merged = pd.merge(df_latest, lt_stats, on=['Material ID', 'Plant ID'], how='left').fillna(0)

    return df_merged

# ==========================================
# 3. SIDEBAR CONTROLS
# ==========================================
st.sidebar.header("Data Upload")
uploaded_po = st.sidebar.file_uploader("Upload POHistory.csv", type="csv")
uploaded_plan = st.sidebar.file_uploader("Upload RawPlanningBook.csv", type="csv")

if uploaded_po and uploaded_plan:
    data = load_and_process(uploaded_po, uploaded_plan)
    
    # GLOBAL MATERIAL SELECTOR
    st.sidebar.markdown("---")
    st.sidebar.header("Global Filter")
    material_list = sorted(data['Material ID'].unique())
    selected_material = st.sidebar.selectbox("Select Material ID", material_list)
    
    z_score = st.sidebar.slider("Safety Factor (Z)", 1.0, 3.0, 1.65)

    # Filter data for the selected material
    mat_data = data[data['Material ID'] == selected_material].copy()

    # CALCULATIONS
    # Formula: Z * sqrt( (AvgLT * Demand_Std^2) + (AvgDemand^2 * LT_Std^2) )
    term1 = mat_data['Avg_LT'] * (mat_data['Std Dev Last 12 Mths Factory Usage Qty'] ** 2)
    term2 = (mat_data['Avg Last 12 Mths Factory Usage Qty'] ** 2) * (mat_data['Std_LT'] ** 2)
    mat_data['Safety_Stock'] = z_score * np.sqrt(term1 + term2)
    
    mat_data['Coverage'] = np.where(mat_data['Avg Last 12 Mths Factory Usage Qty'] > 0, 
                                   mat_data['Unrestricted Stock Qty'] / mat_data['Avg Last 12 Mths Factory Usage Qty'], 0)

    # ==========================================
    # 4. TABS
    # ==========================================
    tab1, tab2, tab3 = st.tabs(["🌐 Network (Total) View", "🏭 Local Plant View", "📅 5-Month Outlook"])

    # --- TAB 1: NETWORK VIEW ---
    with tab1:
        st.subheader(f"Total Network Summary for: {selected_material}")
        
        col1, col2, col3, col4 = st.columns(4)
        total_stock = mat_data['Unrestricted Stock Qty'].sum()
        total_ss = mat_data['Safety_Stock'].sum()
        avg_demand = mat_data['Avg Last 12 Mths Factory Usage Qty'].sum()
        
        col1.metric("Total Stock", f"{total_stock:,.0f} KG")
        col2.metric("Total Safety Stock", f"{total_ss:,.0f} KG")
        col3.metric("Avg Monthly Demand", f"{avg_demand:,.0f} KG")
        col4.metric("Net Coverage (Mths)", f"{(total_stock/avg_demand if avg_demand > 0 else 0):.2f}")

        # Graph: Stock vs Safety Stock per Plant
        fig_net = px.bar(mat_data, x='Plant ID', y=['Unrestricted Stock Qty', 'Safety_Stock'],
                         barmode='group', title="Stock vs Safety Stock by Location",
                         labels={'value': 'Quantity (KG)', 'variable': 'Metric'})
        st.plotly_chart(fig_net, use_container_width=True)

    # --- TAB 2: LOCAL PLANT VIEW ---
    with tab2:
        plant_list = sorted(mat_data['Plant ID'].unique())
        selected_plant = st.selectbox("Select Location (Plant ID)", plant_list)
        plant_row = mat_data[mat_data['Plant ID'] == selected_plant].iloc[0]

        st.write(f"### Details for {selected_plant} - {plant_row['Plant']}")
        
        c1, c2 = st.columns(2)
        with c1:
            st.info(f"**Lead Time Stats:**\n\nAvg: {plant_row['Avg_LT']:.1f} days\n\nStd Dev: {plant_row['Std_LT']:.1f} days")
        with c2:
            st.success(f"**Demand Stats:**\n\nAvg Monthly: {plant_row['Avg Last 12 Mths Factory Usage Qty']:,.0f}\n\nStd Dev: {plant_row['Std Dev Last 12 Mths Factory Usage Qty']:,.0f}")

        # Visual indicator
        ss_status = "Healthy" if plant_row['Unrestricted Stock Qty'] >= plant_row['Safety_Stock'] else "REORDER"
        st.subheader(f"Status: {ss_status}")
        
        # Simple Chart
        fig_plant = go.Figure(go.Indicator(
            mode = "gauge+number",
            value = plant_row['Unrestricted Stock Qty'],
            title = {'text': "Stock level vs Safety Stock Target"},
            gauge = {
                'axis': {'range': [0, max(plant_row['Unrestricted Stock Qty'], plant_row['Safety_Stock']) * 1.5]},
                'threshold': {
                    'line': {'color': "red", 'width': 4},
                    'thickness': 0.75,
                    'value': plant_row['Safety_Stock']}
            }
        ))
        st.plotly_chart(fig_plant)

    # --- TAB 3: PROJECTION ---
    with tab3:
        st.subheader("5-Month Inventory Forecast")
        
        # Logic: Current Stock - (Forecast * Months)
        months = ["Month 1", "Month 2", "Month 3", "Month 4", "Month 5"]
        stock_proj = []
        current = plant_row['Unrestricted Stock Qty']
        # Use MRP Forecast if available, else use Avg Demand
        monthly_burn = plant_row['MRP Total Forecast Qty'] / 5 if plant_row['MRP Total Forecast Qty'] > 0 else plant_row['Avg Last 12 Mths Factory Usage Qty']
        
        for i in range(5):
            current -= monthly_burn
            stock_proj.append(max(0, current))
        
        fig_proj = px.line(x=months, y=stock_proj, markers=True, title=f"Inventory Burn-down for {selected_plant}")
        fig_proj.add_hline(y=plant_row['Safety_Stock'], line_dash="dash", line_color="red", annotation_text="Safety Stock")
        st.plotly_chart(fig_proj, use_container_width=True)

else:
    st.info("Please upload both CSV files in the sidebar to populate the Cockpit.")
