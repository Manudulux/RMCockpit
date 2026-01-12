import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go

# ==========================================
# 1. HELPER FUNCTIONS
# ==========================================
def clean_numeric(series):
    """Handles commas and non-numeric characters in SAP exports."""
    return pd.to_numeric(series.astype(str).str.replace(',', ''), errors='coerce').fillna(0)

@st.cache_data
def process_data(po_file, plan_file, z):
    # Load Files
    df_po = pd.read_csv(po_file, low_memory=False)
    df_plan = pd.read_csv(plan_file, low_memory=False)

    # Lead Time Stats (per Material & Plant)
    df_po['real leadtime'] = pd.to_numeric(df_po['real leadtime'], errors='coerce')
    df_po = df_po.dropna(subset=['real leadtime'])
    lt_stats = df_po.groupby(['Material', 'Plnt'])['real leadtime'].agg(
        Avg_LT='mean', Std_LT='std'
    ).reset_index().rename(columns={'Material': 'Material ID', 'Plnt': 'Plant ID'}).fillna(0)

    # Planning Data Cleaning
    num_cols = [
        'Unrestricted Stock Qty', 'Blocked Stock Qty', 'Quality Inspection Qty', 
        'Consignment Stock Qty', 'Avg Last 12 Mths Factory Usage Qty', 
        'Std Dev Last 12 Mths Factory Usage Qty'
    ]
    for col in num_cols:
        df_plan[col] = clean_numeric(df_plan[col])

    # Get latest snapshot by Date
    df_plan['Report_Date'] = pd.to_datetime(df_plan['Report_Date'], errors='coerce')
    latest = df_plan[df_plan['Report_Date'] == df_plan['Report_Date'].max()].copy()

    # Merge
    full_df = pd.merge(latest, lt_stats, on=['Material ID', 'Plant ID'], how='left').fillna(0)

    # SAFETY STOCK CALCULATION (Independent Uncertainty Formula)
    # Z * sqrt( (AvgLT * Demand_Std^2) + (AvgDemand^2 * LT_Std^2) )
    t1 = full_df['Avg_LT'] * (full_df['Std Dev Last 12 Mths Factory Usage Qty']**2)
    t2 = (full_df['Avg Last 12 Mths Factory Usage Qty']**2) * (full_df['Std_LT']**2)
    full_df['SS_Min'] = z * np.sqrt(t1 + t2)
    
    # Corridor Max = SS + 1 Month Average Demand
    full_df['SS_Max'] = full_df['SS_Min'] + full_df['Avg Last 12 Mths Factory Usage Qty']

    return full_df

# ==========================================
# 2. APP UI
# ==========================================
st.set_page_config(page_title="Network Inventory Cockpit", layout="wide")
st.title("🛡️ Inventory Corridor & Quality Cockpit")

# Sidebar
st.sidebar.header("Configuration")
z_val = st.sidebar.slider("Service Level Factor (Z)", 1.0, 3.0, 1.65, help="1.65 = 95% SL")
up_po = st.sidebar.file_uploader("Upload POHistory.csv", type=['csv'])
up_plan = st.sidebar.file_uploader("Upload RawPlanningBook.csv", type=['csv'])

if up_po and up_plan:
    data = process_data(up_po, up_plan, z_val)
    
    # Global Material Selector
    mat_list = sorted(data['Material ID'].unique())
    selected_mat = st.sidebar.selectbox("🎯 Target Material", mat_list)
    
    # Filter Data
    m_data = data[data['Material ID'] == selected_mat]
    
    # Network Aggregates
    net_unrestricted = m_data['Unrestricted Stock Qty'].sum()
    net_blocked = m_data['Blocked Stock Qty'].sum()
    net_quality = m_data['Quality Inspection Qty'].sum()
    net_ss_min = m_data['SS_Min'].sum()
    net_ss_max = m_data['SS_Max'].sum()
    net_demand = m_data['Avg Last 12 Mths Factory Usage Qty'].sum()

    # Health Status Logic
    def calc_health(stock, ss):
        if stock < ss: return "🔴 CRITICAL (Under SS)"
        if stock > (ss * 2): return "🟠 OVERSTOCK"
        return "🟢 HEALTHY"

    network_health = calc_health(net_unrestricted, net_ss_min)

    # --- TABBED VIEW ---
    tab_net, tab_plant = st.tabs(["🌍 Total Network Level", "🏭 Local Plant Breakdown"])

    with tab_net:
        st.header(f"Global Health: {network_health}")
        
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Network Unrestricted", f"{net_unrestricted:,.0f}")
        c2.metric("Network Blocked", f"{net_blocked:,.0f}", delta_color="inverse")
        c3.metric("Network Safety Min", f"{net_ss_min:,.0f}")
        c4.metric("Network Avg Demand", f"{net_demand:,.0f}")

        # INVENTORY CORRIDOR CHART
        fig_cor = go.Figure()
        fig_cor.add_trace(go.Bar(x=['Network Total'], y=[net_unrestricted], name='Current Unrestricted', marker_color='#1f77b4'))
        # Add Corridor lines
        fig_cor.add_hline(y=net_ss_min, line_dash="dash", line_color="red", annotation_text="Safety Floor (Min)")
        fig_cor.add_hline(y=net_ss_max, line_dash="dash", line_color="green", annotation_text="Target Ceiling (Max)")
        fig_cor.update_layout(title="Total Network Inventory vs. Corridor", yaxis_title="Quantity (KG)")
        st.plotly_chart(fig_cor, use_container_width=True)

        # QUALITY PIE
        st.subheader("Inventory Quality Distribution")
        qual_df = pd.DataFrame({
            'Type': ['Unrestricted', 'Blocked', 'Quality Inspection', 'Consignment'],
            'Qty': [net_unrestricted, net_blocked, net_quality, m_data['Consignment Stock Qty'].sum()]
        })
        st.plotly_chart(px.pie(qual_df, names='Type', values='Qty', hole=0.4, color_discrete_sequence=px.colors.qualitative.Safe))

    with tab_plant:
        st.header("Plant Performance Detail")
        
        # Table of Demand and Stock Quality
        plant_disp = m_data[[
            'Plant ID', 'Plant', 'Avg Last 12 Mths Factory Usage Qty', 
            'Unrestricted Stock Qty', 'Blocked Stock Qty', 'SS_Min'
        ]].copy()
        
        plant_disp['Status'] = plant_disp.apply(lambda x: calc_health(x['Unrestricted Stock Qty'], x['SS_Min']), axis=1)
        
        st.dataframe(plant_disp.style.highlight_max(axis=0, subset=['Blocked Stock Qty'], color='#ffcccc'), use_container_width=True)

        # Plant Comparison Chart
        fig_p = px.bar(m_data, x='Plant ID', y=['Unrestricted Stock Qty', 'Avg Last 12 Mths Factory Usage Qty'], 
                       barmode='group', title="Local Stock vs Local Demand")
        st.plotly_chart(fig_p, use_container_width=True)

else:
    st.info("Please upload your POHistory and RawPlanningBook CSV/Excel files to begin.")
