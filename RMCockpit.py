import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go

# ==========================================
# 1. APP CONFIG & STYLING
# ==========================================
st.set_page_config(page_title="Global Supply Cockpit", layout="wide")
st.title("🚀 Global Supply Chain Cockpit")

# Sidebar for Controls
st.sidebar.header("Settings & Inputs")
z_score = st.sidebar.slider("Service Level (Z-Score)", 1.0, 3.0, 1.65, help="1.65 = 95%, 2.33 = 99%")
uploaded_po = st.sidebar.file_uploader("Upload PO History", type="csv")
uploaded_plan = st.sidebar.file_uploader("Upload Planning Book", type="csv")

# ==========================================
# 2. DATA PROCESSING FUNCTIONS
# ==========================================
@st.cache_data
def process_data(po_df, plan_df, z_val):
    # --- Lead Time Logic (from PO History) ---
    po_df['real leadtime'] = pd.to_numeric(po_df['real leadtime'], errors='coerce')
    po_df = po_df.dropna(subset=['real leadtime'])
    
    lt_stats = po_df.groupby(['Material', 'Plnt'])['real leadtime'].agg(
        Avg_LT='mean', Std_LT='std'
    ).reset_index().rename(columns={'Plnt': 'Plant', 'Material': 'Material ID'})

    # --- Planning Logic ---
    plan_df['Report_Date'] = pd.to_datetime(plan_df['Report_Date'], errors='coerce')
    
    # Get the latest snapshot for current metrics
    latest_date = plan_df['Report_Date'].max()
    current = plan_df[plan_df['Report_Date'] == latest_date].copy()
    
    # Clean numeric columns
    num_cols = ['Unrestricted Stock Qty', 'Avg Last 12 Mths Factory Usage Qty', 
                'Std Dev Last 12 Mths Factory Usage Qty', 'MRP Total Forecast Qty']
    for col in num_cols:
        current[col] = pd.to_numeric(current[col], errors='coerce').fillna(0)

    # Merge
    df = pd.merge(current, lt_stats, on=['Plant', 'Material ID'], how='left').fillna(0)

    # --- Safety Stock Calculation ---
    # Formula: Z * sqrt( (Avg_LT * Std_Demand^2) + (Avg_Demand^2 * Std_LT^2) )
    t1 = df['Avg_LT'] * (df['Std Dev Last 12 Mths Factory Usage Qty'] ** 2)
    t2 = (df['Avg Last 12 Mths Factory Usage Qty'] ** 2) * (df['Std_LT'] ** 2)
    df['Safety_Stock'] = z_val * np.sqrt(t1 + t2)
    
    # Coverage logic
    df['Coverage_Mths'] = np.where(df['Avg Last 12 Mths Factory Usage Qty'] > 0, 
                                   df['Unrestricted Stock Qty'] / df['Avg Last 12 Mths Factory Usage Qty'], 0)
    
    # Status Alert
    df['Status'] = np.where(df['Unrestricted Stock Qty'] < df['Safety_Stock'], '🔴 Under SS', '🟢 Healthy')
    df['Status'] = np.where(df['Unrestricted Stock Qty'] == 0, '⚫ Out of Stock', df['Status'])
    
    return df

# ==========================================
# 3. MAIN DASHBOARD LOGIC
# ==========================================
if uploaded_po and uploaded_plan:
    df_po = pd.read_csv(uploaded_po)
    df_plan = pd.read_csv(uploaded_plan)
    
    data = process_data(df_po, df_plan, z_score)
    
    # --- TABS ---
    tab_net, tab_plant, tab_proj = st.tabs(["🌐 Network View", "🏭 Plant Analysis", "📈 5-Month Projection"])

    # ------------------------------------------
    # TAB 1: NETWORK VIEW
    # ------------------------------------------
    with tab_net:
        st.subheader("Global Inventory Health")
        
        # Top Level KPIs
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Total Stock On Hand", f"{data['Unrestricted Stock Qty'].sum():,.0f}")
        m2.metric("Avg Global Coverage", f"{data['Coverage_Mths'].mean():.2f} Months")
        m3.metric("Critical Items (Under SS)", len(data[data['Status'] == '🔴 Under SS']))
        m4.metric("Stockout Materials", len(data[data['Unrestricted Stock Qty'] == 0]))

        # Chart: Network Stock vs Safety Stock
        agg_data = data.groupby('Material Desc')[['Unrestricted Stock Qty', 'Safety_Stock']].sum().reset_index()
        fig_net = px.bar(agg_data.sort_values('Unrestricted Stock Qty', ascending=False).head(15), 
                         x='Material Desc', y=['Unrestricted Stock Qty', 'Safety_Stock'],
                         barmode='group', title="Top 15 Materials: Total Stock vs Safety Stock",
                         color_discrete_map={'Unrestricted Stock Qty': '#3366CC', 'Safety_Stock': '#DC3912'})
        st.plotly_chart(fig_net, use_container_width=True)

    # ------------------------------------------
    # TAB 2: PLANT ANALYSIS
    # ------------------------------------------
    with tab_plant:
        selected_plant = st.selectbox("Select Location", options=sorted(data['Plant'].unique()))
        p_data = data[data['Plant'] == selected_plant]
        
        col_a, col_b = st.columns([1, 2])
        
        with col_a:
            st.write(f"**Stock Status Distribution at {selected_plant}**")
            fig_pie = px.pie(p_data, names='Status', color='Status',
                             color_discrete_map={'🟢 Healthy': 'green', '🔴 Under SS': 'red', '⚫ Out of Stock': 'grey'})
            st.plotly_chart(fig_pie, use_container_width=True)
            
        with col_b:
            st.write("**Material Detail Table**")
            st.dataframe(p_data[['Material ID', 'Material Desc', 'Unrestricted Stock Qty', 'Safety_Stock', 'Coverage_Mths', 'Status']], 
                         height=400, use_container_width=True)

    # ------------------------------------------
    # TAB 3: 5-MONTH PROJECTION
    # ------------------------------------------
    with tab_proj:
        st.subheader("Inventory Projection (Stock vs Forecast)")
        target_mat = st.selectbox("Select Material for Projection", options=data['Material Desc'].unique())
        
        mat_row = data[data['Material Desc'] == target_mat].iloc[0]
        
        # Simplified Projection Logic (Stock - Forecast over 5 months)
        months = ["Month 1", "Month 2", "Month 3", "Month 4", "Month 5"]
        curr_stock = mat_row['Unrestricted Stock Qty']
        monthly_forecast = mat_row['MRP Total Forecast Qty'] / 5 # Estimating monthly if field is total
        
        projection = []
        temp_stock = curr_stock
        for m in months:
            temp_stock -= monthly_forecast
            projection.append(max(0, temp_stock))
            
        fig_proj = go.Figure()
        fig_proj.add_trace(go.Scatter(x=months, y=projection, mode='lines+markers', name='Projected Stock', line=dict(color='firebrick', width=4)))
        fig_proj.add_trace(go.Bar(x=months, y=[mat_row['Safety_Stock']]*5, name='Safety Stock Level', opacity=0.3))
        
        fig_proj.update_layout(title=f"5-Month Outlook for {target_mat}", xaxis_title="Timeline", yaxis_title="Units")
        st.plotly_chart(fig_proj, use_container_width=True)
        
        if projection[-1] < mat_row['Safety_Stock']:
            st.warning(f"⚠️ Warning: Projected stock for {target_mat} falls below Safety Stock in {months[next(i for i, v in enumerate(projection) if v < mat_row['Safety_Stock'])]}.")

else:
    st.info("👋 Welcome! Please upload the CSV files in the sidebar to begin.")
    # Add a screenshot or instructions here
