import streamlit as st
import pandas as pd
import numpy as np

# ==========================================
# 1. APP CONFIGURATION
# ==========================================
st.set_page_config(page_title="Supply Chain Cockpit", layout="wide")
st.title("📊 Supply Chain Cockpit Generator")
st.markdown("Upload your **PO History** and **Planning Book** to generate the safety stock analysis.")

# ==========================================
# 2. FILE UPLOADERS
# ==========================================
col1, col2 = st.columns(2)

with col1:
    po_file = st.file_uploader("Upload POHistory.csv", type=['csv'])

with col2:
    plan_file = st.file_uploader("Upload RawPlanningBook.csv", type=['csv'])

# Only run the logic if both files are uploaded
if po_file is not None and plan_file is not None:
    
    # ==========================================
    # 3. LOAD DATA
    # ==========================================
    try:
        # Load files
        df_po = pd.read_csv(po_file, low_memory=False)
        df_plan = pd.read_csv(plan_file, low_memory=False)
        
        st.success("Files loaded successfully! Processing data...")

        # ==========================================
        # 4. PROCESS PO HISTORY (LEAD TIME)
        # ==========================================
        # Clean up Lead Time column (force numeric)
        df_po['real leadtime'] = pd.to_numeric(df_po['real leadtime'], errors='coerce')
        df_po = df_po.dropna(subset=['real leadtime'])

        # Calculate Lead Time Stats
        lt_stats = df_po.groupby(['Material', 'Plnt'])['real leadtime'].agg(
            Avg_LT='mean',
            Std_LT='std'
        ).reset_index()

        lt_stats.rename(columns={'Plnt': 'Plant', 'Material': 'Material ID'}, inplace=True)

        # ==========================================
        # 5. PROCESS PLANNING BOOK (DEMAND)
        # ==========================================
        # Sort by date and take the latest entry
        df_plan['Report_Date'] = pd.to_datetime(df_plan['Report_Date'], errors='coerce')
        current_snapshot = df_plan.sort_values('Report_Date').groupby(['Plant', 'Material ID']).tail(1).copy()

        # --- FIX: FORCE NUMERIC CONVERSION FOR DEMAND COLUMNS ---
        # This fixes the "unsupported operand type" error
        cols_to_clean = [
            'Unrestricted Stock Qty', 
            'Avg Last 12 Mths Factory Usage Qty', 
            'Std Dev Last 12 Mths Factory Usage Qty'
        ]
        
        for col in cols_to_clean:
            if col in current_snapshot.columns:
                # coerce turns text like '1,000' or '-' into valid numbers or NaN
                current_snapshot[col] = pd.to_numeric(current_snapshot[col], errors='coerce')

        # Select relevant columns
        cols_to_keep = [
            'Plant', 'Material ID', 'Material Desc', 
            'Unrestricted Stock Qty', 
            'Avg Last 12 Mths Factory Usage Qty', 
            'Std Dev Last 12 Mths Factory Usage Qty'
        ]
        
        # Ensure only existing columns are selected
        available_cols = [c for c in cols_to_keep if c in current_snapshot.columns]
        df_cockpit = current_snapshot[available_cols].copy()

        # Rename for easier code usage
        df_cockpit.rename(columns={
            'Unrestricted Stock Qty': 'Current_Stock',
            'Avg Last 12 Mths Factory Usage Qty': 'Avg_Demand',
            'Std Dev Last 12 Mths Factory Usage Qty': 'Std_Demand'
        }, inplace=True)

        # ==========================================
        # 6. MERGE & CALCULATE
        # ==========================================
        final_df = pd.merge(df_cockpit, lt_stats, on=['Plant', 'Material ID'], how='left')

        # Fill NaNs with 0 to allow calculation
        final_df[['Avg_LT', 'Std_LT', 'Avg_Demand', 'Std_Demand', 'Current_Stock']] = \
            final_df[['Avg_LT', 'Std_LT', 'Avg_Demand', 'Std_Demand', 'Current_Stock']].fillna(0)

        # Safety Stock Formula (Z=1.65 for 95%)
        # Formula: Z * sqrt( (Avg_LT * Std_Demand^2) + (Avg_Demand^2 * Std_LT^2) )
        Z_SCORE = 1.65
        
        # Calculate terms separately for debugging safety
        term1 = final_df['Avg_LT'] * (final_df['Std_Demand'] ** 2)
        term2 = (final_df['Avg_Demand'] ** 2) * (final_df['Std_LT'] ** 2)
        
        final_df['Calculated_Safety_Stock'] = Z_SCORE * np.sqrt(term1 + term2)

        # Months of Coverage Calculation
        final_df['Months_Coverage'] = final_df.apply(
            lambda x: x['Current_Stock'] / x['Avg_Demand'] if x['Avg_Demand'] > 0 else 0, axis=1
        )

        # ==========================================
        # 7. DISPLAY & DOWNLOAD
        # ==========================================
        # Format for display
        output_columns = [
            'Plant', 'Material ID', 'Material Desc', 'Current_Stock', 
            'Avg_Demand', 'Std_Demand', 'Avg_LT', 'Std_LT', 
            'Calculated_Safety_Stock', 'Months_Coverage'
        ]
        
        # Ensure only existing columns are selected
        final_output_cols = [c for c in output_columns if c in final_df.columns]
        
        # Sort by Months Coverage (Critical items first)
        cockpit_output = final_df[final_output_cols].round(2).sort_values('Months_Coverage')

        st.subheader("Results")
        st.dataframe(cockpit_output)

        # CSV Download Button
        csv = cockpit_output.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="Download Cockpit Report as CSV",
            data=csv,
            file_name='Generated_Cockpit_Report.csv',
            mime='text/csv',
        )

    except Exception as e:
        st.error(f"An error occurred: {e}")
        st.write("Hint: Check if your CSV files have the correct column names.")

else:
    st.info("Please upload both CSV files to proceed.")
