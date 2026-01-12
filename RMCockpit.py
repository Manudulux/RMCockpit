import pandas as pd
import numpy as np

# ==========================================
# 1. LOAD DATA
# ==========================================
# Replace these filenames with your actual file paths
po_history_file = 'POHistory.csv'
planning_book_file = 'RawPlanningBook.csv'

# Load PO History (handling potential encoding/separator issues common in SAP exports)
df_po = pd.read_csv(po_history_file, low_memory=False)

# Load Planning Book
df_plan = pd.read_csv(planning_book_file, low_memory=False)

# ==========================================
# 2. PROCESS PO HISTORY (LEAD TIME ANALYSIS)
# ==========================================
print("Processing Lead Times...")

# Clean up Lead Time column (force numeric, remove empty/errors)
df_po['real leadtime'] = pd.to_numeric(df_po['real leadtime'], errors='coerce')
df_po = df_po.dropna(subset=['real leadtime'])

# Group by Material and Plant to calculate Lead Time stats
# We calculate: Average Lead Time (Avg_LT) and Standard Deviation (Std_LT)
lt_stats = df_po.groupby(['Material', 'Plnt'])['real leadtime'].agg(
    Avg_LT='mean',
    Std_LT='std'
).reset_index()

# Rename columns to match join keys later
lt_stats.rename(columns={'Plnt': 'Plant', 'Material': 'Material ID'}, inplace=True)

# ==========================================
# 3. PROCESS PLANNING BOOK (DEMAND & STOCK)
# ==========================================
print("Processing Demand & Stock...")

# Ensure Report_Date is datetime to find the latest snapshot
df_plan['Report_Date'] = pd.to_datetime(df_plan['Report_Date'], errors='coerce')

# Sort by date and take the latest entry for each Plant/Material combination
# This gives us the current Stock and the pre-calculated 12-month stats
current_snapshot = df_plan.sort_values('Report_Date').groupby(['Plant', 'Material ID']).tail(1).copy()

# Select relevant columns for the cockpit
# Note: 'Avg Last 12 Mths Factory Usage Qty' = Average Sale/Demand
# Note: 'Std Dev Last 12 Mths Factory Usage Qty' = Demand Standard Deviation
cols_to_keep = [
    'Plant', 
    'Material ID', 
    'Material Desc', 
    'Unrestricted Stock Qty', 
    'Avg Last 12 Mths Factory Usage Qty', 
    'Std Dev Last 12 Mths Factory Usage Qty',
    'Report_Date'
]

df_cockpit = current_snapshot[cols_to_keep].copy()

# Rename columns for clarity in formulas
df_cockpit.rename(columns={
    'Unrestricted Stock Qty': 'Current_Stock',
    'Avg Last 12 Mths Factory Usage Qty': 'Avg_Demand',
    'Std Dev Last 12 Mths Factory Usage Qty': 'Std_Demand'
}, inplace=True)

# ==========================================
# 4. MERGE DATA SOURCES
# ==========================================
print("Merging Datasets...")

# Merge Demand/Stock data with Lead Time data
final_df = pd.merge(df_cockpit, lt_stats, on=['Plant', 'Material ID'], how='left')

# Fill NaN values for Lead Times (if no PO history exists) with 0 or a default
final_df['Avg_LT'] = final_df['Avg_LT'].fillna(0)
final_df['Std_LT'] = final_df['Std_LT'].fillna(0)
final_df['Avg_Demand'] = final_df['Avg_Demand'].fillna(0)
final_df['Std_Demand'] = final_df['Std_Demand'].fillna(0)

# ==========================================
# 5. CALCULATE SAFETY STOCK
# ==========================================
# Formula from your snippet:
# Z * sqrt( (Avg LT * (Demand Std Dev)^2) + (Avg Sale * Lead Time Std Dev)^2 )
# Note: The snippet implies the second term is squared inside or outside. 
# Standard Inventory theory usually follows: Z * sqrt( (Avg_LT * Std_Demand^2) + (Avg_Demand^2 * Std_LT^2) )

Z_SCORE = 1.65  # 95% Service Level (Adjustable)

print(f"Calculating Safety Stock (Z={Z_SCORE})...")

# Calculate components for readability
term1 = final_df['Avg_LT'] * (final_df['Std_Demand'] ** 2)
term2 = (final_df['Avg_Demand'] ** 2) * (final_df['Std_LT'] ** 2)

# Apply Formula
final_df['Calculated_Safety_Stock'] = Z_SCORE * np.sqrt(term1 + term2)

# ==========================================
# 6. SUPPLY SITUATION SUMMARY
# ==========================================
# Calculate Months of Coverage
# Avoid division by zero
final_df['Months_Coverage'] = final_df.apply(
    lambda x: x['Current_Stock'] / x['Avg_Demand'] if x['Avg_Demand'] > 0 else 0, axis=1
)

# Organize columns for final output
output_columns = [
    'Plant', 
    'Material ID', 
    'Material Desc', 
    'Current_Stock', 
    'Avg_Demand', 
    'Std_Demand', 
    'Avg_LT', 
    'Std_LT', 
    'Calculated_Safety_Stock',
    'Months_Coverage'
]

# Formatting the output
cockpit_output = final_df[output_columns].round(2)

# Sort by lowest coverage first to highlight risks
cockpit_output = cockpit_output.sort_values('Months_Coverage', ascending=True)

# ==========================================
# 7. EXPORT
# ==========================================
output_filename = 'Generated_Cockpit_Report.csv'
cockpit_output.to_csv(output_filename, index=False)

print(f"Success! Cockpit generated: {output_filename}")
print(cockpit_output.head())
