# streamlit_app.py
# Streamlit app to ingest Excel files into SQLite and report on Blocked Stock Qty evolution
# Author: Emmanuel + Copilot

import io
import os
from datetime import datetime
from typing import List, Optional

import numpy as np
import pandas as pd
import sqlalchemy as sa
import streamlit as st

# -------------------------------------
# Config
# -------------------------------------
st.set_page_config(
    page_title="Blocked Stock Report",
    page_icon="📦",
    layout="wide"
)

# SQLite DB on Streamlit Cloud is persisted between sessions (as long as the app keeps the same container)
DB_PATH = os.environ.get("APP_DB_PATH", "data.db")

# Tables
TABLE_RM = "rm_inventory_raw"       # from: "RM Extract - Data by Month.xlsx"
TABLE_PO = "po_history_raw"         # from: "PO_history.xlsx"
TABLE_LOG = "ingestion_log"

# Columns we need for the report
REPORT_COLUMNS = [
    "Month/Year", "Report_Date", "Plant", "Plant ID", "Material ID",
    "Material Desc", "Material Group Desc", "Blocked Stock Qty"
]

# -------------------------------------
# Helpers
# -------------------------------------
@st.cache_resource(show_spinner=False)
def get_engine():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True) if "/" in DB_PATH else None
    return sa.create_engine(f"sqlite:///{DB_PATH}", future=True)

def ensure_tables(engine: sa.Engine):
    with engine.begin() as con:
        con.exec_driver_sql(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_LOG} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            table_name TEXT NOT NULL,
            source_file TEXT NOT NULL,
            version_tag TEXT NOT NULL,
            uploaded_at_utc TEXT NOT NULL,
            rows_loaded INTEGER NOT NULL
        );
        """)
        # We intentionally do NOT precreate rm/po tables with fixed schema—pandas will create them on first load
        # because headers sometimes change. We do, however, consistently add meta columns below.

def excel_serial_to_datetime(series: pd.Series) -> pd.Series:
    """
    Convert Excel serial numbers to pandas datetime.
    Handles decimals (fractional days) and returns date at day precision.
    If string dates are present, falls back to to_datetime.
    """
    s = series.copy()
    # Attempt numeric conversion first
    if not np.issubdtype(s.dtype, np.number):
        # try to coerce non-numeric into numeric (for mixed columns)
        s_numeric = pd.to_numeric(s, errors="coerce")
    else:
        s_numeric = s

    dt = pd.to_datetime(s_numeric, unit="D", origin="1899-12-30", errors="coerce")
    # For entries that didn't convert (NaT), try parsing as normal datetime/string
    mask_nat = dt.isna()
    if mask_nat.any():
        dt2 = pd.to_datetime(s.astype(str), errors="coerce")
        dt.loc[mask_nat] = dt2.loc[mask_nat]
    # Return normalized to date (no time)
    return pd.to_datetime(dt.dt.date, errors="coerce")

def normalize_rm_dataframe(df: pd.DataFrame, src_name: str, version_tag: str) -> pd.DataFrame:
    """Standardize RM extract columns and add metadata."""
    # Trim whitespace in headers
    df.columns = [c.strip() for c in df.columns]

    # We only keep/report these columns (if present)
    missing = [c for c in REPORT_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(
            f"RM Extract appears to be missing columns required for reporting: {missing}"
        )

    out = df.copy()

    # Create a consistent snapshot date:
    # Prefer 'Report_Date' if present; otherwise fallback to 'Month/Year'
    snap = None
    if "Report_Date" in out.columns:
        snap = excel_serial_to_datetime(out["Report_Date"])
    if snap is None or snap.isna().all():
        snap = excel_serial_to_datetime(out["Month/Year"])

    out["snapshot_date"] = snap

    # Coerce numeric for Blocked Stock Qty
    out["Blocked Stock Qty"] = pd.to_numeric(out["Blocked Stock Qty"], errors="coerce").fillna(0.0)

    # Add metadata for versioning
    out["source_file"] = src_name
    out["version_tag"] = version_tag
    out["uploaded_at_utc"] = datetime.utcnow().isoformat(timespec="seconds")

    return out

def detect_file_kind(file_name: str, df: pd.DataFrame) -> str:
    """
    Heuristics to decide which table the file belongs to.
    - RM Extract: must contain 'Blocked Stock Qty' and 'Material ID' etc.
    - Otherwise PO history.
    """
    cols = set(df.columns)
    if {"Blocked Stock Qty", "Material ID", "Plant", "Plant ID"}.issubset(cols):
        return "RM"
    # Fallback by filename hints
    name_lower = file_name.lower()
    if "rm extract" in name_lower or "data by month" in name_lower:
        return "RM"
    return "PO"

def load_uploaded_files(files: List[io.BytesIO], version_tag: str) -> List[str]:
    """
    Load uploaded Excel files into SQLite via pandas.to_sql
    Returns status messages.
    """
    engine = get_engine()
    ensure_tables(engine)
    msgs = []

    for f in files:
        fname = getattr(f, "name", "uploaded.xlsx")
        try:
            # Read all sheets—most relevant data is usually in the first sheet named "Data"
            # but reading all increases resilience to format changes.
            xls = pd.ExcelFile(f, engine="openpyxl")
            frames = []
            for sh in xls.sheet_names:
                tmp = xls.parse(sh)
                if not tmp.empty:
                    tmp["__sheet__"] = sh
                    frames.append(tmp)
            if not frames:
                msgs.append(f"⚠️ {fname}: no data found in workbook.")
                continue
            df_all = pd.concat(frames, ignore_index=True)

            kind = detect_file_kind(fname, df_all)
            if kind == "RM":
                # Normalize for reporting; but we still store the raw as well
                df_norm = normalize_rm_dataframe(df_all, fname, version_tag)

                # Upsert/append into rm table
                with engine.begin() as con:
                    df_norm.to_sql(TABLE_RM, con, if_exists="append", index=False)
                    con.exec_driver_sql(
                        f"INSERT INTO {TABLE_LOG}(table_name, source_file, version_tag, uploaded_at_utc, rows_loaded) "
                        "VALUES (?, ?, ?, ?, ?)",
                        (TABLE_RM, fname, version_tag, datetime.utcnow().isoformat(timespec="seconds"), int(df_norm.shape[0]))
                    )
                msgs.append(f"✅ {fname}: loaded {df_norm.shape[0]:,} RM rows into '{TABLE_RM}' (version '{version_tag}').")

            else:
                # PO history—store raw with metadata
                df_po = df_all.copy()
                df_po["source_file"] = fname
                df_po["version_tag"] = version_tag
                df_po["uploaded_at_utc"] = datetime.utcnow().isoformat(timespec="seconds")
                with engine.begin() as con:
                    df_po.to_sql(TABLE_PO, con, if_exists="append", index=False)
                    con.exec_driver_sql(
                        f"INSERT INTO {TABLE_LOG}(table_name, source_file, version_tag, uploaded_at_utc, rows_loaded) "
                        "VALUES (?, ?, ?, ?, ?)",
                        (TABLE_PO, fname, version_tag, datetime.utcnow().isoformat(timespec="seconds"), int(df_po.shape[0]))
                    )
                msgs.append(f"✅ {fname}: loaded {df_po.shape[0]:,} PO rows into '{TABLE_PO}' (version '{version_tag}').")

        except Exception as e:
            msgs.append(f"❌ {fname}: ingestion failed — {e}")

    return msgs

@st.cache_data(show_spinner=False)
def get_versions(engine: sa.Engine) -> List[str]:
    try:
        return pd.read_sql(f"SELECT DISTINCT version_tag FROM {TABLE_LOG} ORDER BY uploaded_at_utc DESC", engine)["version_tag"].tolist()
    except Exception:
        return []

@st.cache_data(show_spinner=False)
def load_rm_for_report(engine: sa.Engine, versions: Optional[List[str]] = None) -> pd.DataFrame:
    base_sql = f"""
        SELECT
            [Month/Year], [Report_Date], [Plant], [Plant ID], [Material ID], [Material Desc],
            [Material Group Desc], [Blocked Stock Qty], snapshot_date, source_file, version_tag, uploaded_at_utc
        FROM {TABLE_RM}
        WHERE snapshot_date IS NOT NULL
    """
    params = {}
    if versions:
        qmarks = ",".join([f":v{i}" for i in range(len(versions))])
        base_sql += f" AND version_tag IN ({qmarks})"
        params = {f"v{i}": v for i, v in enumerate(versions)}

    df = pd.read_sql(base_sql, engine, params=params, parse_dates=["snapshot_date"])
    # Clean strings
    for c in ["Plant", "Plant ID", "Material ID", "Material Desc", "Material Group Desc"]:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()
    return df

def apply_filters(df: pd.DataFrame) -> pd.DataFrame:
    st.sidebar.markdown("### 🔎 Filters")

    # Build filters from current dataset
    plant = st.sidebar.multiselect("Plant", sorted(df["Plant"].dropna().unique().tolist()))
    plant_id = st.sidebar.multiselect("Plant ID", sorted(df["Plant ID"].dropna().unique().tolist()))
    material_id = st.sidebar.multiselect("Material ID", sorted(df["Material ID"].dropna().unique().tolist()))
    material_desc = st.sidebar.multiselect("Material Desc", sorted(df["Material Desc"].dropna().unique().tolist()))
    mg_desc = st.sidebar.multiselect("Material Group Desc", sorted(df["Material Group Desc"].dropna().unique().tolist()))

    mask = pd.Series(True, index=df.index)
    if plant:
        mask &= df["Plant"].isin(plant)
    if plant_id:
        mask &= df["Plant ID"].isin(plant_id)
    if material_id:
        mask &= df["Material ID"].isin(material_id)
    if material_desc:
        mask &= df["Material Desc"].isin(material_desc)
    if mg_desc:
        mask &= df["Material Group Desc"].isin(mg_desc)

    return df.loc[mask].copy()

def render_time_series(df: pd.DataFrame):
    st.markdown("### 📈 Blocked Stock Qty — Evolution Over Time")

    if df.empty:
        st.info("No data after filters.")
        return

    ts = (
        df.groupby("snapshot_date", as_index=False)["Blocked Stock Qty"]
          .sum()
          .sort_values("snapshot_date")
    )

    # Allow date range restriction
    min_d, max_d = ts["snapshot_date"].min(), ts["snapshot_date"].max()
    dr = st.slider(
        "Snapshot date range",
        min_value=min_d.to_pydatetime(),
        max_value=max_d.to_pydatetime(),
        value=(min_d.to_pydatetime(), max_d.to_pydatetime()),
        format="YYYY-MM-DD",
        key="date_slider"
    )
    ts = ts[(ts["snapshot_date"] >= pd.to_datetime(dr[0])) & (ts["snapshot_date"] <= pd.to_datetime(dr[1]))]

    # Plotly (nice tooltips on Streamlit)
    import plotly.express as px
    fig = px.line(
        ts,
        x="snapshot_date",
        y="Blocked Stock Qty",
        markers=True,
        title="Blocked Stock Qty Evolution",
        labels={"snapshot_date": "Snapshot Date", "Blocked Stock Qty": "Blocked Stock Qty"},
    )
    fig.update_layout(hovermode="x unified", height=420)
    st.plotly_chart(fig, use_container_width=True)

    # Detail table
    with st.expander("Show aggregated data"):
        st.dataframe(ts, use_container_width=True)
        st.download_button(
            "Download aggregated CSV",
            ts.to_csv(index=False).encode("utf-8"),
            file_name="blocked_stock_evolution.csv",
            mime="text/csv",
        )

def render_cut_by_dimensions(df: pd.DataFrame):
    st.markdown("#### 🔬 Optional: Cut by dimension (top contributors)")
    dim = st.selectbox(
        "Group by",
        ["Plant", "Plant ID", "Material ID", "Material Desc", "Material Group Desc"],
        index=0
    )
    top_n = st.slider("Top N", 3, 25, 10)
    grouped = (
        df.groupby([dim, "snapshot_date"], as_index=False)["Blocked Stock Qty"]
          .sum()
    )
    # Pick top N by latest snapshot
    latest = grouped["snapshot_date"].max()
    top_dim = (
        grouped[grouped["snapshot_date"] == latest]
        .nlargest(top_n, "Blocked Stock Qty")[dim]
        .tolist()
    )
    view = grouped[grouped[dim].isin(top_dim)]
    import plotly.express as px
    fig = px.line(
        view, x="snapshot_date", y="Blocked Stock Qty", color=dim,
        title=f"Blocked Stock Qty by {dim} (Top {top_n} at latest snapshot)",
        labels={"snapshot_date": "Snapshot Date", "Blocked Stock Qty": "Blocked Stock Qty", dim: dim},
    )
    fig.update_layout(hovermode="x unified", height=420, legend=dict(orientation="h", y=-0.2))
    st.plotly_chart(fig, use_container_width=True)

    with st.expander("Show grouped data"):
        st.dataframe(view, use_container_width=True)

# -------------------------------------
# UI
# -------------------------------------
st.title("📦 Blocked Stock Reporting (RM) + Ingestion")
st.caption("Drag-and-drop new versions of your files, store them in SQLite, and explore Blocked Stock Qty over time.")

engine = get_engine()
ensure_tables(engine)

with st.sidebar:
    st.markdown("### 📥 Ingest data")
    version_tag = st.text_input(
        "Version tag (e.g., 2026-02 monthly, or sprint name)",
        value=datetime.utcnow().strftime("%Y-%m-%d")
    )
    uploads = st.file_uploader(
        "Upload Excel files (.xlsx)",
        type=["xlsx"],
        accept_multiple_files=True,
        help="Upload any combination of 'PO_history.xlsx' and 'RM Extract - Data by Month.xlsx'. Each upload stores a new version."
    )
    if st.button("Load files into database", type="primary", use_container_width=True, disabled=(not uploads)):
        msgs = load_uploaded_files(uploads, version_tag=version_tag)
        for m in msgs:
            st.toast(m, icon="✅" if m.startswith("✅") else "⚠️" if m.startswith("⚠️") else "❌")
        # Clear caches so filters and data refresh
        get_versions.clear()
        load_rm_for_report.clear()

st.markdown("### 🗂️ Data source versions")
avail_versions = get_versions(engine)
if not avail_versions:
    st.info("No data in the database yet. Upload at least the **RM Extract** file to build the Blocked Stock report.")
    st.stop()

pick_versions = st.multiselect(
    "Select version(s) to include in the report",
    options=avail_versions,
    default=avail_versions[:1]  # default to most recent one
)

df_rm = load_rm_for_report(engine, versions=pick_versions)
if df_rm.empty:
    st.warning("No RM data found for the selected version(s).")
    st.stop()

# Filters
df_filtered = apply_filters(df_rm)

# KPIs
col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Rows (after filters)", f"{len(df_filtered):,}")
with col2:
    st.metric("Snapshots", df_filtered["snapshot_date"].nunique())
with col3:
    st.metric("Blocked Stock Total", f"{df_filtered['Blocked Stock Qty'].sum():,.2f}")

# Charts
render_time_series(df_filtered)
render_cut_by_dimensions(df_filtered)

# Raw view (optional)
with st.expander("Show filtered rows (raw)"):
    st.dataframe(df_filtered, use_container_width=True, height=400)
    st.download_button(
        "Download filtered CSV",
        df_filtered.to_csv(index=False).encode("utf-8"),
        file_name="blocked_stock_filtered.csv",
        mime="text/csv"
    )

st.markdown("---")
st.markdown(
    "ℹ️ **Notes**\n"
    "- The app stores each upload with a **version tag** and **timestamp**. You can include multiple versions in the report.\n"
    "- Dates are parsed from `Report_Date` (preferred) or `Month/Year` if needed. Both can be Excel serials.\n"
    "- `PO_history.xlsx` is ingested and logged for completeness, though the **Blocked Stock** report uses the **RM Extract** dataset."
)



