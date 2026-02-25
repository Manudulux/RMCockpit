# streamlit_app.py
# Robust Streamlit Cloud app: drag & drop Excel, store in SQLite (sqlite3), and report Blocked Stock Qty
# Author: Emmanuel + Copilot

import io
import os
import sqlite3
from datetime import datetime
from typing import List, Optional

import numpy as np
import pandas as pd
import streamlit as st

# ---------------------------
# Page config
# ---------------------------
st.set_page_config(page_title="Blocked Stock Report", page_icon="📦", layout="wide")

DB_PATH = os.environ.get("APP_DB_PATH", "data.db")

TABLE_RM = "rm_inventory_raw"       # normalized for reporting
TABLE_PO = "po_history_raw"         # raw storage (logged), not used for the chart
TABLE_LOG = "ingestion_log"

REPORT_COLUMNS = [
    "Month/Year", "Report_Date", "Plant", "Plant ID", "Material ID",
    "Material Desc", "Material Group Desc", "Blocked Stock Qty"
]

# ---------------------------
# DB helpers (sqlite3)
# ---------------------------
def get_conn() -> sqlite3.Connection:
    # Ensure DB file exists
    conn = sqlite3.connect(DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn

def ensure_tables(conn: sqlite3.Connection):
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_LOG} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            table_name TEXT NOT NULL,
            source_file TEXT NOT NULL,
            version_tag TEXT NOT NULL,
            uploaded_at_utc TEXT NOT NULL,
            rows_loaded INTEGER NOT NULL
        );
    """)
    # Data tables are created via pandas to_sql (schema inferred).

# ---------------------------
# Utilities
# ---------------------------
def excel_serial_to_datetime(series: pd.Series) -> pd.Series:
    """Convert Excel serial or string dates → pandas datetime.date."""
    s = series.copy()

    # Try numeric first
    s_num = pd.to_numeric(s, errors="coerce")
    dt = pd.to_datetime(s_num, unit="D", origin="1899-12-30", errors="coerce")

    # Fill NaT by attempting standard parse
    need = dt.isna()
    if need.any():
        dt2 = pd.to_datetime(s.astype(str), errors="coerce")
        dt.loc[need] = dt2.loc[need]

    return pd.to_datetime(dt.dt.date, errors="coerce")

def detect_file_kind(file_name: str, df: pd.DataFrame) -> str:
    cols = set(df.columns)
    if {"Blocked Stock Qty", "Material ID", "Plant", "Plant ID"}.issubset(cols):
        return "RM"
    name_lower = file_name.lower()
    if "rm extract" in name_lower or "data by month" in name_lower:
        return "RM"
    return "PO"

def normalize_rm_dataframe(df: pd.DataFrame, src_name: str, version_tag: str) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip() for c in df.columns]

    missing = [c for c in REPORT_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"RM Extract is missing required columns: {missing}")

    # Compute snapshot_date (prefer Report_Date, else Month/Year)
    if "Report_Date" in df.columns:
        snap = excel_serial_to_datetime(df["Report_Date"])
    else:
        snap = None
    if snap is None or snap.isna().all():
        snap = excel_serial_to_datetime(df["Month/Year"])
    df["snapshot_date"] = snap

    df["Blocked Stock Qty"] = pd.to_numeric(df["Blocked Stock Qty"], errors="coerce").fillna(0.0)
    df["source_file"] = src_name
    df["version_tag"] = version_tag
    df["uploaded_at_utc"] = datetime.utcnow().isoformat(timespec="seconds")
    return df

def load_uploaded_files(files: List[io.BytesIO], version_tag: str) -> List[str]:
    msgs = []
    conn = get_conn()
    ensure_tables(conn)

    for f in files:
        fname = getattr(f, "name", "uploaded.xlsx")
        try:
            xls = pd.ExcelFile(f, engine="openpyxl")
            frames = []
            for sh in xls.sheet_names:
                tmp = xls.parse(sh)
                if not tmp.empty:
                    tmp["__sheet__"] = sh
                    frames.append(tmp)
            if not frames:
                msgs.append(f"⚠️ {fname}: no data found.")
                continue

            df_all = pd.concat(frames, ignore_index=True)
            kind = detect_file_kind(fname, df_all)

            if kind == "RM":
                df_norm = normalize_rm_dataframe(df_all, fname, version_tag)

                # append to RM table
                df_norm.to_sql(TABLE_RM, conn, if_exists="append", index=False)
                conn.execute(
                    f"INSERT INTO {TABLE_LOG}(table_name, source_file, version_tag, uploaded_at_utc, rows_loaded) VALUES (?, ?, ?, ?, ?)",
                    (TABLE_RM, fname, version_tag, datetime.utcnow().isoformat(timespec="seconds"), int(df_norm.shape[0]))
                )
                conn.commit()
                msgs.append(f"✅ {fname}: loaded {df_norm.shape[0]:,} rows into '{TABLE_RM}' (version '{version_tag}').")
            else:
                df_po = df_all.copy()
                df_po["source_file"] = fname
                df_po["version_tag"] = version_tag
                df_po["uploaded_at_utc"] = datetime.utcnow().isoformat(timespec="seconds")
                df_po.to_sql(TABLE_PO, conn, if_exists="append", index=False)
                conn.execute(
                    f"INSERT INTO {TABLE_LOG}(table_name, source_file, version_tag, uploaded_at_utc, rows_loaded) VALUES (?, ?, ?, ?, ?)",
                    (TABLE_PO, fname, version_tag, datetime.utcnow().isoformat(timespec="seconds"), int(df_po.shape[0]))
                )
                conn.commit()
                msgs.append(f"✅ {fname}: loaded {df_po.shape[0]:,} rows into '{TABLE_PO}' (version '{version_tag}').")

        except Exception as e:
            msgs.append(f"❌ {fname}: ingestion failed — {e!s}")

    return msgs

@st.cache_data(show_spinner=False)
def get_versions() -> List[str]:
    try:
        conn = get_conn()
        df = pd.read_sql(f"SELECT DISTINCT version_tag FROM {TABLE_LOG} ORDER BY uploaded_at_utc DESC", conn)
        return df["version_tag"].tolist()
    except Exception:
        return []

@st.cache_data(show_spinner=False)
def load_rm_for_report(versions: Optional[List[str]] = None) -> pd.DataFrame:
    conn = get_conn()
    base_sql = f"""
        SELECT
            [Month/Year], [Report_Date], [Plant], [Plant ID], [Material ID], [Material Desc],
            [Material Group Desc], [Blocked Stock Qty], snapshot_date, source_file, version_tag, uploaded_at_utc
        FROM {TABLE_RM}
        WHERE snapshot_date IS NOT NULL
    """
    if versions:
        placeholders = ",".join(["?"] * len(versions))
        sql = base_sql + f" AND version_tag IN ({placeholders})"
        df = pd.read_sql(sql, conn, params=versions, parse_dates=["snapshot_date"])
    else:
        df = pd.read_sql(base_sql, conn, parse_dates=["snapshot_date"])

    for c in ["Plant", "Plant ID", "Material ID", "Material Desc", "Material Group Desc"]:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()
    return df

def apply_filters(df: pd.DataFrame) -> pd.DataFrame:
    st.sidebar.markdown("### 🔎 Filters")

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

    # Date range slider
    min_d, max_d = ts["snapshot_date"].min(), ts["snapshot_date"].max()
    dr = st.slider(
        "Snapshot date range",
        min_value=min_d.to_pydatetime(),
        max_value=max_d.to_pydatetime(),
        value=(min_d.to_pydatetime(), max_d.to_pydatetime()),
        format="YYYY-MM-DD"
    )
    ts = ts[(ts["snapshot_date"] >= pd.to_datetime(dr[0])) & (ts["snapshot_date"] <= pd.to_datetime(dr[1]))]

    # Use Plotly (works well on Streamlit Cloud)
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

    with st.expander("Show aggregated data"):
        st.dataframe(ts, use_container_width=True)
        st.download_button(
            "Download aggregated CSV",
            ts.to_csv(index=False).encode("utf-8"),
            file_name="blocked_stock_evolution.csv",
            mime="text/csv",
        )

def render_cut_by_dimensions(df: pd.DataFrame):
    st.markdown("#### 🔬 Cut by dimension (top contributors)")
    dim = st.selectbox(
        "Group by",
        ["Plant", "Plant ID", "Material ID", "Material Desc", "Material Group Desc"],
        index=0
    )
    top_n = st.slider("Top N", 3, 25, 10)
    grouped = df.groupby([dim, "snapshot_date"], as_index=False)["Blocked Stock Qty"].sum()
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
        title=f"Blocked Stock Qty by {dim} (Top {top_n} @ latest snapshot)",
        labels={"snapshot_date": "Snapshot Date", "Blocked Stock Qty": "Blocked Stock Qty", dim: dim},
    )
    fig.update_layout(hovermode="x unified", height=420, legend=dict(orientation="h", y=-0.2))
    st.plotly_chart(fig, use_container_width=True)

    with st.expander("Show grouped data"):
        st.dataframe(view, use_container_width=True)

# ---------------------------
# UI
# ---------------------------
st.title("📦 Blocked Stock Reporting (RM) + Ingestion")
st.caption("Upload Excel files, store them in SQLite, and explore Blocked Stock Qty over time.")

# Sidebar: ingestion
with st.sidebar:
    st.markdown("### 📥 Ingest data")
    version_tag = st.text_input(
        "Version tag (e.g., 2026-02)",
        value=datetime.utcnow().strftime("%Y-%m-%d")
    )
    uploads = st.file_uploader(
        "Upload Excel (.xlsx)",
        type=["xlsx"],
        accept_multiple_files=True,
        help="Upload 'RM Extract - Data by Month.xlsx' (and optionally 'PO_history.xlsx')."
    )
    if st.button("Load files into database", type="primary", use_container_width=True, disabled=(not uploads)):
        try:
            msgs = load_uploaded_files(uploads, version_tag=version_tag)
            for m in msgs:
                st.toast(m, icon="✅" if m.startswith("✅") else "⚠️" if m.startswith("⚠️") else "❌")
            get_versions.clear()
            load_rm_for_report.clear()
        except Exception as e:
            st.error(f"Ingestion failed: {e}")

# Versions available
versions = get_versions()
if not versions:
    st.info("No data yet. Upload the **RM Extract** to start.")
    st.stop()

pick_versions = st.multiselect("Select version(s) for the report", options=versions, default=versions[:1])

try:
    df_rm = load_rm_for_report(pick_versions)
except Exception as e:
    st.error(f"Failed to load RM data for reporting: {e}")
    st.stop()

if df_rm.empty:
    st.warning("No RM data found for selected version(s).")
    st.stop()

# Filters
df_filtered = apply_filters(df_rm)

# KPIs
c1, c2, c3 = st.columns(3)
c1.metric("Rows (after filters)", f"{len(df_filtered):,}")
c2.metric("Snapshots", df_filtered["snapshot_date"].nunique())
c3.metric("Blocked Stock Total", f"{df_filtered['Blocked Stock Qty'].sum():,.2f}")

# Charts
render_time_series(df_filtered)
render_cut_by_dimensions(df_filtered)

with st.expander("Show filtered rows"):
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
    "- Each upload is stored with your **version tag** and a **timestamp**.\n"
    "- Dates are parsed from `Report_Date` (preferred) or `Month/Year`.\n"
    "- `PO_history.xlsx` is ingested for completeness; the chart uses the RM dataset."
)
