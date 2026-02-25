# streamlit_app.py
# Streamlit app: auto-load Excel from folder + drag&drop, store to SQLite, and report Blocked Stock Qty
# Hardened for cloud, tuned for speed, with optional DuckDB turbo mode.
# Author: Emmanuel + Copilot

import io
import os
import glob
import sqlite3
from datetime import datetime
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd
import streamlit as st

# ---------------------------
# Page config
# ---------------------------
st.set_page_config(page_title="Blocked Stock Report", page_icon="📦", layout="wide")

# ---------------------------
# Settings
# ---------------------------
DB_PATH = os.environ.get("APP_DB_PATH", "data.db")
UPLOADS_DIR = os.environ.get("APP_UPLOADS_DIR", "uploads")   # default folder for auto-ingest

TABLE_RM = "rm_inventory_raw"       # normalized for reporting
TABLE_PO = "po_history_raw"         # raw storage (logged), not used for the chart
TABLE_LOG = "ingestion_log"

REPORT_COLUMNS = [
    "Month/Year", "Report_Date", "Plant", "Plant ID", "Material ID",
    "Material Desc", "Material Group Desc", "Blocked Stock Qty"
]

# Optional: DuckDB turbo mode
ENABLE_DUCKDB_TURBO = st.sidebar.toggle("Turbo mode (DuckDB in-memory, Parquet snapshots)", value=False, help="Enable faster analytics (optional).")

# ---------------------------
# Extra hardening: preflight checks
# ---------------------------
# Give a clean error if openpyxl didn't install in Streamlit Cloud
try:
    import openpyxl  # noqa: F401
except Exception as e:
    st.error(
        "The **openpyxl** package is required to read `.xlsx` files but is not available.\n\n"
        "Please ensure `openpyxl` is listed in **requirements.txt**, redeploy the app, and try again.\n\n"
        f"Details: `{e}`"
    )
    st.stop()

# If DuckDB turbo is enabled, check availability
if ENABLE_DUCKDB_TURBO:
    try:
        import duckdb  # noqa: F401
        import pyarrow as pa  # noqa: F401
        import pyarrow.parquet as pq  # noqa: F401
    except Exception as e:
        st.warning(
            "Turbo mode is ON but `duckdb`/`pyarrow` are not available. "
            "Disable turbo or add these packages to requirements.txt and redeploy.\n\n"
            f"Details: `{e}`"
        )
        ENABLE_DUCKDB_TURBO = False

# ---------------------------
# DB helpers (sqlite3)
# ---------------------------
def get_conn() -> sqlite3.Connection:
    # Ensure DB file exists and tune for read perf
    conn = sqlite3.connect(
        DB_PATH,
        detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
        check_same_thread=False,
    )
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    # Negative cache_size is KB; -256000 ≈ 256MB page cache (tweak if memory is tight)
    conn.execute("PRAGMA cache_size=-256000;")
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
    # Data tables are created dynamically via pandas to_sql.

def ensure_indexes(conn: sqlite3.Connection):
    # Indexes to speed filtering and time slicing
    idx_sql = [
        f"CREATE INDEX IF NOT EXISTS idx_rm_snapshot ON {TABLE_RM}(snapshot_date);",
        f"CREATE INDEX IF NOT EXISTS idx_rm_plant ON {TABLE_RM}([Plant]);",
        f"CREATE INDEX IF NOT EXISTS idx_rm_plant_id ON {TABLE_RM}([Plant ID]);",
        f"CREATE INDEX IF NOT EXISTS idx_rm_material_id ON {TABLE_RM}([Material ID]);",
        f"CREATE INDEX IF NOT EXISTS idx_rm_material_desc ON {TABLE_RM}([Material Desc]);",
        f"CREATE INDEX IF NOT EXISTS idx_rm_mg_desc ON {TABLE_RM}([Material Group Desc]);",
        f"CREATE INDEX IF NOT EXISTS idx_rm_version ON {TABLE_RM}(version_tag);",
    ]
    for sql in idx_sql:
        try:
            conn.execute(sql)
        except Exception:
            # Some SQLite builds are picky with quoting; ignore if fails harmlessly
            pass
    conn.commit()

# ---------------------------
# Utilities
# ---------------------------
def excel_serial_to_datetime(series: pd.Series) -> pd.Series:
    """Convert Excel serial or string dates → pandas datetime.date (normalized, no time)."""
    if series is None:
        return pd.to_datetime(pd.Series([], dtype="float64"), errors="coerce")

    s = series.copy()

    # Try numeric → Excel serial
    s_num = pd.to_numeric(s, errors="coerce")
    dt = pd.to_datetime(s_num, unit="D", origin="1899-12-30", errors="coerce")

    # Fill NaT via normal parsing
    need = dt.isna()
    if need.any():
        dt2 = pd.to_datetime(s.astype(str), errors="coerce", dayfirst=False, infer_datetime_format=True)
        dt.loc[need] = dt2.loc[need]

    # Normalize to date (drop time)
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
        sample_cols = ", ".join(list(df.columns)[:15])
        raise ValueError(
            "RM Extract is missing required columns: "
            f"{missing}. First columns seen: {sample_cols}"
        )

    # Keep only what we need + a few meta cols to reduce memory
    keep = REPORT_COLUMNS + ["__sheet__"]
    keep = [c for c in keep if c in df.columns]
    df = df[keep].copy()

    # Compute snapshot_date (prefer Report_Date, else Month/Year)
    snap = excel_serial_to_datetime(df.get("Report_Date"))
    if snap.isna().all():
        snap = excel_serial_to_datetime(df.get("Month/Year"))
    df["snapshot_date"] = snap

    # Coerce numeric for Blocked Stock Qty
    df["Blocked Stock Qty"] = pd.to_numeric(df["Blocked Stock Qty"], errors="coerce").fillna(0.0)

    # Add meta
    df["source_file"] = src_name
    df["version_tag"] = version_tag
    df["uploaded_at_utc"] = datetime.utcnow().isoformat(timespec="seconds")

    # Optional: drop obvious empty rows (no snapshot & no material id)
    if "Material ID" in df.columns:
        df = df[~(df["snapshot_date"].isna() & df["Material ID"].astype(str).str.strip().eq(""))]

    # Convert common filter columns to category → less memory, slightly faster filters
    for c in ["Plant", "Plant ID", "Material ID", "Material Desc", "Material Group Desc"]:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip().astype("category")

    return df

def read_xlsx_all_sheets(uploaded_file) -> pd.DataFrame:
    """
    Read all sheets from an uploaded .xlsx file and concat them.
    Raises a descriptive error on common failure modes.
    """
    try:
        xls = pd.ExcelFile(uploaded_file)  # engine auto-detected (needs openpyxl)
    except Exception as e:
        raise RuntimeError(
            "Failed to open Excel file. Make sure the file is a valid `.xlsx` workbook. "
            f"Reader error: {e}"
        ) from e

    frames = []
    try:
        for sh in xls.sheet_names:
            # read without dtype inference across all columns (faster), then we will prune
            tmp = xls.parse(sh)
            if not tmp.empty:
                tmp["__sheet__"] = sh
                frames.append(tmp)
    except Exception as e:
        raise RuntimeError(
            f"Failed to parse at least one sheet in the workbook. Sheet list: {xls.sheet_names}. "
            f"Parser error: {e}"
        ) from e

    if not frames:
        raise RuntimeError("No non-empty sheets found in the uploaded workbook.")

    return pd.concat(frames, ignore_index=True)

def to_sql_fast(df: pd.DataFrame, table: str, conn: sqlite3.Connection):
    # Chunked multi-row insert reduces overhead significantly
    df.to_sql(table, conn, if_exists="append", index=False, chunksize=10_000, method="multi")

def load_uploaded_files(files: List[io.BytesIO], version_tag: str) -> List[str]:
    msgs = []
    conn = get_conn()
    ensure_tables(conn)

    for f in files:
        fname = getattr(f, "name", "uploaded.xlsx")
        try:
            df_all = read_xlsx_all_sheets(f)
            kind = detect_file_kind(fname, df_all)

            if kind == "RM":
                df_norm = normalize_rm_dataframe(df_all, fname, version_tag)
                to_sql_fast(df_norm, TABLE_RM, conn)
                conn.execute(
                    f"INSERT INTO {TABLE_LOG}(table_name, source_file, version_tag, uploaded_at_utc, rows_loaded) "
                    f"VALUES (?, ?, ?, ?, ?)",
                    (TABLE_RM, fname, version_tag, datetime.utcnow().isoformat(timespec="seconds"), int(df_norm.shape[0]))
                )
                conn.commit()
                msgs.append(f"✅ {fname}: loaded {df_norm.shape[0]:,} rows into '{TABLE_RM}' (version '{version_tag}').")

                # Optional: if turbo mode, mirror to Parquet per version (one file per version)
                if ENABLE_DUCKDB_TURBO:
                    os.makedirs("parquet", exist_ok=True)
                    pq_path = os.path.join("parquet", f"rm_{version_tag}.parquet")
                    df_norm.to_parquet(pq_path, index=False)

            else:
                df_po = df_all.copy()
                df_po["source_file"] = fname
                df_po["version_tag"] = version_tag
                df_po["uploaded_at_utc"] = datetime.utcnow().isoformat(timespec="seconds")
                to_sql_fast(df_po, TABLE_PO, conn)
                conn.execute(
                    f"INSERT INTO {TABLE_LOG}(table_name, source_file, version_tag, uploaded_at_utc, rows_loaded) "
                    f"VALUES (?, ?, ?, ?, ?)",
                    (TABLE_PO, fname, version_tag, datetime.utcnow().isoformat(timespec="seconds"), int(df_po.shape[0]))
                )
                conn.commit()
                msgs.append(f"✅ {fname}: loaded {df_po.shape[0]:,} rows into '{TABLE_PO}' (version '{version_tag}').")

        except Exception as e:
            msgs.append(f"❌ {fname}: ingestion failed — {e!s}")

    # After first load, ensure indexes exist
    try:
        ensure_indexes(conn)
    except Exception:
        pass

    return msgs

def scan_folder_and_ingest(folder: str, version_strategy: str = "mtime") -> List[str]:
    """
    Scan uploads folder for .xlsx and ingest. Version tag:
      - "mtime": derived from file modified time (UTC YYYY-MM-DD)
      - "now": use current UTC timestamp (YYYY-MM-DD)
    """
    if not os.path.isdir(folder):
        return [f"⚠️ Folder '{folder}' does not exist (create it or upload files via UI)."]

    files = sorted(glob.glob(os.path.join(folder, "*.xlsx")))
    if not files:
        return [f"⚠️ No .xlsx files found under '{folder}'."]
    msgs_all = []
    for path in files:
        # Build a file-like object
        with open(path, "rb") as fh:
            b = io.BytesIO(fh.read())
            b.name = os.path.basename(path)

        if version_strategy == "mtime":
            ts = datetime.utcfromtimestamp(os.path.getmtime(path))
            vtag = ts.strftime("%Y-%m-%d")
        else:
            vtag = datetime.utcnow().strftime("%Y-%m-%d")

        msgs = load_uploaded_files([b], version_tag=vtag)
        msgs_all.extend(msgs)
    return msgs_all

@st.cache_data(show_spinner=False)
def get_versions() -> List[str]:
    try:
        conn = get_conn()
        df = pd.read_sql(f"SELECT DISTINCT version_tag FROM {TABLE_LOG} ORDER BY uploaded_at_utc DESC", conn)
        return df["version_tag"].tolist()
    except Exception:
        return []

@st.cache_data(show_spinner=False)
def load_rm_for_report_sqlite(versions: Optional[List[str]] = None) -> pd.DataFrame:
    conn = get_conn()
    # Select only needed columns
    base_sql = f"""
        SELECT
            [Plant], [Plant ID], [Material ID], [Material Desc], [Material Group Desc],
            [Blocked Stock Qty], snapshot_date, version_tag
        FROM {TABLE_RM}
        WHERE snapshot_date IS NOT NULL
    """
    if versions:
        placeholders = ",".join(["?"] * len(versions))
        sql = base_sql + f" AND version_tag IN ({placeholders})"
        df = pd.read_sql(sql, conn, params=versions, parse_dates=["snapshot_date"])
    else:
        df = pd.read_sql(base_sql, conn, parse_dates=["snapshot_date"])

    # Clean strings
    for c in ["Plant", "Plant ID", "Material ID", "Material Desc", "Material Group Desc"]:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()
    return df

@st.cache_data(show_spinner=False)
def load_rm_for_report_duckdb(versions: Optional[List[str]] = None) -> pd.DataFrame:
    """
    Load via DuckDB from Parquet snapshots (one per version). Much faster for aggregations.
    """
    import duckdb
    import pyarrow as pa
    import pyarrow.parquet as pq

    if not versions:
        # If no versions selected, try all available Parquet snapshots
        paths = sorted(glob.glob("parquet/rm_*.parquet"))
    else:
        paths = [os.path.join("parquet", f"rm_{v}.parquet") for v in versions if os.path.exists(os.path.join("parquet", f"rm_{v}.parquet"))]

    if not paths:
        # Fallback to SQLite path if no parquet is available
        return load_rm_for_report_sqlite(versions)

    con = duckdb.connect()
    # Read only required columns
    query = f"""
      SELECT
        "Plant", "Plant ID", "Material ID", "Material Desc", "Material Group Desc",
        "Blocked Stock Qty", snapshot_date, version_tag
      FROM read_parquet({paths})
    """
    df = con.execute(query).df()
    # Normalize dtypes
    df["snapshot_date"] = pd.to_datetime(df["snapshot_date"])
    for c in ["Plant", "Plant ID", "Material ID", "Material Desc", "Material Group Desc"]:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()
    return df

def load_rm_for_report(versions: Optional[List[str]] = None) -> pd.DataFrame:
    if ENABLE_DUCKDB_TURBO:
        return load_rm_for_report_duckdb(versions)
    return load_rm_for_report_sqlite(versions)

def apply_filters(df: pd.DataFrame) -> pd.DataFrame:
    st.sidebar.markdown("### 🔎 Filters")

    # To keep the multiselects responsive, pre-limit options if dataset is huge (> 500k rows)
    LARGE = len(df) > 500_000

    def opts(col: str, limit: int = 1000):
        unique = df[col].dropna().astype(str).unique().tolist()
        unique.sort()
        return unique[:limit] if LARGE and len(unique) > limit else unique

    plant = st.sidebar.multiselect("Plant", opts("Plant"))
    plant_id = st.sidebar.multiselect("Plant ID", opts("Plant ID"))
    material_id = st.sidebar.multiselect("Material ID", opts("Material ID"))
    material_desc = st.sidebar.multiselect("Material Desc", opts("Material Desc"))
    mg_desc = st.sidebar.multiselect("Material Group Desc", opts("Material Group Desc"))

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

    # Use Plotly
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
st.caption("Upload Excel, auto-load from folder, store in SQLite, and explore Blocked Stock Qty over time. Turn on Turbo for faster analytics.")

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

    col_a, col_b = st.columns(2)
    with col_a:
        if st.button("Load uploaded files", type="primary", use_container_width=True, disabled=(not uploads)):
            try:
                msgs = load_uploaded_files(uploads, version_tag=version_tag)
                for m in msgs:
                    st.toast(m, icon="✅" if m.startswith("✅") else "⚠️" if m.startswith("⚠️") else "❌")
                # Clear caches so the page reflects new data immediately
                get_versions.clear()
                load_rm_for_report_sqlite.clear()
                if ENABLE_DUCKDB_TURBO:
                    load_rm_for_report_duckdb.clear()
            except Exception as e:
                st.error(f"Ingestion failed: {e}")

    with col_b:
        if st.button("Scan & load from folder", use_container_width=True):
            msgs = scan_folder_and_ingest(UPLOADS_DIR, version_strategy="mtime")
            for m in msgs:
                st.toast(m, icon="✅" if m.startswith("✅") else "⚠️")
            get_versions.clear()
            load_rm_for_report_sqlite.clear()
            if ENABLE_DUCKDB_TURBO:
                load_rm_for_report_duckdb.clear()

    st.caption(f"Default folder for auto-ingest: `{UPLOADS_DIR}/`")

# Versions available
versions = get_versions()
if not versions:
    st.info("No data yet. Upload the **RM Extract** or place files in `uploads/` and click **Scan & load from folder**.")
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
    "- Each upload is stored with your **version tag** and a **timestamp**; folder scan uses file **modified date** (UTC) by default.\n"
    "- Dates are parsed from `Report_Date` (preferred) or `Month/Year` (Excel serials supported).\n"
    "- `PO_history.xlsx` is ingested for completeness; the Blocked Stock report uses the RM dataset.\n"
    "- For large datasets, enable **Turbo mode** to query Parquet snapshots with DuckDB in-memory (very fast for aggregations)."
)



