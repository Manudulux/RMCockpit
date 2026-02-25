# streamlit_app.py
# Streamlit app: scan the script folder (same dir) and/or drag&drop Excel files,
# load into SQLite with progress bars, and build Blocked Stock Qty reporting.
# Author: Emmanuel + Copilot

import glob
import io
import os
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import numpy as np
import pandas as pd
import streamlit as st

# -------------------------------------------
# Page config
# -------------------------------------------
st.set_page_config(page_title="Blocked Stock Report", page_icon="📦", layout="wide")

# -------------------------------------------
# Paths / Settings
# -------------------------------------------
SCRIPT_DIR = Path(__file__).parent.resolve()                 # we scan THIS folder
DB_PATH = os.environ.get("APP_DB_PATH", str(SCRIPT_DIR / "data.db"))

TABLE_RM = "rm_inventory_raw"       # normalized RM data for reporting
TABLE_PO = "po_history_raw"         # raw PO history (stored for completeness)
TABLE_LOG = "ingestion_log"

REPORT_COLUMNS = [
    "Month/Year", "Report_Date", "Plant", "Plant ID", "Material ID",
    "Material Desc", "Material Group Desc", "Blocked Stock Qty"
]

# -------------------------------------------
# Hardening: require openpyxl for .xlsx
# -------------------------------------------
try:
    import openpyxl  # noqa: F401
except Exception as e:
    st.error(
        "The **openpyxl** package is required to read `.xlsx` files but is not available.\n\n"
        "Please ensure `openpyxl` is in **requirements.txt**, redeploy, and try again.\n\n"
        f"Details: `{e}`"
    )
    st.stop()

# -------------------------------------------
# SQLite helpers
# -------------------------------------------
def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(
        DB_PATH,
        detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
        check_same_thread=False,
    )
    # Pragmas to improve read/write performance
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    conn.execute("PRAGMA cache_size=-256000;")  # ~256MB cache (adjust if needed)
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn

def ensure_tables(conn: sqlite3.Connection):
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_LOG} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            table_name      TEXT NOT NULL,
            source_file     TEXT NOT NULL,
            version_tag     TEXT NOT NULL,
            uploaded_at_utc TEXT NOT NULL,
            rows_loaded     INTEGER NOT NULL
        );
    """)
    conn.commit()

def ensure_indexes(conn: sqlite3.Connection):
    # Indexes to speed up WHERE filters and date slicing
    idx_sql = [
        f'CREATE INDEX IF NOT EXISTS idx_rm_snapshot      ON {TABLE_RM}(snapshot_date);',
        f'CREATE INDEX IF NOT EXISTS idx_rm_plant         ON {TABLE_RM}("Plant");',
        f'CREATE INDEX IF NOT EXISTS idx_rm_plant_id      ON {TABLE_RM}("Plant ID");',
        f'CREATE INDEX IF NOT EXISTS idx_rm_material_id   ON {TABLE_RM}("Material ID");',
        f'CREATE INDEX IF NOT EXISTS idx_rm_material_desc ON {TABLE_RM}("Material Desc");',
        f'CREATE INDEX IF NOT EXISTS idx_rm_mg_desc       ON {TABLE_RM}("Material Group Desc");',
        f'CREATE INDEX IF NOT EXISTS idx_rm_version       ON {TABLE_RM}(version_tag);',
    ]
    for sql in idx_sql:
        try:
            conn.execute(sql)
        except Exception:
            pass
    conn.commit()

# -------------------------------------------
# Utilities
# -------------------------------------------
def excel_serial_to_datetime(series: pd.Series) -> pd.Series:
    """Convert Excel serial or string dates → pandas datetime.date (normalized)."""
    if series is None:
        return pd.to_datetime(pd.Series([], dtype="float64"), errors="coerce")

    s = series.copy()
    # numeric → Excel serial
    s_num = pd.to_numeric(s, errors="coerce")
    dt = pd.to_datetime(s_num, unit="D", origin="1899-12-30", errors="coerce")

    # fill NaT with normal parse
    need = dt.isna()
    if need.any():
        dt2 = pd.to_datetime(s.astype(str), errors="coerce", dayfirst=False, infer_datetime_format=True)
        dt.loc[need] = dt2.loc[need]

    return pd.to_datetime(dt.dt.date, errors="coerce")

def detect_file_kind(file_name: str, df: pd.DataFrame) -> str:
    """Simple heuristic: RM Extract contains these columns; else treat as PO."""
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
        sample = ", ".join(df.columns[:15])
        raise ValueError(
            f"RM Extract missing required columns: {missing}. "
            f"First columns seen: {sample}"
        )

    # keep only the columns we actually need
    df = df[[c for c in REPORT_COLUMNS if c in df.columns]].copy()

    # snapshot date (prefer Report_Date else Month/Year)
    snap = excel_serial_to_datetime(df.get("Report_Date"))
    if snap.isna().all():
        snap = excel_serial_to_datetime(df.get("Month/Year"))
    df["snapshot_date"] = snap

    # numeric conversion
    df["Blocked Stock Qty"] = pd.to_numeric(df["Blocked Stock Qty"], errors="coerce").fillna(0.0)

    # meta
    df["source_file"] = src_name
    df["version_tag"] = version_tag
    df["uploaded_at_utc"] = datetime.utcnow().isoformat(timespec="seconds")

    # tidy filter columns
    for c in ["Plant", "Plant ID", "Material ID", "Material Desc", "Material Group Desc"]:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()

    return df

def read_xlsx_all_sheets(uploaded_file) -> pd.DataFrame:
    """Read and concat all non-empty sheets; raise clear errors if reading fails."""
    try:
        xls = pd.ExcelFile(uploaded_file)  # engine auto-detected (needs openpyxl)
    except Exception as e:
        raise RuntimeError(
            "Failed to open Excel file. Ensure it is a valid `.xlsx` workbook. "
            f"Reader error: {e}"
        ) from e

    frames = []
    try:
        for sh in xls.sheet_names:
            tmp = xls.parse(sh)
            if not tmp.empty:
                tmp["__sheet__"] = sh
                frames.append(tmp)
    except Exception as e:
        raise RuntimeError(
            f"Failed parsing at least one sheet. Sheets: {xls.sheet_names}. Error: {e}"
        ) from e

    if not frames:
        raise RuntimeError("No non-empty sheets found in workbook.")

    return pd.concat(frames, ignore_index=True)

def to_sql_with_progress(df: pd.DataFrame, table: str, conn: sqlite3.Connection, label: str = "") -> int:
    """Chunked insert with a progress bar in the UI."""
    total = len(df)
    if total == 0:
        return 0

    chunk = 10_000
    pb = st.progress(0, text=f"Writing {label} → {table} …")
    written = 0

    for start in range(0, total, chunk):
        end = min(start + chunk, total)
        df.iloc[start:end].to_sql(table, conn, if_exists="append", index=False, method="multi")
        written = end
        pct = int(written / total * 100)
        pb.progress(pct, text=f"Writing {label} → {table} … {pct}%")

    pb.empty()
    return written

def ingest_filelike(file_like: io.BytesIO, version_tag: str) -> List[str]:
    """Ingest a single file-like object (BytesIO) and return messages."""
    msgs = []
    conn = get_conn()
    ensure_tables(conn)

    fname = getattr(file_like, "name", "uploaded.xlsx")
    try:
        st.write(f"📄 **Processing:** {fname}")
        df_all = read_xlsx_all_sheets(file_like)
        kind = detect_file_kind(fname, df_all)

        if kind == "RM":
            df_norm = normalize_rm_dataframe(df_all, fname, version_tag)
            rows = to_sql_with_progress(df_norm, TABLE_RM, conn, label=fname)
            conn.execute(
                f"INSERT INTO {TABLE_LOG}(table_name, source_file, version_tag, uploaded_at_utc, rows_loaded) "
                f"VALUES (?, ?, ?, ?, ?)",
                (TABLE_RM, fname, version_tag, datetime.utcnow().isoformat(timespec="seconds"), int(rows))
            )
            conn.commit()
            msgs.append(f"✅ {fname}: loaded {rows:,} rows into '{TABLE_RM}' (version '{version_tag}').")
        else:
            df_po = df_all.copy()
            df_po["source_file"] = fname
            df_po["version_tag"] = version_tag
            df_po["uploaded_at_utc"] = datetime.utcnow().isoformat(timespec="seconds")
            rows = to_sql_with_progress(df_po, TABLE_PO, conn, label=fname)
            conn.execute(
                f"INSERT INTO {TABLE_LOG}(table_name, source_file, version_tag, uploaded_at_utc, rows_loaded) "
                f"VALUES (?, ?, ?, ?, ?)",
                (TABLE_PO, fname, version_tag, datetime.utcnow().isoformat(timespec="seconds"), int(rows))
            )
            conn.commit()
            msgs.append(f"✅ {fname}: loaded {rows:,} rows into '{TABLE_PO}' (version '{version_tag}').")

    except Exception as e:
        msgs.append(f"❌ {fname}: ingestion failed — {e!s}")

    # Ensure indexes (safe to repeat)
    try:
        ensure_indexes(conn)
    except Exception:
        pass

    return msgs

def load_uploaded_files(files: List[io.BytesIO], version_tag: str) -> List[str]:
    """Ingest files from st.file_uploader (with overall progress)."""
    msgs_all = []
    overall = st.progress(0, text="Starting ingestion …")
    total = len(files)

    for i, f in enumerate(files, start=1):
        msgs_all.extend(ingest_filelike(f, version_tag))
        overall.progress(int(i / total * 100), text=f"Ingested {i}/{total} files")

    overall.empty()
    return msgs_all

def scan_script_folder_and_ingest() -> List[str]:
    """
    Scan THIS folder (where streamlit_app.py lives) for .xlsx files and ingest.
    Version tag = file modified date (UTC, YYYY-MM-DD).
    """
    xlsx_paths = sorted(glob.glob(str(SCRIPT_DIR / "*.xlsx")))

    # Show EXACT paths we found to avoid any ambiguity
    with st.expander(f"📂 Files detected in script folder ({SCRIPT_DIR}):", expanded=True):
        if xlsx_paths:
            for p in xlsx_paths:
                ts = datetime.utcfromtimestamp(os.path.getmtime(p)).strftime("%Y-%m-%d %H:%M:%S")
                st.write(f"- `{Path(p).name}` (mtime UTC: {ts})")
        else:
            st.write("_No `.xlsx` files found here._")

    if not xlsx_paths:
        return [f"⚠️ No .xlsx files found in script folder: `{SCRIPT_DIR}`"]

    msgs_all = []
    overall = st.progress(0, text=f"Scanning {len(xlsx_paths)} file(s) …")
    for i, path in enumerate(xlsx_paths, start=1):
        fname = os.path.basename(path)
        try:
            # Build a BytesIO so we reuse the same ingestion path as uploads
            with open(path, "rb") as fh:
                b = io.BytesIO(fh.read())
                b.name = fname

            # Version tag derived from file modified date (UTC)
            vtag = datetime.utcfromtimestamp(os.path.getmtime(path)).strftime("%Y-%m-%d")

            msgs_all.extend(ingest_filelike(b, version_tag=vtag))
        except Exception as e:
            msgs_all.append(f"❌ {fname}: ingestion failed — {e!s}")

        overall.progress(int(i / len(xlsx_paths) * 100), text=f"Processed {i}/{len(xlsx_paths)} file(s)")

    overall.empty()
    return msgs_all

# -------------------------------------------
# Data access for reporting
# -------------------------------------------
@st.cache_data(show_spinner=False)
def get_versions() -> List[str]:
    try:
        conn = get_conn()
        df = pd.read_sql(
            f"SELECT DISTINCT version_tag FROM {TABLE_LOG} ORDER BY uploaded_at_utc DESC",
            conn
        )
        return df["version_tag"].tolist()
    except Exception:
        return []

@st.cache_data(show_spinner=False)
def load_rm_for_report(versions: Optional[List[str]] = None) -> pd.DataFrame:
    conn = get_conn()
    base_sql = f"""
        SELECT
            "Plant", "Plant ID", "Material ID", "Material Desc", "Material Group Desc",
            "Blocked Stock Qty", snapshot_date, version_tag
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

# -------------------------------------------
# Filtering & Charts
# -------------------------------------------
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

# -------------------------------------------
# UI
# -------------------------------------------
st.title("📦 Blocked Stock Reporting (RM) + Ingestion")
st.caption("Scan this folder or upload files, load into SQLite (with progress), and explore Blocked Stock Qty over time.")

with st.sidebar:
    st.markdown("### 📥 Ingest data")
    version_tag = st.text_input(
        "Version tag for uploads (e.g., 2026-02)",
        value=datetime.utcnow().strftime("%Y-%m-%d"),
        help="Used for files uploaded via drag & drop. Folder scan uses each file's modified date."
    )

    uploads = st.file_uploader(
        "Upload Excel (.xlsx)",
        type=["xlsx"],
        accept_multiple_files=True,
        help="Upload 'RM Extract - Data by Month.xlsx' (and optionally 'PO_history.xlsx')."
    )

    col1, col2 = st.columns(2)
    with col1:
        if st.button("Load uploaded files", type="primary", use_container_width=True, disabled=(not uploads)):
            msgs = load_uploaded_files(uploads, version_tag=version_tag)
            for m in msgs:
                st.toast(m, icon="✅" if m.startswith("✅") else "⚠️" if m.startswith("⚠️") else "❌")
            # refresh caches
            get_versions.clear()
            load_rm_for_report.clear()

    with col2:
        if st.button("Scan & load (script folder)", use_container_width=True):
            msgs = scan_script_folder_and_ingest()
            for m in msgs:
                st.toast(m, icon="✅" if m.startswith("✅") else "⚠️")
            get_versions.clear()
            load_rm_for_report.clear()

    st.caption(f"Script folder: `{SCRIPT_DIR}`")

# Pull versions
versions = get_versions()
if not versions:
    st.info("No data yet. Upload the **RM Extract** (drag & drop) or place `.xlsx` files **next to this script** and click **Scan & load (script folder)**.")
    st.stop()

pick_versions = st.multiselect("Select version(s) for the report", options=versions, default=versions[:1])

try:
    df_rm = load_rm_for_report(pick_versions)
except Exception as e:
    st.error(f"Failed to load RM data for reporting: {e}")
    st.stop()

if df_rm.empty:
    st.warning("No RM data found for the selected version(s).")
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
    "- Folder scan shows **exactly which `.xlsx` files** are detected in the script folder.\n"
    "- For folder scan, the **version tag** is the file's **modified date (UTC)**; uploads use the textbox value.\n"
    "- The report uses **RM Extract** (columns: Plant, Plant ID, Material ID, Material Desc, Material Group Desc, Blocked Stock Qty, Month/Year or Report_Date)."
)



