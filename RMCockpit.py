# streamlit_app.py
# Streamlit app: Latest-only ingestion with auto-load on startup (if DB is empty),
# monthly inventory reporting from "Month/Year", optional DuckDB+Parquet fast path,
# and a PO Analysis section (Top 30 by quantity + average/std dev lead-time).
# Author: Emmanuel + Copilot

import glob
import io
import os
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd
import streamlit as st

# -------------------------------------------
# Page & layout
# -------------------------------------------
st.set_page_config(page_title="Blocked Stock Report (Monthly) – Latest Only", page_icon="📦", layout="wide")

# -------------------------------------------
# Paths / Settings
# -------------------------------------------
SCRIPT_DIR = Path(__file__).parent.resolve()  # scan THIS folder
DB_PATH = os.environ.get("APP_DB_PATH", str(SCRIPT_DIR / "data.db"))
PARQUET_DIR = SCRIPT_DIR / "parquet"
PARQUET_DIR.mkdir(exist_ok=True)

TABLE_RM = "rm_inventory_raw"       # normalized RM data (uses 'month_date')
TABLE_PO = "po_history_raw"         # raw PO history
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

# Optional optimized mode: Parquet + DuckDB (fast analytics)
opt_mode = st.sidebar.toggle(
    "⚡ Optimized mode (Parquet + DuckDB)",
    value=True,
    help="Reads the latest Parquet snapshot with DuckDB for faster aggregations. "
         "If unavailable, the app falls back to SQLite."
)
HAVE_DUCKDB = True
if opt_mode:
    try:
        import duckdb  # noqa: F401
        import pyarrow as pa  # noqa: F401
        import pyarrow.parquet as pq  # noqa: F401
    except Exception as e:
        st.warning(
            "Optimized mode is ON but `duckdb`/`pyarrow` not available; falling back to SQLite.\n\n"
            f"Details: `{e}`"
        )
        HAVE_DUCKDB = False
else:
    HAVE_DUCKDB = False

# -------------------------------------------
# SQLite helpers
# -------------------------------------------
def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(
        DB_PATH,
        detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
        check_same_thread=False,
    )
    # Pragmas for perf
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
    # Indexes to speed WHERE filters and date slicing
    idx_sql = [
        f'CREATE INDEX IF NOT EXISTS idx_rm_month        ON {TABLE_RM}(month_date);',
        f'CREATE INDEX IF NOT EXISTS idx_rm_plant        ON {TABLE_RM}("Plant");',
        f'CREATE INDEX IF NOT EXISTS idx_rm_plant_id     ON {TABLE_RM}("Plant ID");',
        f'CREATE INDEX IF NOT EXISTS idx_rm_material_id  ON {TABLE_RM}("Material ID");',
        f'CREATE INDEX IF NOT EXISTS idx_rm_material_desc ON {TABLE_RM}("Material Desc");',
        f'CREATE INDEX IF NOT EXISTS idx_rm_mg_desc      ON {TABLE_RM}("Material Group Desc");',
    ]
    for sql in idx_sql:
        try:
            conn.execute(sql)
        except Exception:
            pass
    conn.commit()

def purge_all_previous_records(conn: sqlite3.Connection):
    """Hard reset: drop tables + remove Parquet so the new schema includes month_date and only latest data remains."""
    with st.spinner("🧹 Purging previous tables and snapshots …"):
        for tbl in (TABLE_RM, TABLE_PO, TABLE_LOG):
            try:
                conn.execute(f"DROP TABLE IF EXISTS {tbl}")
            except Exception:
                pass
        conn.commit()
        # Remove Parquet snapshot(s)
        try:
            for p in PARQUET_DIR.glob("rm_*.parquet"):
                p.unlink(missing_ok=True)
            (PARQUET_DIR / "rm_latest.parquet").unlink(missing_ok=True)
        except Exception:
            pass
    st.success("Cleanup complete — only the incoming load will be kept.")

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
    """Heuristic: RM Extract contains these columns; else treat as PO."""
    cols = set(df.columns)
    if {"Blocked Stock Qty", "Material ID", "Plant", "Plant ID"}.issubset(cols):
        return "RM"
    name_lower = file_name.lower()
    if "rm extract" in name_lower or "data by month" in name_lower:
        return "RM"
    return "PO"

def normalize_rm_dataframe(df: pd.DataFrame, src_name: str, version_tag: str) -> pd.DataFrame:
    """
    Normalize RM data:
      - Parse Month/Year → month_date (first day of month)
      - Keep only needed columns
      - Numeric conversion; trim strings
      - Drop invalids and de-dupe inside batch
    """
    df = df.copy()
    df.columns = [c.strip() for c in df.columns]

    missing = [c for c in REPORT_COLUMNS if c not in df.columns]
    if missing:
        sample = ", ".join(df.columns[:15])
        raise ValueError(
            f"RM Extract missing required columns: {missing}. "
            f"First columns seen: {sample}"
        )

    # keep only needed columns
    df = df[[c for c in REPORT_COLUMNS if c in df.columns]].copy()

    # authoritative monthly axis from Month/Year → first day of month
    df["month_date"] = excel_serial_to_datetime(df["Month/Year"]).dt.to_period("M").dt.to_timestamp()

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

    # drop rows with no month_date or no material id (header bleed)
    if "Material ID" in df.columns:
        df = df[~(df["month_date"].isna() | df["Material ID"].astype(str).str.strip().eq(""))]

    # de-dupe within batch
    df = df.drop_duplicates(
        subset=["month_date", "Plant ID", "Material ID"],
        keep="last"
    )

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

def write_parquet_snapshot(df_norm: pd.DataFrame) -> str:
    """
    Write a Parquet snapshot (for DuckDB fast reads).
    Only essential columns are persisted. Overwrites previous snapshot to keep only the latest.
    """
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except Exception:
        return ""
    keep = [
        "Plant", "Plant ID", "Material ID", "Material Desc", "Material Group Desc",
        "Blocked Stock Qty", "month_date"
    ]
    dfp = df_norm[keep].copy()
    path = PARQUET_DIR / "rm_latest.parquet"
    table = pa.Table.from_pandas(dfp)
    pq.write_table(table, path)
    return str(path)

# -------------------------------------------
# Ingestion (LATEST ONLY, hard drop)
# -------------------------------------------
def ingest_batch(files: List[io.BytesIO], version_tag_for_uploads: str) -> List[str]:
    """
    Ingest a batch of files:
      - DROP TABLES + remove Parquet (latest-only; guarantees schema freshness)
      - Ingest new files (RM normalized monthly + PO raw)
      - Build a single Parquet snapshot named rm_latest.parquet (if optimized mode on)
    """
    msgs_all = []
    conn = get_conn()
    # hard drop before each batch
    purge_all_previous_records(conn)
    ensure_tables(conn)

    overall = st.progress(0, text="Starting ingestion …")
    total = len(files) if files else 0
    loaded_any_rm = False
    latest_rm_rows = 0
    latest_rm_df = None

    for i, f in enumerate(files, start=1):
        fname = getattr(f, "name", "uploaded.xlsx")
        try:
            st.write(f"📄 **Processing:** {fname}")
            df_all = read_xlsx_all_sheets(f)
            kind = detect_file_kind(fname, df_all)

            if kind == "RM":
                vtag = version_tag_for_uploads
                df_norm = normalize_rm_dataframe(df_all, fname, vtag)
                rows = to_sql_with_progress(df_norm, TABLE_RM, conn, label=fname)

                latest_rm_rows += rows
                loaded_any_rm = True
                latest_rm_df = df_norm if latest_rm_df is None else pd.concat([latest_rm_df, df_norm], ignore_index=True)

                # Log entry (for traceability; note: log table is recreated each batch)
                conn.execute(
                    f"INSERT INTO {TABLE_LOG}(table_name, source_file, version_tag, uploaded_at_utc, rows_loaded) "
                    f"VALUES (?, ?, ?, ?, ?)",
                    (TABLE_RM, fname, vtag, datetime.utcnow().isoformat(timespec="seconds"), int(rows))
                )
                conn.commit()
                msgs_all.append(f"✅ {fname}: loaded {rows:,} RM row(s).")

            else:
                vtag = version_tag_for_uploads
                df_po = df_all.copy()
                df_po["source_file"] = fname
                df_po["version_tag"] = vtag
                df_po["uploaded_at_utc"] = datetime.utcnow().isoformat(timespec="seconds")
                rows = to_sql_with_progress(df_po, TABLE_PO, conn, label=fname)
                conn.execute(
                    f"INSERT INTO {TABLE_LOG}(table_name, source_file, version_tag, uploaded_at_utc, rows_loaded) "
                    f"VALUES (?, ?, ?, ?, ?)",
                    (TABLE_PO, fname, vtag, datetime.utcnow().isoformat(timespec="seconds"), int(rows))
                )
                conn.commit()
                msgs_all.append(f"✅ {fname}: loaded {rows:,} PO row(s).")

        except Exception as e:
            msgs_all.append(f"❌ {fname}: ingestion failed — {e!s}")

        if total:
            overall.progress(int(i / total * 100), text=f"Ingested {i}/{total} files")

    overall.empty()

    # Indexes for fast reads
    try:
        ensure_indexes(conn)
    except Exception:
        pass

    # Latest-only Parquet snapshot
    if loaded_any_rm and opt_mode and HAVE_DUCKDB and latest_rm_df is not None:
        path = write_parquet_snapshot(latest_rm_df)
        if path:
            msgs_all.append(f"💾 Parquet snapshot created: {Path(path).name}")

    # Summary
    if loaded_any_rm:
        msgs_all.append(f"✅ Finished. Kept only the latest RM dataset with {latest_rm_rows:,} row(s).")
    else:
        msgs_all.append("⚠️ No RM data detected in this batch — database is empty (latest-only policy).")

    return msgs_all

def load_uploaded_files(files: List[io.BytesIO], version_tag: str) -> List[str]:
    """Wrapper for uploader ingestion (latest only)."""
    return ingest_batch(files, version_tag_for_uploads=version_tag)

def scan_script_folder_and_ingest() -> List[str]:
    """
    Scan THIS folder (where streamlit_app.py lives) for .xlsx files and ingest (latest only).
    """
    xlsx_paths = sorted(glob.glob(str(SCRIPT_DIR / "*.xlsx")))

    # Show EXACT paths we found
    with st.expander(f"📂 Files detected in script folder ({SCRIPT_DIR}):", expanded=True):
        if xlsx_paths:
            for p in xlsx_paths:
                ts = datetime.utcfromtimestamp(os.path.getmtime(p)).strftime("%Y-%m-%d %H:%M:%S")
                st.write(f"- `{Path(p).name}` (mtime UTC: {ts})")
        else:
            st.write("_No `.xlsx` files found here._")

    if not xlsx_paths:
        return [f"⚠️ No .xlsx files found in script folder: `{SCRIPT_DIR}`"]

    # Build file-like objects and ingest as one batch
    files = []
    for path in xlsx_paths:
        with open(path, "rb") as fh:
            b = io.BytesIO(fh.read())
            b.name = os.path.basename(path)
            files.append(b)

    return ingest_batch(files, version_tag_for_uploads=datetime.utcnow().strftime("%Y-%m-%d"))

# -------------------------------------------
# Data access for reporting (RM)
# -------------------------------------------
@st.cache_data(show_spinner=False)
def load_rm_for_report_sqlite() -> pd.DataFrame:
    conn = get_conn()
    # If table doesn't exist yet, return empty (friendly message shown later)
    try:
        df = pd.read_sql(
            f"""
            SELECT
                "Plant", "Plant ID", "Material ID", "Material Desc", "Material Group Desc",
                "Blocked Stock Qty", month_date
            FROM {TABLE_RM}
            WHERE month_date IS NOT NULL
            """,
            conn,
            parse_dates=["month_date"]
        )
    except Exception:
        return pd.DataFrame()

    for c in ["Plant", "Plant ID", "Material ID", "Material Desc", "Material Group Desc"]:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()
    return df

@st.cache_data(show_spinner=False)
def load_rm_for_report_duckdb() -> pd.DataFrame:
    """Load latest-only snapshot via DuckDB Parquet; fallback to SQLite if missing."""
    if not (opt_mode and HAVE_DUCKDB):
        return load_rm_for_report_sqlite()

    path = PARQUET_DIR / "rm_latest.parquet"
    if not path.exists():
        return load_rm_for_report_sqlite()

    import duckdb
    con = duckdb.connect()
    df = con.execute(f"""
        SELECT
          "Plant", "Plant ID", "Material ID", "Material Desc", "Material Group Desc",
          "Blocked Stock Qty", CAST(month_date AS TIMESTAMP) AS month_date
        FROM read_parquet('{str(path)}')
    """).df()
    df["month_date"] = pd.to_datetime(df["month_date"])
    for c in ["Plant", "Plant ID", "Material ID", "Material Desc", "Material Group Desc"]:
        df[c] = df[c].astype(str).str.strip()
    return df

def load_rm_for_report() -> pd.DataFrame:
    return load_rm_for_report_duckdb() if (opt_mode and HAVE_DUCKDB) else load_rm_for_report_sqlite()

# -------------------------------------------
# PO Analysis helpers
# -------------------------------------------
def get_table_columns(conn: sqlite3.Connection, table: str) -> List[str]:
    try:
        cur = conn.execute(f'PRAGMA table_info("{table}")')
        cols = [r[1] for r in cur.fetchall()]
        return cols
    except Exception:
        return []

def _find_col(cols: Sequence[str], candidates: Sequence[str]) -> Optional[str]:
    cols_lc = {c.lower(): c for c in cols}
    for cand in candidates:
        key = cand.lower()
        if key in cols_lc:
            return cols_lc[key]
    # try relaxed match (remove spaces / underscores)
    relaxed = {c.lower().replace(" ", "").replace("_", ""): c for c in cols}
    for cand in candidates:
        key = cand.lower().replace(" ", "").replace("_", "")
        if key in relaxed:
            return relaxed[key]
    return None

def load_po_material_choices() -> Tuple[List[str], dict]:
    """Return a sorted list of material IDs and a mapping of canonical names actually present."""
    conn = get_conn()
    cols = get_table_columns(conn, TABLE_PO)
    if not cols:
        return [], {}

    material_col = _find_col(cols, ["Material ID", "Material"])
    if material_col is None:
        return [], {}

    # fetch distinct materials (limit to keep UI snappy if huge)
    df = pd.read_sql(f'SELECT DISTINCT "{material_col}" AS material FROM {TABLE_PO} WHERE "{material_col}" IS NOT NULL LIMIT 20000', conn)
    materials = sorted(df["material"].astype(str).str.strip().unique().tolist())

    # build a mapping of useful columns we may later extract
    mapping = {
        "material": material_col,
        "po_number": _find_col(cols, ["PO Number", "PurDoc", "PO", "EBELN"]),
        "creation_date": _find_col(cols, ["PO Creation Date", "Created On", "Doc. Date"]),
        "gr_date": _find_col(cols, ["Goods Receipt Date", "Actual GR Date", "GR Date"]),
        "qty": _find_col(cols, ["PO qty", "PO Qty", "Order Qty", "Quantity"]),
        "vendor": _find_col(cols, ["Vendor", "Supplier"]),
        "plant": _find_col(cols, ["Plant", "Plnt", "Plant ID"]),
        "short_text": _find_col(cols, ["Short Text", "Material Desc", "Description"]),
        "currency": _find_col(cols, ["Crcy", "Currency"]),
        "net_price": _find_col(cols, ["Net Price", "Price"]),
    }
    return materials, mapping

def _parse_date_series(s: pd.Series) -> pd.Series:
    # handle either Excel serials or text dates
    return excel_serial_to_datetime(s)

def load_po_for_material(material_id: str, mapping: dict) -> pd.DataFrame:
    """Load PO rows for a given material with essential columns; compute lead-time."""
    conn = get_conn()
    mcol = mapping.get("material")
    if not mcol:
        return pd.DataFrame()

    # Build the SELECT only with available columns
    select_cols = [c for c in [
        mapping.get("po_number") or "rowid",
        mcol,
        mapping.get("creation_date"),
        mapping.get("gr_date"),
        mapping.get("qty"),
        mapping.get("vendor"),
        mapping.get("plant"),
        mapping.get("short_text"),
        mapping.get("currency"),
        mapping.get("net_price"),
    ] if c is not None]

    select_sql_cols = ", ".join([f'"{c}"' if c != "rowid" else "rowid" for c in select_cols])
    sql = f'SELECT {select_sql_cols} FROM {TABLE_PO} WHERE "{mcol}" = ?'
    df = pd.read_sql(sql, conn, params=[material_id])

    # Rename to canonical names where possible for UI clarity
    ren = {}
    if mapping.get("po_number"): ren[mapping["po_number"]] = "PO Number"
    ren[mcol] = "Material ID"
    if mapping.get("creation_date"): ren[mapping["creation_date"]] = "PO Creation Date"
    if mapping.get("gr_date"): ren[mapping["gr_date"]] = "Goods Receipt Date"
    if mapping.get("qty"): ren[mapping["qty"]] = "PO Qty"
    if mapping.get("vendor"): ren[mapping["vendor"]] = "Vendor"
    if mapping.get("plant"): ren[mapping["plant"]] = "Plant"
    if mapping.get("short_text"): ren[mapping["short_text"]] = "Short Text"
    if mapping.get("currency"): ren[mapping["currency"]] = "Currency"
    if mapping.get("net_price"): ren[mapping["net_price"]] = "Net Price"
    df = df.rename(columns=ren)

    # Parse dates & compute lead-time (days)
    if "PO Creation Date" in df.columns:
        df["PO Creation Date"] = _parse_date_series(df["PO Creation Date"])
    else:
        df["PO Creation Date"] = pd.NaT

    if "Goods Receipt Date" in df.columns:
        df["Goods Receipt Date"] = _parse_date_series(df["Goods Receipt Date"])
    else:
        df["Goods Receipt Date"] = pd.NaT

    df["lead_time_days"] = (df["Goods Receipt Date"] - df["PO Creation Date"]).dt.days

    # Qty numeric
    if "PO Qty" in df.columns:
        df["PO Qty"] = pd.to_numeric(df["PO Qty"], errors="coerce")
    else:
        df["PO Qty"] = np.nan

    # Clean up PO Number if missing
    if "PO Number" not in df.columns:
        df["PO Number"] = df.index.astype(str)

    # Drop rows without dates or qty for the lead-time analysis
    df = df.dropna(subset=["PO Creation Date", "Goods Receipt Date", "lead_time_days", "PO Qty"])
    return df

# -------------------------------------------
# Filtering & Charts (RM)
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

def extend_to_current_month(ts: pd.DataFrame, value_col: str) -> pd.DataFrame:
    """
    Reindex monthly time series to run through the current month and forward-fill values (global series).
    """
    if ts.empty:
        return ts
    cur_month = pd.Timestamp.today().to_period("M").to_timestamp()
    ts = ts.sort_values("month_date")
    start = ts["month_date"].min().to_period("M").to_timestamp()
    idx = pd.date_range(start, cur_month, freq="MS")
    g = ts.set_index("month_date").reindex(idx).rename_axis("month_date").reset_index()
    g[value_col] = g[value_col].ffill().fillna(0.0)
    return g

def render_time_series(df: pd.DataFrame, extend_series: bool):
    st.markdown("### 📈 Blocked Stock Qty — Monthly Inventory Evolution")

    if df.empty:
        st.info("No data after filters.")
        return

    ts = (
        df.groupby("month_date", as_index=False)["Blocked Stock Qty"]
          .sum()
          .sort_values("month_date")
    )

    if extend_series:
        ts = extend_to_current_month(ts, value_col="Blocked Stock Qty")

    unique_dates = ts["month_date"].dropna().unique()
    if len(unique_dates) <= 1:
        st.caption("Not enough distinct months to build a range slider.")
        import plotly.express as px
        fig = px.line(
            ts,
            x="month_date",
            y="Blocked Stock Qty",
            markers=True,
            title="Blocked Stock Qty Evolution (single month)",
        )
        st.plotly_chart(fig, use_container_width=True)
        with st.expander("Show monthly data"):
            st.dataframe(ts, use_container_width=True)
        return

    min_d, max_d = ts["month_date"].min(), ts["month_date"].max()
    min_dt = pd.to_datetime(min_d).to_pydatetime()
    max_dt = pd.to_datetime(max_d).to_pydatetime()

    dr = st.slider(
        "Select month range",
        min_value=min_dt,
        max_value=max_dt,
        value=(min_dt, max_dt),
        format="YYYY-MM"
    )
    ts = ts[(ts["month_date"] >= pd.to_datetime(dr[0])) &
            (ts["month_date"] <= pd.to_datetime(dr[1]))]

    import plotly.express as px
    fig = px.line(
        ts,
        x="month_date",
        y="Blocked Stock Qty",
        markers=True,
        title="Blocked Stock Qty – Monthly Evolution",
        labels={"month_date": "Month", "Blocked Stock Qty": "Blocked Stock Qty"},
    )
    fig.update_layout(hovermode="x unified", height=420)
    st.plotly_chart(fig, use_container_width=True)

    with st.expander("Show monthly data"):
        st.dataframe(ts, use_container_width=True)
        st.download_button(
            "Download aggregated CSV",
            ts.to_csv(index=False).encode("utf-8"),
            file_name="blocked_stock_monthly_evolution.csv",
            mime="text/csv",
        )

def render_cut_by_dimensions(df: pd.DataFrame, extend_series: bool):
    st.markdown("#### 🔬 Cut by dimension (top contributors)")
    dim = st.selectbox(
        "Group by",
        ["Plant", "Plant ID", "Material ID", "Material Desc", "Material Group Desc"],
        index=0
    )
    top_n = st.slider("Top N", 3, 25, 10)

    grouped = df.groupby([dim, "month_date"], as_index=False)["Blocked Stock Qty"].sum()

    if extend_series:
        # forward fill per dimension through current month
        cur_month = pd.Timestamp.today().to_period("M").to_timestamp()
        out_frames = []
        for val, g in grouped.groupby(dim, dropna=False):
            g = g.sort_values("month_date")
            start = g["month_date"].min().to_period("M").to_timestamp()
            idx = pd.date_range(start, cur_month, freq="MS")
            gg = g.set_index("month_date").reindex(idx).rename_axis("month_date").reset_index()
            gg[dim] = val
            gg["Blocked Stock Qty"] = gg["Blocked Stock Qty"].ffill().fillna(0.0)
            out_frames.append(gg)
        grouped = pd.concat(out_frames, ignore_index=True)

    latest = grouped["month_date"].max()
    top_dim = (
        grouped[grouped["month_date"] == latest]
        .nlargest(top_n, "Blocked Stock Qty")[dim]
        .tolist()
    )
    view = grouped[grouped[dim].isin(top_dim)].sort_values(["month_date", dim])

    import plotly.express as px
    fig = px.line(
        view, x="month_date", y="Blocked Stock Qty", color=dim,
        title=f"Blocked Stock Qty by {dim} (Top {top_n} @ latest month)",
        labels={"month_date": "Month", "Blocked Stock Qty": "Blocked Stock Qty", dim: dim},
    )
    fig.update_layout(hovermode="x unified", height=420, legend=dict(orientation="h", y=-0.2))
    st.plotly_chart(fig, use_container_width=True)

    with st.expander("Show grouped data"):
        st.dataframe(view, use_container_width=True)

# -------------------------------------------
# UI – ingestion controls
# -------------------------------------------
st.title("📦 Blocked Stock Reporting — Monthly (Latest Only)")
st.caption("Each load **drops** the previous data and snapshots; only the newest dataset is kept.")

with st.sidebar:
    st.markdown("### 📥 Ingest data (latest-only)")
    version_tag = st.text_input(
        "Version tag for uploads (informational)",
        value=datetime.utcnow().strftime("%Y-%m-%d"),
        help="Used in logs for this single latest load; data retention is latest-only."
    )

    uploads = st.file_uploader(
        "Upload Excel (.xlsx)",
        type=["xlsx"],
        accept_multiple_files=True,
        help="Upload 'RM Extract - Data by Month.xlsx' (and optionally 'PO_history.xlsx')."
    )

    col1, col2 = st.columns(2)
    with col1:
        if st.button("Load uploaded files (overwrite previous)", type="primary", use_container_width=True, disabled=(not uploads)):
            msgs = load_uploaded_files(uploads, version_tag=version_tag)
            for m in msgs:
                st.toast(m, icon="✅" if m.startswith("✅") else "⚠️" if m.startswith("⚠️") else "❌")
            # refresh caches
            load_rm_for_report_sqlite.clear()
            load_rm_for_report_duckdb.clear()

    with col2:
        if st.button("Scan & load (script folder, overwrite previous)", use_container_width=True):
            xlsx_paths = sorted(glob.glob(str(SCRIPT_DIR / "*.xlsx")))
            if not xlsx_paths:
                st.warning(f"No .xlsx files found next to the script in {SCRIPT_DIR}.")
            else:
                msgs = scan_script_folder_and_ingest()
                for m in msgs:
                    st.toast(m, icon="✅" if m.startswith("✅") else "⚠️")
                load_rm_for_report_sqlite.clear()
                load_rm_for_report_duckdb.clear()

    st.caption(f"Script folder: `{SCRIPT_DIR}`")

st.markdown("### ⚙️ Chart options")
extend_series = st.checkbox(
    "Extend to current month (forward‑fill last known value)",
    value=True,
    help="Reindexes the monthly series through the current month and forward‑fills."
)

# ----------------------------------------------------------
# AUTO-LOAD ON STARTUP WHEN DB IS EMPTY (recommended)
# ----------------------------------------------------------
# If no RM table or it's empty, and .xlsx files exist next to the script,
# auto-trigger the folder ingestion ONCE.
initial_check_conn = get_conn()
try:
    existing = pd.read_sql(
        f"SELECT COUNT(*) AS n FROM {TABLE_RM}",
        initial_check_conn
    )
    rm_count = existing["n"].iloc[0]
except Exception:
    rm_count = 0  # table does not exist yet

# Auto-trigger only if database is empty
if rm_count == 0:
    xlsx_paths = sorted(glob.glob(str(SCRIPT_DIR / "*.xlsx")))
    if xlsx_paths:
        st.info("📂 No data in DB — auto-loading Excel files from script folder...")
        msgs = scan_script_folder_and_ingest()
        for m in msgs:
            st.toast(m, icon="✅" if m.startswith("✅") else "⚠️")
        # Clear caches so freshly loaded data is visible
        load_rm_for_report_sqlite.clear()
        load_rm_for_report_duckdb.clear()

# -------------------------------------------
# Load latest-only RM data for reporting
# -------------------------------------------
try:
    df_rm = load_rm_for_report()
except Exception as e:
    st.error(f"Failed to load RM data for reporting: {e}")
    st.stop()

if df_rm.empty:
    st.info("No data available. Ensure `.xlsx` files are next to the script or upload via the sidebar. Previous contents are dropped each time.")
    st.stop()

# Filters
df_filtered = apply_filters(df_rm)

# KPIs
c1, c2, c3 = st.columns(3)
c1.metric("Rows (after filters)", f"{len(df_filtered):,}")
c2.metric("Months", df_filtered["month_date"].nunique())
c3.metric("Blocked Stock Total", f"{df_filtered['Blocked Stock Qty'].sum():,.2f}")

# Charts
render_time_series(df_filtered, extend_series=extend_series)
render_cut_by_dimensions(df_filtered, extend_series=extend_series)

with st.expander("Show filtered rows"):
    st.dataframe(df_filtered.sort_values(["month_date"]), use_container_width=True, height=400)
    st.download_button(
        "Download filtered CSV",
        df_filtered.to_csv(index=False).encode("utf-8"),
        file_name="blocked_stock_filtered_latest.csv",
        mime="text/csv"
    )

st.markdown("---")

# ===========================================
# 🆕 PO ANALYSIS SECTION
# ===========================================
st.header("📑 PO Analysis (Top 30 by Quantity)")

materials, po_mapping = load_po_material_choices()
if not materials:
    st.info("PO data not available yet (or missing required columns like Material / Dates / Quantity). Load PO history and try again.")
else:
    colA, colB = st.columns([2, 1])
    with colA:
        mat_choice = st.selectbox("Select Material ID (from PO history)", options=materials, index=0)
    with colB:
        go_btn = st.button("Show PO analysis", type="primary", use_container_width=True)

    if go_btn:
        df_po = load_po_for_material(mat_choice, po_mapping)
        if df_po.empty:
            st.warning("No valid PO rows found for this material with usable dates and quantity.")
        else:
            # Top 30 by PO Qty
            top = df_po.sort_values("PO Qty", ascending=False).head(30).copy()

            # KPIs
            avg_lt = float(np.nanmean(top["lead_time_days"])) if len(top) else np.nan
            std_lt = float(np.nanstd(top["lead_time_days"], ddof=1)) if len(top) > 1 else np.nan

            k1, k2, k3 = st.columns(3)
            with k1:
                st.metric("PO lines (top set)", f"{len(top):,}")
            with k2:
                st.metric("Avg Lead‑Time (days)", f"{avg_lt:,.1f}" if np.isfinite(avg_lt) else "—")
            with k3:
                st.metric("Std Dev Lead‑Time", f"{std_lt:,.1f}" if np.isfinite(std_lt) else "—")

            # Table view
            show_cols = [c for c in [
                "PO Number", "Material ID", "PO Creation Date", "Goods Receipt Date",
                "lead_time_days", "PO Qty", "Vendor", "Plant", "Short Text", "Currency", "Net Price"
            ] if c in top.columns]
            st.dataframe(top[show_cols], use_container_width=True, height=420)

            st.download_button(
                "Download Top 30 PO lines (CSV)",
                top[show_cols].to_csv(index=False).encode("utf-8"),
                file_name=f"po_top30_{mat_choice}.csv",
                mime="text/csv"
            )

            # Chart: lead-time vs PO (sorted by quantity)
            import plotly.express as px
            top_plot = top.sort_values(["PO Qty", "lead_time_days"], ascending=[False, True]).copy()
            top_plot["PO Number"] = top_plot["PO Number"].astype(str)
            fig = px.bar(
                top_plot,
                x="PO Number",
                y="lead_time_days",
                hover_data=show_cols,
                title=f"Lead‑Time (days) by PO — Top 30 by Quantity for {mat_choice}",
                labels={"lead_time_days": "Lead‑Time (days)", "PO Number": "PO Number"},
            )
            fig.update_layout(xaxis_tickangle=-45, height=460)
            st.plotly_chart(fig, use_container_width=True)

st.markdown(
    "ℹ️ **Notes**\n"
    "- Lead‑time is computed as **Goods Receipt Date − PO Creation Date** (in days).\n"
    "- Column name variants are supported (e.g., *Created On* / *Actual GR Date* / *PO qty*).\n"
    "- The PO view uses the **latest-only dataset** (reloaded on each ingestion)."
)



