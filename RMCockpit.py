# streamlit_app.py
# Streamlit app: Latest-only ingestion with auto-load on startup (if DB is empty),
# monthly inventory reporting from "Month/Year" with global filters above tabs,
# optional DuckDB+Parquet fast path, robust PO normalization+tolerant fallback,
# PO Analysis tab (Top 30 by quantity with Avg / Std Dev lead-time),
# and VERBOSE PO ingestion diagnostics.
# Author: Emmanuel + Copilot

import glob
import io
import os
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Sequence, Tuple, Dict, Any

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

TABLE_RM = "rm_inventory_raw"        # normalized RM data (uses 'month_date')
TABLE_PO_RAW = "po_history_raw"      # raw PO (optional, for traceability)
TABLE_PO_NORM = "po_history_norm"    # canonical PO table (used by analysis)
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
# Sidebar: Optimized mode + Ingestion controls
# -------------------------------------------
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
        st.sidebar.warning(
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
        # PO normalized indexes
        f'CREATE INDEX IF NOT EXISTS idx_po_material     ON {TABLE_PO_NORM}(material_id);',
        f'CREATE INDEX IF NOT EXISTS idx_po_po_qty       ON {TABLE_PO_NORM}(po_qty);',
    ]
    for sql in idx_sql:
        try:
            conn.execute(sql)
        except Exception:
            pass
    conn.commit()

def purge_all_previous_records(conn: sqlite3.Connection):
    """Hard reset: drop tables + remove Parquet so the new schema is fresh and latest-only."""
    with st.spinner("🧹 Purging previous tables and snapshots …"):
        for tbl in (TABLE_RM, TABLE_PO_RAW, TABLE_PO_NORM, TABLE_LOG):
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
def _norm_header(s: str) -> str:
    """normalize header for robust matching"""
    return str(s).strip().lower().replace(" ", "").replace("_", "").replace(".", "")

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
    """
    Classify files as RM or PO using both filename hints and column heuristics.
    """
    name_lower = (file_name or "").lower()

    # Strong filename hints first
    if "po_history" in name_lower or name_lower.endswith("po.xlsx") or name_lower.startswith("po_") or "po" in name_lower:
        return "PO"
    if "rm extract" in name_lower or "data by month" in name_lower or "rm_extract" in name_lower:
        return "RM"

    # Fallback: column-based heuristic
    cols = set(df.columns)
    if {"Blocked Stock Qty", "Material ID", "Plant", "Plant ID"}.issubset(cols):
        return "RM"
    return "PO"

# ---------- RM normalize ----------
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

# ---------- PO normalize + VERBOSE DIAGNOSTICS ----------
def normalize_po_dataframe(df: pd.DataFrame, src_name: str, version_tag: str) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Normalize PO history to a canonical schema:
      po_number, material_id, po_creation_date, gr_date (or delivery_date fallback),
      po_qty, vendor, plant, short_text, currency, net_price, lead_time_days

    Returns: (normalized_df, diagnostics_dict)
    """
    raw = df.copy()
    raw.columns = [c.strip() for c in raw.columns]
    header_map = { _norm_header(c): c for c in raw.columns }

    def pick(*cands: str) -> Optional[str]:
        """Pick first existing source column among candidates using relaxed matching."""
        for cand in cands:
            k = _norm_header(cand)
            if k in header_map:
                return header_map[k]
        # relaxed scan
        for c in raw.columns:
            if _norm_header(c) in [_norm_header(x) for x in cands]:
                return c
        return None

    # Identify columns (broad coverage)
    col_po     = pick("PO Number", "PurDoc", "PO", "EBELN", "Purchase Order", "Purch Doc")
    col_mat    = pick("Material ID", "Material", "Material Number", "Material Code", "MATNR")
    col_cdate  = pick("PO Creation Date", "Created On", "Doc. Date", "Creation Date", "Document Date")
    col_grdate = pick("Goods Receipt Date", "Actual GR Date", "GR Date", "Goods Rcpt Date")
    col_ddate  = pick("Delivery Date", "Deliv. Date", "Scheduled Delivery Date", "DeliveryDt")
    col_qty    = pick("PO qty", "PO Qty", "Order Qty", "Quantity", "Qty", "Order Quantity")
    col_vendor = pick("Vendor", "Supplier")
    col_plant  = pick("Plant", "Plnt", "Plant ID")
    col_text   = pick("Short Text", "Material Desc", "Description")
    col_curr   = pick("Crcy", "Currency")
    col_price  = pick("Net Price", "Price")

    diags: Dict[str, Any] = {
        "file": src_name,
        "version_tag": version_tag,
        "total_rows": int(raw.shape[0]),
        "map_po_number": col_po or "",
        "map_material": col_mat or "",
        "map_creation_date": col_cdate or "",
        "map_gr_date": col_grdate or "",
        "map_delivery_date": col_ddate or "",
        "map_qty": col_qty or "",
        "missing_creation_date": 0,
        "missing_gr_date": 0,
        "missing_delivery_date": 0,
        "missing_both_dates": 0,
        "missing_qty": 0,
        "non_numeric_qty": 0,
        "negative_lead_time": 0,
        "rows_after_filters": 0,
        "duplicates_dropped": 0,
        "rows_written": 0,
        "note": "",
    }

    essentials = [col_mat, col_cdate, (col_grdate or col_ddate), col_qty]
    if any(c is None for c in essentials):
        diags["note"] = "Essential columns missing (Material, Creation Date, GR/Delivery Date, or Quantity)."
        # Return an empty, but schema-correct DataFrame
        empty = pd.DataFrame(columns=[
            "po_number","material_id","po_creation_date","gr_date","po_qty",
            "vendor","plant","short_text","currency","net_price","lead_time_days",
            "source_file","version_tag","uploaded_at_utc"
        ])
        return empty, diags

    # Build temp for diagnostics
    tmp = pd.DataFrame()
    tmp["po_number"] = (raw[col_po] if col_po else raw.index).astype(str)
    tmp["material_id"] = raw[col_mat].astype(str).str.strip()
    tmp["po_creation_date"] = excel_serial_to_datetime(raw[col_cdate])
    # Prefer GR date; if absent, fall back to Delivery Date
    if col_grdate:
        tmp["gr_date"] = excel_serial_to_datetime(raw[col_grdate])
    else:
        tmp["gr_date"] = pd.NaT
    tmp["delivery_date"] = excel_serial_to_datetime(raw[col_ddate]) if col_ddate else pd.NaT
    qty_raw = pd.to_numeric(raw[col_qty], errors="coerce")
    tmp["po_qty"] = qty_raw

    # Optional attributes
    tmp["vendor"] = raw[col_vendor].astype(str).str.strip() if col_vendor else ""
    tmp["plant"]  = raw[col_plant].astype(str).str.strip()  if col_plant  else ""
    tmp["short_text"] = raw[col_text].astype(str).str.strip() if col_text else ""
    tmp["currency"] = raw[col_curr].astype(str).str.strip()   if col_curr else ""
    tmp["net_price"] = pd.to_numeric(raw[col_price], errors="coerce") if col_price else np.nan

    # Diagnostics on missing fields
    diags["missing_creation_date"] = int(tmp["po_creation_date"].isna().sum())
    diags["missing_gr_date"] = int(tmp["gr_date"].isna().sum())
    if col_ddate:
        diags["missing_delivery_date"] = int(tmp["delivery_date"].isna().sum())
    diags["missing_qty"] = int(tmp["po_qty"].isna().sum())

    # Pick the actual receipt date we use (GR preferred, else Delivery)
    actual_receipt = tmp["gr_date"].copy()
    use_delivery_mask = actual_receipt.isna() & tmp["delivery_date"].notna()
    actual_receipt.loc[use_delivery_mask] = tmp.loc[use_delivery_mask, "delivery_date"]
    # Rows with both missing dates:
    both_missing_mask = actual_receipt.isna() | tmp["po_creation_date"].isna()
    diags["missing_both_dates"] = int((tmp["po_creation_date"].isna() & actual_receipt.isna()).sum())

    # Non-numeric qty (coerced to NaN)
    diags["non_numeric_qty"] = int(tmp["po_qty"].isna().sum())  # same as missing_qty, but explicit

    # Lead-time for sanity checks
    lead_time_days = (actual_receipt - tmp["po_creation_date"]).dt.days
    diags["negative_lead_time"] = int((lead_time_days < 0).sum())

    # Build normalized output
    out = pd.DataFrame({
        "po_number": tmp["po_number"],
        "material_id": tmp["material_id"],
        "po_creation_date": tmp["po_creation_date"],
        "gr_date": actual_receipt,  # GR if available, else Delivery
        "po_qty": tmp["po_qty"],
        "vendor": tmp["vendor"],
        "plant": tmp["plant"],
        "short_text": tmp["short_text"],
        "currency": tmp["currency"],
        "net_price": tmp["net_price"],
    })

    # Compute final lead time
    out["lead_time_days"] = (out["gr_date"] - out["po_creation_date"]).dt.days

    # Keep only rows that can produce a lead-time and have a quantity
    before_filter = len(out)
    out = out.dropna(subset=["po_creation_date", "gr_date", "po_qty"])
    diags["rows_after_filters"] = int(len(out))

    # De-duplicate: if duplicates occur, keep the last
    before_dedup = len(out)
    out = out.drop_duplicates(
        subset=["po_number", "material_id", "po_creation_date", "gr_date", "po_qty"],
        keep="last"
    )
    diags["duplicates_dropped"] = int(before_dedup - len(out))

    # Meta
    out["source_file"] = src_name
    out["version_tag"] = version_tag
    out["uploaded_at_utc"] = datetime.utcnow().isoformat(timespec="seconds")

    diags["rows_written"] = int(len(out))
    if len(out) == 0 and before_filter > 0:
        diags["note"] = "All rows dropped after filters (dates/qty) or de-dup."

    return out, diags

# ---------- IO helpers ----------
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
# Ingestion (LATEST ONLY, hard drop) + Diagnostics
# -------------------------------------------
def ingest_batch(files: List[io.BytesIO], version_tag_for_uploads: str) -> List[str]:
    """
    Ingest a batch of files:
      - DROP TABLES + remove Parquet (latest-only; guarantees schema freshness)
      - Ingest new files (RM normalized monthly + PO raw + PO normalized)
      - Build a single Parquet snapshot named rm_latest.parquet (if optimized mode on)
      - Collect VERBOSE diagnostics for PO normalization and display them
    """
    msgs_all = []
    po_diags_all: List[Dict[str, Any]] = []
    conn = get_conn()
    # hard drop before each batch
    purge_all_previous_records(conn)
    ensure_tables(conn)

    overall = st.progress(0, text="Starting ingestion …")
    total = len(files) if files else 0
    loaded_any_rm = False
    latest_rm_rows = 0
    latest_rm_df = None
    po_norm_df_all = []  # accumulate to one normalized table

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
                # Log entry
                conn.execute(
                    f"INSERT INTO {TABLE_LOG}(table_name, source_file, version_tag, uploaded_at_utc, rows_loaded) "
                    f"VALUES (?, ?, ?, ?, ?)",
                    (TABLE_RM, fname, vtag, datetime.utcnow().isoformat(timespec="seconds"), int(rows))
                )
                conn.commit()
                msgs_all.append(f"✅ {fname}: loaded {rows:,} RM row(s).")

            else:
                vtag = version_tag_for_uploads
                # store raw for traceability
                df_raw = df_all.copy()
                df_raw["source_file"] = fname
                df_raw["version_tag"] = vtag
                df_raw["uploaded_at_utc"] = datetime.utcnow().isoformat(timespec="seconds")
                rows_raw = to_sql_with_progress(df_raw, TABLE_PO_RAW, conn, label=fname)
                conn.execute(
                    f"INSERT INTO {TABLE_LOG}(table_name, source_file, version_tag, uploaded_at_utc, rows_loaded) "
                    f"VALUES (?, ?, ?, ?, ?)",
                    (TABLE_PO_RAW, fname, vtag, datetime.utcnow().isoformat(timespec="seconds"), int(rows_raw))
                )
                conn.commit()

                # normalize PO into canonical schema (po_history_norm) + collect diagnostics
                df_po_norm, po_diag = normalize_po_dataframe(df_all, fname, vtag)
                po_diags_all.append(po_diag)

                if not df_po_norm.empty:
                    po_norm_df_all.append(df_po_norm)
                    msgs_all.append(f"✅ {fname}: normalized {len(df_po_norm):,} PO row(s).")
                else:
                    msgs_all.append(f"⚠️ {fname}: PO normalization returned no rows. See diagnostics below.")

        except Exception as e:
            msgs_all.append(f"❌ {fname}: ingestion failed — {e!s}")

        if total:
            overall.progress(int(i / total * 100), text=f"Ingested {i}/{total} files")

    overall.empty()

    # Write accumulated normalized PO in one go (fewer commits → faster)
    if po_norm_df_all:
        df_norm_all = pd.concat(po_norm_df_all, ignore_index=True)
        _ = to_sql_with_progress(df_norm_all, TABLE_PO_NORM, conn, label="PO normalized (all)")
        conn.commit()

    # Indexes for fast reads
    try:
        ensure_indexes(conn)
    except Exception:
        pass

    # Latest-only Parquet snapshot for RM
    if loaded_any_rm and opt_mode and HAVE_DUCKDB and latest_rm_df is not None:
        path = write_parquet_snapshot(latest_rm_df)
        if path:
            msgs_all.append(f"💾 Parquet snapshot created: {Path(path).name}")

    # Summary
    if loaded_any_rm:
        msgs_all.append(f"✅ Finished. Kept only the latest RM dataset with {latest_rm_rows:,} row(s).")
    else:
        msgs_all.append("⚠️ No RM data detected in this batch — database is empty (latest-only policy).")

    # PO summary
    conn.commit()
    try:
        po_cnt = pd.read_sql(f"SELECT COUNT(*) AS n FROM {TABLE_PO_NORM}", conn)["n"].iloc[0]
        msgs_all.append(f"🧾 PO normalized rows available: {po_cnt:,}")
    except Exception:
        msgs_all.append("⚠️ No normalized PO rows available.")

    # ---- DIAGNOSTICS RENDERING ----
    if po_diags_all:
        with st.expander("🧪 PO Ingestion Diagnostics (verbose)", expanded=True):
            df_diag = pd.DataFrame(po_diags_all)
            # Pretty order for columns
            preferred_cols = [
                "file","version_tag","total_rows",
                "map_material","map_creation_date","map_gr_date","map_delivery_date","map_qty","map_po_number",
                "missing_creation_date","missing_gr_date","missing_delivery_date","missing_both_dates",
                "missing_qty","non_numeric_qty","negative_lead_time",
                "rows_after_filters","duplicates_dropped","rows_written","note"
            ]
            show_cols = [c for c in preferred_cols if c in df_diag.columns] + [c for c in df_diag.columns if c not in preferred_cols]
            st.dataframe(df_diag[show_cols], use_container_width=True, height=320)
            st.download_button(
                "Download diagnostics CSV",
                df_diag[show_cols].to_csv(index=False).encode("utf-8"),
                file_name="po_ingestion_diagnostics.csv",
                mime="text/csv"
            )

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
# PO Analysis helpers (use normalized table) + fallback to raw
# -------------------------------------------
@st.cache_data(show_spinner=False)
def po_material_choices_from_norm() -> List[str]:
    conn = get_conn()
    try:
        df = pd.read_sql(f'SELECT DISTINCT material_id FROM {TABLE_PO_NORM} WHERE material_id IS NOT NULL LIMIT 50000', conn)
        return sorted(df["material_id"].astype(str).str.strip().unique().tolist())
    except Exception:
        return []

@st.cache_data(show_spinner=False)
def load_po_top30_for_material(material_id: str) -> pd.DataFrame:
    """Return Top 30 POs by po_qty for a material from normalized table."""
    conn = get_conn()
    try:
        df = pd.read_sql(
            f"""
            SELECT
              po_number, material_id, po_creation_date, gr_date, po_qty,
              vendor, plant, short_text, currency, net_price, lead_time_days
            FROM {TABLE_PO_NORM}
            WHERE material_id = ?
            """,
            conn,
            params=[material_id],
            parse_dates=["po_creation_date", "gr_date"]
        )
    except Exception:
        return pd.DataFrame()

    # Ensure types
    if "po_qty" in df.columns:
        df["po_qty"] = pd.to_numeric(df["po_qty"], errors="coerce")

    # Keep valid rows only
    df = df.dropna(subset=["po_creation_date", "gr_date", "lead_time_days", "po_qty"])
    df = df.sort_values("po_qty", ascending=False).head(30)
    return df

@st.cache_data(show_spinner=False)
def load_po_top30_for_material_from_raw(material_id: str) -> pd.DataFrame:
    """
    Fallback: infer columns from the raw PO table on-the-fly and compute lead-time.
    Returns Top 30 by quantity (if possible).
    """
    conn = get_conn()
    # Identify material column in raw quickly
    try:
        cols = pd.read_sql(f'PRAGMA table_info("{TABLE_PO_RAW}")', conn)
        raw_cols = cols["name"].tolist() if not cols.empty else []
    except Exception:
        raw_cols = []

    if not raw_cols:
        return pd.DataFrame()

    def pick_in_raw(rc: List[str], *cands: str) -> Optional[str]:
        rc_norm = { _norm_header(c): c for c in rc }
        for cand in cands:
            k = _norm_header(cand)
            if k in rc_norm:
                return rc_norm[k]
        for c in rc:
            if _norm_header(c) in [_norm_header(x) for x in cands]:
                return c
        return None

    mat_col = pick_in_raw(raw_cols, "Material ID", "Material", "Material Number", "Material Code", "MATNR")
    if mat_col is None:
        return pd.DataFrame()

    # Bring subset
    df = pd.read_sql(f'SELECT * FROM {TABLE_PO_RAW} WHERE "{mat_col}" = ?', conn, params=[material_id])

    col_po     = pick_in_raw(df.columns.tolist(), "PO Number", "PurDoc", "PO", "EBELN", "Purchase Order", "Purch Doc")
    col_cdate  = pick_in_raw(df.columns.tolist(), "PO Creation Date", "Created On", "Doc. Date", "Creation Date", "Document Date")
    col_grdate = pick_in_raw(df.columns.tolist(), "Goods Receipt Date", "Actual GR Date", "GR Date", "Goods Rcpt Date")
    col_ddate  = pick_in_raw(df.columns.tolist(), "Delivery Date", "Deliv. Date", "Scheduled Delivery Date", "DeliveryDt")
    col_qty    = pick_in_raw(df.columns.tolist(), "PO qty", "PO Qty", "Order Qty", "Quantity", "Qty", "Order Quantity")

    if col_cdate is None or (col_grdate is None and col_ddate is None) or col_qty is None:
        return pd.DataFrame()

    out = pd.DataFrame()
    out["po_number"] = (df[col_po] if col_po else df.index).astype(str)
    out["material_id"] = df[mat_col].astype(str).str.strip()
    out["po_creation_date"] = excel_serial_to_datetime(df[col_cdate])
    out["gr_date"] = excel_serial_to_datetime(df[col_grdate]) if col_grdate else excel_serial_to_datetime(df[col_ddate])
    out["po_qty"] = pd.to_numeric(df[col_qty], errors="coerce")
    out = out.dropna(subset=["po_creation_date", "gr_date", "po_qty"])
    out["lead_time_days"] = (out["gr_date"] - out["po_creation_date"]).dt.days
    out = out.sort_values("po_qty", ascending=False).head(30)
    return out

# -------------------------------------------
# RM: Filters (rendered ABOVE tabs)
# -------------------------------------------
def render_global_filters(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, List[str]]]:
    """
    Render global filters for RM dataset on top of the page and return filtered dataframe
    plus the selected filter values (for potential future use).
    """
    st.subheader("🔎 Filters (RM dataset)")
    # Build options
    plant_opt   = sorted(df["Plant"].dropna().astype(str).unique().tolist()) if "Plant" in df else []
    plant_id_opt= sorted(df["Plant ID"].dropna().astype(str).unique().tolist()) if "Plant ID" in df else []
    mat_id_opt  = sorted(df["Material ID"].dropna().astype(str).unique().tolist()) if "Material ID" in df else []
    mat_desc_opt= sorted(df["Material Desc"].dropna().astype(str).unique().tolist()) if "Material Desc" in df else []
    mg_opt      = sorted(df["Material Group Desc"].dropna().astype(str).unique().tolist()) if "Material Group Desc" in df else []

    c1, c2, c3 = st.columns(3)
    with c1:
        sel_plant = st.multiselect("Plant", plant_opt)
    with c2:
        sel_plant_id = st.multiselect("Plant ID", plant_id_opt)
    with c3:
        sel_mg = st.multiselect("Material Group Desc", mg_opt)

    c4, c5 = st.columns(2)
    with c4:
        sel_mat_id = st.multiselect("Material ID", mat_id_opt)
    with c5:
        sel_mat_desc = st.multiselect("Material Desc", mat_desc_opt)

    # Apply filters
    mask = pd.Series(True, index=df.index)
    if sel_plant:
        mask &= df["Plant"].isin(sel_plant)
    if sel_plant_id:
        mask &= df["Plant ID"].isin(sel_plant_id)
    if sel_mat_id:
        mask &= df["Material ID"].isin(sel_mat_id)
    if sel_mat_desc:
        mask &= df["Material Desc"].isin(sel_mat_desc)
    if sel_mg:
        mask &= df["Material Group Desc"].isin(sel_mg)

    df_f = df.loc[mask].copy()
    return df_f, {
        "Plant": sel_plant,
        "Plant ID": sel_plant_id,
        "Material ID": sel_mat_id,
        "Material Desc": sel_mat_desc,
        "Material Group Desc": sel_mg,
    }

# -------------------------------------------
# RM: Charts
# -------------------------------------------
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
    st.markdown("#### 📈 Blocked Stock Qty — Monthly Inventory Evolution")

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
# UI – ingestion controls (sidebar)
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
        help="Upload 'RM Extract - Data by Month.xlsx' and 'PO_history.xlsx' (or similarly named files)."
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
            po_material_choices_from_norm.clear()
            load_po_top30_for_material.clear()
            load_po_top30_for_material_from_raw.clear()

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
                po_material_choices_from_norm.clear()
                load_po_top30_for_material.clear()
                load_po_top30_for_material_from_raw.clear()

    st.caption(f"Script folder: `{SCRIPT_DIR}`")

# ----------------------------------------------------------
# AUTO-LOAD ON STARTUP WHEN DB IS EMPTY (recommended)
# ----------------------------------------------------------
initial_check_conn = get_conn()
try:
    existing = pd.read_sql(
        f"SELECT COUNT(*) AS n FROM {TABLE_RM}",
        initial_check_conn
    )
    rm_count = existing["n"].iloc[0]
except Exception:
    rm_count = 0  # table does not exist yet

if rm_count == 0:
    xlsx_paths = sorted(glob.glob(str(SCRIPT_DIR / "*.xlsx")))
    if xlsx_paths:
        st.info("📂 No data in DB — auto-loading Excel files from script folder...")
        msgs = scan_script_folder_and_ingest()
        for m in msgs:
            st.toast(m, icon="✅" if m.startswith("✅") else "⚠️")
        load_rm_for_report_sqlite.clear()
        load_rm_for_report_duckdb.clear()
        po_material_choices_from_norm.clear()
        load_po_top30_for_material.clear()
        load_po_top30_for_material_from_raw.clear()

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

# -------------------------------------------
# Global controls ABOVE tabs
# -------------------------------------------
st.markdown("### ⚙️ Chart options")
extend_series = st.checkbox(
    "Extend to current month (forward‑fill last known value)",
    value=True,
    help="Reindexes the monthly series through the current month and forward‑fills."
)

# Render global filters for RM and get filtered df
df_filtered, selected_filters = render_global_filters(df_rm)

# -------------------------------------------
# TABS
# -------------------------------------------
tab1, tab2 = st.tabs(["📦 Blocked Stock", "📑 PO Analysis"])

with tab1:
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

with tab2:
    st.markdown("Select a material to analyze the **Top 30 POs by quantity** and compute **Avg / Std Dev lead‑time** (GR/Delivery − Creation).")
    material_options = po_material_choices_from_norm()
    if not material_options:
        st.info("PO data not available (normalized table is empty). "
                "Please ensure a PO .xlsx was included during ingestion.")
    else:
        colA, colB = st.columns([2, 1])
        with colA:
            mat_choice = st.selectbox("Material ID (from PO history)", options=material_options, index=0)
        with colB:
            go = st.button("Show PO analysis", type="primary", use_container_width=True)

        if go:
            # First try normalized table
            df_po_top = load_po_top30_for_material(mat_choice)
            # Fallback: try to build from raw if normalized has nothing
            if df_po_top.empty:
                df_po_top = load_po_top30_for_material_from_raw(mat_choice)

            if df_po_top.empty:
                st.warning("No valid PO lines found for this material (with dates and quantity). "
                           "Check that the PO file has Creation and GR (or Delivery) dates, and a Quantity column.")
            else:
                # KPIs
                avg_lt = float(np.nanmean(df_po_top["lead_time_days"])) if len(df_po_top) else np.nan
                std_lt = float(np.nanstd(df_po_top["lead_time_days"], ddof=1)) if len(df_po_top) > 1 else np.nan

                k1, k2, k3 = st.columns(3)
                with k1:
                    st.metric("PO lines (top set)", f"{len(df_po_top):,}")
                with k2:
                    st.metric("Avg Lead‑Time (days)", f"{avg_lt:,.1f}" if np.isfinite(avg_lt) else "—")
                with k3:
                    st.metric("Std Dev Lead‑Time", f"{std_lt:,.1f}" if np.isfinite(std_lt) else "—")

                # Table
                show_cols = [c for c in [
                    "po_number", "material_id", "po_creation_date", "gr_date",
                    "lead_time_days", "po_qty", "vendor", "plant", "short_text", "currency", "net_price"
                ] if c in df_po_top.columns]
                st.dataframe(df_po_top[show_cols], use_container_width=True, height=420)

                st.download_button(
                    "Download Top 30 PO lines (CSV)",
                    df_po_top[show_cols].to_csv(index=False).encode("utf-8"),
                    file_name=f"po_top30_{mat_choice}.csv",
                    mime="text/csv"
                )

                # Chart: lead-time vs PO (sorted by quantity)
                import plotly.express as px
                plot_df = df_po_top.sort_values(["po_qty", "lead_time_days"], ascending=[False, True]).copy()
                plot_df["po_number"] = plot_df["po_number"].astype(str)
                fig = px.bar(
                    plot_df,
                    x="po_number",
                    y="lead_time_days",
                    hover_data=show_cols,
                    title=f"Lead‑Time (days) by PO — Top 30 by Quantity for {mat_choice}",
                    labels={"lead_time_days": "Lead‑Time (days)", "po_number": "PO Number"},
                )
                fig.update_layout(xaxis_tickangle=-45, height=460)
                st.plotly_chart(fig, use_container_width=True)

st.markdown(
    "ℹ️ **Notes**\n"
    "- **Latest-only**: each ingestion **drops** previous rows and snapshots; only the newest dataset remains.\n"
    "- The monthly axis is derived from **`Month/Year`** and normalized to the **first day of each month**.\n"
    "- PO lead‑time is **Goods Receipt / Delivery Date − PO Creation Date** (days). Column variants are normalized automatically.\n"
    "- See **PO Ingestion Diagnostics** inside the ingestion step to understand row drops and mappings per file."
)

