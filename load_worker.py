"""
Worker for loading a single CSV + detail.log.
Used from data_loader.py with ThreadPoolExecutor (no Streamlit, no multiprocessing).
"""
import os
import re
from typing import List

import pandas as pd

from config_loader import get_csv_parser_metadata_test_names

CSV_COL_TESTNAME = "TestName"
CSV_COL_TESTVARIATION = "TestVariation"
CSV_COL_VALUE = "Value"
CSV_COL_UNITS = "Units"
CSV_COL_LOWER = "LowerLimit"
CSV_COL_UPPER = "UpperLimit"
CSV_COL_OPERATION = "Operation"


def _parse_csv_pega(csv_path: str, station_name: str) -> List[dict]:
    """Parse a Pega CSV file (without Streamlit dependency)."""
    rows = []
    df_csv = None
    for enc in ["utf-8", "utf-8-sig", "latin-1", "cp1250"]:
        try:
            df_csv = pd.read_csv(csv_path, encoding=enc)
            break
        except (UnicodeDecodeError, OSError):
            continue
    if df_csv is None or df_csv.empty or CSV_COL_TESTNAME not in df_csv.columns or CSV_COL_VALUE not in df_csv.columns:
        return rows

    path_parts = csv_path.replace("\\", "/").split("/")
    sn, raw_date = "Unknown", ""
    for part in path_parts:
        if "_" in part and part[0].isdigit():
            segs = part.split("_")
            if len(segs) >= 2 and len(segs[0]) >= 10:
                sn = segs[0]
                if len(segs[1]) >= 14:
                    raw_date = f"{segs[1][:4]}-{segs[1][4:6]}-{segs[1][6:8]} {segs[1][8:10]}:{segs[1][10:12]}:{segs[1][12:14]}"
                elif len(segs[1]) >= 8:
                    raw_date = f"{segs[1][:4]}-{segs[1][4:6]}-{segs[1][6:8]}"
                break
    if "SerialNumber" in df_csv.columns and not df_csv["SerialNumber"].empty:
        first_sn = df_csv["SerialNumber"].iloc[0]
        if pd.notna(first_sn) and str(first_sn).strip():
            sn = str(first_sn).strip()

    metadata_tests = get_csv_parser_metadata_test_names()
    ncols = len(df_csv.columns)
    for r in df_csv.itertuples(index=False):
        if CSV_COL_TESTVARIATION in df_csv.columns:
            var = str(getattr(r, CSV_COL_TESTVARIATION, "") or "").strip()
            if var and "retry" in var.lower():
                continue
        test_name = str(getattr(r, CSV_COL_TESTNAME, "") or "").strip()
        if not test_name:
            continue
        val_raw = getattr(r, CSV_COL_VALUE, None)
        if pd.isna(val_raw) or str(val_raw).strip() == "":
            continue
        val_str = str(val_raw).strip()
        if not val_str:
            continue
        is_metadata = test_name in metadata_tests
        if not is_metadata:
            try:
                float(val_raw)
            except (TypeError, ValueError):
                continue
        unit = str(getattr(r, CSV_COL_UNITS, "") or "").strip()
        if not unit or unit.lower() in ("nan", "times", ""):
            unit = ""
        lower = getattr(r, CSV_COL_LOWER, None)
        upper = getattr(r, CSV_COL_UPPER, None)
        try:
            lower_f = float(lower) if pd.notna(lower) and str(lower).strip() else None
        except (TypeError, ValueError):
            lower_f = None
        try:
            upper_f = float(upper) if pd.notna(upper) and str(upper).strip() else None
        except (TypeError, ValueError):
            upper_f = None
        raw_dt_val = str(getattr(r, "StartDateTime", "") or "").strip() if "StartDateTime" in df_csv.columns else ""
        if not raw_dt_val:
            raw_dt_val = raw_date
        try:
            dt_val = pd.to_datetime(raw_dt_val) if raw_dt_val else None
        except Exception:
            dt_val = None
        operation = ""
        if CSV_COL_OPERATION in df_csv.columns:
            operation = str(getattr(r, CSV_COL_OPERATION, "") or "").strip()
        elif ncols > 3:
            operation = str(r[3]).strip() if pd.notna(r[3]) else ""
        if operation and operation.lower() in ("nan", "none", ""):
            operation = ""
        row_out = {
            "Station": station_name,
            "TestName": test_name,
            "ValueRaw": val_str,
            "SN": sn,
            "Date": dt_val,
            "RawDate": raw_dt_val,
            "Unit": unit,
            "File": os.path.basename(csv_path),
            "LowerLimit": lower_f,
            "UpperLimit": upper_f,
        }
        if operation:
            row_out["Operation"] = operation
        rows.append(row_out)
    return rows


def _extract_detail_log_versions(log_path: str) -> dict:
    """Read versions from detail.log (without Streamlit)."""
    result = {}
    encodings = ["utf-8", "utf-8-sig", "latin-1", "cp1250"]
    content = None
    for enc in encodings:
        try:
            with open(log_path, "r", encoding=enc) as f:
                content = f.read()
            break
        except (UnicodeDecodeError, OSError):
            continue
    if not content:
        return result
    patterns = [
        (r"set\s+fw_prodtest_ver\s*=\s*(\S+)", "FW_prodtest"),
        (r"set\s+bootloader_ver\s*=\s*(\S+)", "Bootloader_version"),
        (r"set\s+boardloader_ver\s*=\s*(\S+)", "Boardloader_version"),
        (r"set\s+hwid\s*=\s*(\S+)", "HW_version"),
        (r"set\s+touch_ver\s*=\s*(\S+)", "Touch_version"),
    ]
    for pat, key in patterns:
        m = re.search(pat, content, re.IGNORECASE)
        if m:
            result[key] = m.group(1).strip()
    return result


def process_one_csv_folder(
    csv_path: str, root: str, has_detail: bool, station_name: str
) -> List[dict]:
    """
    Parse one CSV and enrich rows with metadata from detail.log.
    Called in a subprocess - the whole processing is governed by timeout in the main process.
    """
    rows = _parse_csv_pega(csv_path, station_name)
    if not rows:
        return []
    log_path = os.path.join(root, "detail.log")
    versions = _extract_detail_log_versions(log_path) if has_detail else {}
    for r in rows:
        r.update(versions)
        r["_folder"] = root
    return rows
