import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from typing import List, Optional

import pandas as pd

try:
    import streamlit as st
except ImportError:
    st = None

from config_loader import get_csv_parser_metadata_test_names
from load_worker import process_one_csv_folder as _process_one_csv_folder_worker


# --- CSV (Pega) layout: TestName F, skip Retry Times G, Value P, Units Q, LowerLimit R, UpperLimit S ---
# Column D = Operation (station type)
CSV_COL_TESTNAME = "TestName"
CSV_COL_TESTVARIATION = "TestVariation"
CSV_COL_VALUE = "Value"
CSV_COL_UNITS = "Units"
CSV_COL_LOWER = "LowerLimit"
CSV_COL_UPPER = "UpperLimit"
CSV_COL_OPERATION = "Operation"


def _parse_csv_pega(csv_path: str, station_name: str) -> List[dict]:
    """Parse Pega CSV: tests from TestName column, skip Retry Times, Units Q, limits R,S."""
    rows = []
    df_csv = None
    for enc in ["utf-8", "utf-8-sig", "latin-1", "cp1250"]:
        try:
            df_csv = pd.read_csv(csv_path, encoding=enc)
            break
        except (UnicodeDecodeError, OSError):
            continue
    if df_csv is None or df_csv.empty:
        return rows
    if df_csv.empty or CSV_COL_TESTNAME not in df_csv.columns or CSV_COL_VALUE not in df_csv.columns:
        return rows

    # SN from path (SN_timestamp folder) or SerialNumber column
    path_parts = csv_path.replace("\\", "/").split("/")
    sn, raw_date = "Unknown", ""
    for part in path_parts:
        if "_" in part and part[0].isdigit():
            segs = part.split("_")
            if len(segs) >= 2 and len(segs[0]) >= 10:
                sn = segs[0]
                if len(segs[1]) >= 14:
                    raw_date = (
                        f"{segs[1][:4]}-{segs[1][4:6]}-{segs[1][6:8]} "
                        f"{segs[1][8:10]}:{segs[1][10:12]}:{segs[1][12:14]}"
                    )
                elif len(segs[1]) >= 8:
                    raw_date = f"{segs[1][:4]}-{segs[1][4:6]}-{segs[1][6:8]}"
                break
    if "SerialNumber" in df_csv.columns and not df_csv["SerialNumber"].empty:
        first_sn = df_csv["SerialNumber"].iloc[0]
        if pd.notna(first_sn) and str(first_sn).strip():
            sn = str(first_sn).strip()

    # Metadata tests — keep in output (for extract_metadata, not core analysis);
    # including non-numeric Value (hex, MAC) per metadata_config.
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
    """Read FW prodtest, bootloader, boardloader, hwid, touch versions from detail.log."""
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


def _parse_trezor_fatp_detail_log(file_path: str, station_name: str, content: str) -> List[dict]:
    """Parse Trezor FATP detail.log (batch output with bat_name, NPT64-diags /cvf, VALUE.000000)."""
    rows = []
    if "set bat_name=" not in content or "NPT64-diags.exe /cvf" not in content:
        return rows

    # SN and date from path: .../20260203/PCZ-PC1325/263928050000538_202602030629167/detail.log
    path_parts = file_path.replace("\\", "/").split("/")
    sn, raw_date = "Unknown", ""
    for part in path_parts:
        if "_" in part and part[0].isdigit():
            segs = part.split("_")
            if len(segs) >= 2 and len(segs[0]) >= 10:
                sn = segs[0]
                if len(segs[1]) >= 14:
                    ts = segs[1]
                    raw_date = f"{ts[:4]}-{ts[4:6]}-{ts[6:8]} {ts[8:10]}:{ts[10:12]}:{ts[12:14]}"
                elif len(segs[1]) >= 8:
                    raw_date = f"{segs[1][:4]}-{segs[1][4:6]}-{segs[1][6:8]}"
            break
    for part in path_parts:
        if len(part) == 8 and part.isdigit():
            raw_date = f"{part[:4]}-{part[4:6]}-{part[6:8]}" if not raw_date else raw_date
            break
    try:
        date_parsed = pd.to_datetime(raw_date) if raw_date else None
    except Exception:
        date_parsed = raw_date

    lines = content.splitlines()
    pat_cvf = re.compile(r"NPT64-diags\.exe\s+/cvf\s+.*?log[\\/]([^.\\/]+)\.log\s+#")
    pat_value = re.compile(r"^([-\d.eE]+)\.000000\s+[<>=]")

    i = 0
    while i < len(lines):
        line = lines[i]
        m = pat_cvf.search(line)
        if m:
            test_name = m.group(1)
            if i + 1 < len(lines):
                next_line = lines[i + 1].strip()
                vm = pat_value.match(next_line)
                if vm:
                    val = vm.group(1)
                    rows.append(
                        {
                            "Station": station_name,
                            "TestName": test_name,
                            "ValueRaw": val,
                            "SN": sn,
                            "Date": date_parsed,
                            "RawDate": raw_date,
                            "Unit": "",
                            "File": os.path.basename(file_path),
                        }
                    )
            i += 2
            continue
        i += 1

    return rows


def _parse_log_file(file_path: str, station_name: str) -> List[dict]:
    """Parse .log file and return records in the same shape as CSV rows."""
    rows = []
    encodings = ["utf-8", "utf-8-sig", "latin-1", "cp1250"]
    content = None
    for enc in encodings:
        try:
            with open(file_path, "r", encoding=enc) as f:
                content = f.read()
            break
        except (UnicodeDecodeError, OSError):
            continue
    if not content:
        return rows

    # 0. Trezor FATP detail.log format
    fatp_rows = _parse_trezor_fatp_detail_log(file_path, station_name, content)
    if fatp_rows:
        return fatp_rows

    # 1. Try as CSV (some .log files are CSV-shaped)
    from io import StringIO

    for delim in [",", "\t", ";", "|"]:
        try:
            df = pd.read_csv(StringIO(content), sep=delim, engine="python")
            if all(c in df.columns for c in ["TestName", "Value", "SerialNumber"]):
                has_units = "Units" in df.columns
                if "StartDateTime" in df.columns:
                    date_col = "StartDateTime"
                else:
                    date_col = next(
                        (c for c in df.columns if "date" in str(c).lower() or "time" in str(c).lower()),
                        None,
                    )
                for r in df.itertuples(index=False):
                    unit = str(getattr(r, "Units", "") or "") if has_units and pd.notna(getattr(r, "Units", None)) else ""
                    raw_dt_val = str(getattr(r, date_col, "") or "") if date_col else ""
                    try:
                        dt = pd.to_datetime(raw_dt_val) if raw_dt_val else None
                    except Exception:
                        dt = raw_dt_val
                    rows.append(
                        {
                            "Station": station_name,
                            "TestName": str(getattr(r, "TestName", "")).strip(),
                            "ValueRaw": getattr(r, "Value", ""),
                            "SN": str(getattr(r, "SerialNumber", "")),
                            "Date": dt,
                            "RawDate": raw_dt_val,
                            "Unit": unit,
                            "File": os.path.basename(file_path),
                        }
                    )
                return rows
        except Exception:
            continue

    # 2. Line-by-line parse (key=value, TestName: value, etc.)
    # TestName=..., Value=..., SerialNumber=..., StartDateTime=...
    pat_kv = re.compile(
        r"(?:TestName|test)\s*[=:]\s*([^\s,;|]+).*?(?:Value|value)\s*[=:]\s*([\d.\-eE]+).*?(?:SerialNumber|SN|Serial)\s*[=:]\s*([^\s,;|]+)",
        re.IGNORECASE,
    )
    pat_simple = re.compile(r"([A-Za-z0-9_\-\.]+)\s+([\d.\-eE]+)\s+([A-Za-z0-9_\-]+)")
    dt_pat = re.compile(
        r"(\d{4}[-/]\d{2}[-/]\d{2}[\sT]\d{2}:\d{2}:\d{2}|\d{2}[-/]\d{2}[-/]\d{4}\s+\d{2}:\d{2}:\d{2})"
    )

    for line in content.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        m = pat_kv.search(line)
        if m:
            test_name, val, sn = m.group(1), m.group(2), m.group(3)
            dt_match = dt_pat.search(line)
            raw_dt = dt_match.group(1) if dt_match else ""
            try:
                date_parsed = pd.to_datetime(raw_dt) if raw_dt else None
            except Exception:
                date_parsed = raw_dt
            rows.append(
                {
                    "Station": station_name,
                    "TestName": test_name.strip(),
                    "ValueRaw": val,
                    "SN": sn.strip(),
                    "Date": date_parsed,
                    "RawDate": raw_dt,
                    "Unit": "",
                    "File": os.path.basename(file_path),
                }
            )
            continue
        m = pat_simple.search(line)
        if m:
            test_name, val, sn = m.group(1), m.group(2), m.group(3)
            if re.match(r"^[\d.\-eE]+$", val) and len(sn) >= 3:
                rows.append(
                    {
                        "Station": station_name,
                        "TestName": test_name,
                        "ValueRaw": val,
                        "SN": sn,
                        "Date": None,
                        "RawDate": "",
                        "Unit": "",
                        "File": os.path.basename(file_path),
                    }
                )
    return rows


LOADING_LABELS = {"CZ": "Loading files", "EN": "Loading files"}


def _extract_ymd_from_path(path: str) -> Optional[int]:
    """
    Find a path component matching YYYYMMDD (e.g. .../202602/20260205/ or .../20260205.zip)
    and return it as int YYYYMMDD. Returns None if not found.
    """
    parts = path.replace("\\", "/").split("/")
    for part in parts:
        base = os.path.splitext(part)[0]
        if len(base) == 8 and base.isdigit():
            try:
                return int(base)
            except ValueError:
                continue
    return None


def _render_loading_overlay(placeholder, progress: float, current: int, total: int, label: str = "Loading files") -> None:
    """Render loading progress overlay."""
    pct = min(100, int(round(progress * 100)))
    total_safe = max(1, total)
    html = f"""
    <div class="loading-overlay">
        <div class="loading-box">
            <h3>{label}</h3>
            <div class="loading-progress-wrapper">
                <div class="loading-progress-bar" style="width: {pct}%"></div>
            </div>
            <p class="loading-status">{current} / {total_safe}</p>
        </div>
    </div>
    """
    placeholder.markdown(html, unsafe_allow_html=True)


# Timeout: if no progress (new rows) for 60s after first successful read, stop loading
NO_PROGRESS_TIMEOUT_SEC = 60


class LoadTimeoutError(Exception):
    """Raised when loading adds no new data for longer than NO_PROGRESS_TIMEOUT_SEC."""
    pass


def _write_debug_log(debug_log_path: Optional[str], msg: str, **kwargs) -> None:
    """Append a line to debug log when path is set."""
    if not debug_log_path:
        return
    try:
        from datetime import datetime
        os.makedirs(os.path.dirname(debug_log_path), exist_ok=True)
        extra = " " + " ".join(f"{k}={v!r}" for k, v in kwargs.items()) if kwargs else ""
        line = f"{datetime.now().isoformat()} | load_data | {msg}{extra}\n"
        with open(debug_log_path, "a", encoding="utf-8") as f:
            f.write(line)
    except Exception:
        pass


def _load_data_impl(
    path: str,
    date_from_ymd: Optional[int] = None,
    date_to_ymd: Optional[int] = None,
    debug_log_path: Optional[str] = None,
    lang: str = "EN",
) -> pd.DataFrame:
    """
    Load data: primarily CSV (Pega), fallback detail.log.
    CSV: TestName, Value, Units (Q), LowerLimit (R), UpperLimit (S), skip Retry Times.
    detail.log: FW/bootloader/boardloader/hwid/touch versions for metadata.
    """
    data_list: List[dict] = []
    processed_folders = set()
    csv_pattern = re.compile(r"^\d+_\d+\.csv$")  # SN_timestamp.csv
    t0 = time.perf_counter()

    _write_debug_log(debug_log_path, "START", path=path, date_from=date_from_ymd, date_to=date_to_ymd)

    # Single tree walk: collect CSV folders and detail.log paths
    csv_roots: List[tuple] = []
    detail_files: List[str] = []
    for root, _, files in os.walk(path):
        if date_from_ymd is not None or date_to_ymd is not None:
            ymd = _extract_ymd_from_path(root)
            if ymd is not None:
                if date_from_ymd is not None and ymd < date_from_ymd:
                    continue
                if date_to_ymd is not None and ymd > date_to_ymd:
                    continue
        csv_files = [f for f in files if csv_pattern.match(f)]
        if csv_files:
            has_detail = "detail.log" in files
            station_name = "Unknown"
            for part in root.split(os.sep):
                if part.upper().startswith("PCZ-PC"):
                    station_name = part.upper()
                    break
            csv_roots.append((root, csv_files, has_detail, station_name))
        if "detail.log" in files:
            detail_files.append(os.path.join(root, "detail.log"))

    class _NoopPlaceholder:
        def markdown(self, *args, **kwargs): pass
        def empty(self): pass
    overlay_placeholder = st.empty() if st is not None else _NoopPlaceholder()
    total = len(csv_roots)
    label = LOADING_LABELS.get(
        (st.session_state.get("_lang", "EN") if st is not None else lang),
        "Loading files",
    )

    try:
        if total > 0:
            _render_loading_overlay(overlay_placeholder, 0.0, 0, total, label)
        _write_debug_log(debug_log_path, "csv_roots_collected", total=total)

        progress_interval = max(1, min(50, total // 30))  # refresh overlay often (~every 50 folders) so progress is visible
        # With debug log, write progress more often (every 20 folders) so loading activity is visible
        debug_progress_interval = 20 if debug_log_path else progress_interval
        last_progress_time = t0  # last time new data arrived; 60s without progress → timeout
        with ThreadPoolExecutor(max_workers=1) as executor:
            for idx, (root, csv_files, has_detail, station_name) in enumerate(csv_roots):
                if time.perf_counter() - last_progress_time > NO_PROGRESS_TIMEOUT_SEC:
                    _write_debug_log(debug_log_path, "timeout", phase="csv", elapsed_sec=round(time.perf_counter() - t0, 1))
                    raise LoadTimeoutError()
                if total > 0 and (idx % progress_interval == 0 or idx == total - 1):
                    _render_loading_overlay(overlay_placeholder, (idx + 1) / total, idx + 1, total, label)
                    if debug_log_path and (idx % max(1, debug_progress_interval) == 0 or idx == total - 1):
                        _write_debug_log(debug_log_path, "progress", current=idx + 1, total=total, rows_so_far=len(data_list))
                    if idx > 0 and idx % 100 == 0:
                        time.sleep(0.02)  # brief yield for UI refresh
                for csv_file in csv_files:
                    if root in processed_folders:
                        continue
                    if time.perf_counter() - last_progress_time > NO_PROGRESS_TIMEOUT_SEC:
                        _write_debug_log(debug_log_path, "timeout", phase="csv", elapsed_sec=round(time.perf_counter() - t0, 1))
                        raise LoadTimeoutError()
                    try:
                        csv_path = os.path.join(root, csv_file)
                        future = executor.submit(
                            _process_one_csv_folder_worker,
                            csv_path, root, has_detail, station_name,
                        )
                        try:
                            rows = future.result(timeout=NO_PROGRESS_TIMEOUT_SEC)
                        except FuturesTimeoutError:
                            _write_debug_log(debug_log_path, "timeout", phase="csv_file", path=csv_path, elapsed_sec=NO_PROGRESS_TIMEOUT_SEC)
                            raise LoadTimeoutError()
                        if rows:
                            processed_folders.add(root)
                            data_list.extend(rows)
                            last_progress_time = time.perf_counter()
                    except LoadTimeoutError:
                        raise
                    except Exception as e:
                        _write_debug_log(debug_log_path, "csv_error", path=csv_path, error=str(e))
                        continue

        csv_duration_sec = round(time.perf_counter() - t0, 2)
        _write_debug_log(
            debug_log_path,
            "csv_phase_done",
            folders_processed=len(processed_folders),
            rows=len(data_list),
            duration_sec=csv_duration_sec,
        )

        # Always parse detail.log (FATP logs) even when CSV data already exists.
        # Previously detail.log ran only when no CSV was found, so FATP stations
        # (FATPFINAL, FATPRF, FATPSUB, ...) were missing when Pega CSV (e.g. PCBAFCT) lived in the same folder.
        data_files = detail_files  # collected in the same tree walk above
        total = len(data_files)

        if total > 0:
            _render_loading_overlay(overlay_placeholder, 0.0, 0, total, label)
        _write_debug_log(debug_log_path, "detail_phase_start", total_files=total)

        progress_interval_detail = max(1, min(100, total // 20))
        last_progress_time = time.perf_counter()
        for idx, file_path in enumerate(data_files):
            if time.perf_counter() - last_progress_time > NO_PROGRESS_TIMEOUT_SEC:
                _write_debug_log(
                    debug_log_path,
                    "timeout",
                    phase="detail",
                    elapsed_sec=round(time.perf_counter() - t0, 1),
                )
                raise LoadTimeoutError()
            if total > 0 and (idx % progress_interval_detail == 0 or idx == total - 1):
                progress = (idx + 1) / total
                _render_loading_overlay(overlay_placeholder, progress, idx + 1, total, label)
                if idx > 0 and idx % 100 == 0:
                    time.sleep(0.02)
            try:

                def _read_one_detail_file() -> list:
                    station_name = "Unknown"
                    for part in file_path.split(os.sep):
                        if part.upper().startswith("PCZ-PC"):
                            station_name = part.upper()
                            break
                    log_rows = _parse_log_file(file_path, station_name)
                    folder = os.path.dirname(file_path)
                    versions = _extract_detail_log_versions(file_path)
                    for r in log_rows:
                        r["LowerLimit"] = None
                        r["UpperLimit"] = None
                        r["_folder"] = folder
                        r.update(versions)
                    return log_rows

                with ThreadPoolExecutor(max_workers=1) as ex:
                    future = ex.submit(_read_one_detail_file)
                    log_rows = future.result(timeout=NO_PROGRESS_TIMEOUT_SEC)
                data_list.extend(log_rows)
                last_progress_time = time.perf_counter()
            except FuturesTimeoutError:
                _write_debug_log(
                    debug_log_path,
                    "timeout",
                    phase="detail_file",
                    path=file_path,
                    elapsed_sec=NO_PROGRESS_TIMEOUT_SEC,
                )
                raise LoadTimeoutError()
            except Exception as e:
                _write_debug_log(
                    debug_log_path,
                    "detail_file_error",
                    path=file_path,
                    error=str(e),
                )
                continue

        _write_debug_log(
            debug_log_path,
            "detail_phase_done",
            files_processed=len(data_files),
            rows=len(data_list),
        )
    finally:
        overlay_placeholder.empty()

    duration_sec = round(time.perf_counter() - t0, 2)
    _write_debug_log(debug_log_path, "END", rows=len(data_list), duration_sec=duration_sec)
    df_out = pd.DataFrame(data_list)
    if not df_out.empty:
        df_out["Value"] = pd.to_numeric(df_out["ValueRaw"], errors="coerce")
        if "LowerLimit" not in df_out.columns:
            df_out["LowerLimit"] = None
        if "UpperLimit" not in df_out.columns:
            df_out["UpperLimit"] = None
    return df_out


def load_data(
    path: str,
    date_from_ymd: Optional[int] = None,
    date_to_ymd: Optional[int] = None,
    debug_log_path: Optional[str] = None,
    lang: str = "EN",
) -> pd.DataFrame:
    """
    Load data: primarily CSV (Pega), fallback detail.log.
    Without Streamlit, pass lang for loading label text.
    """
    if st is not None:
        return _load_data_cached(path, date_from_ymd, date_to_ymd, debug_log_path, lang)
    return _load_data_impl(path, date_from_ymd, date_to_ymd, debug_log_path, lang)


if st is not None:
    _load_data_cached = st.cache_data(show_spinner=False, ttl=3600)(
        lambda path, date_from_ymd=None, date_to_ymd=None, debug_log_path=None, lang="EN": _load_data_impl(
            path, date_from_ymd, date_to_ymd, debug_log_path, lang
        )
    )
else:
    _load_data_cached = _load_data_impl

