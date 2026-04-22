import json
import os
import re
import sys
import pandas as pd
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple

def _config_dir() -> Path:
    """Directory containing config files (works with frozen PyInstaller too)."""
    if getattr(sys, "frozen", False):
        return Path(sys._MEIPASS)
    return Path(__file__).resolve().parent

TESTS_CONFIG_FILE = "tests_config.json"
META_CONFIG_FILE = "metadata_config.json"

def _normalize(name: str) -> str:
    """Normalize strings for comparison."""
    return str(name).strip().lower().replace(" ", "_")

def load_tests_config() -> Dict:
    """Load tests configuration."""
    path = _config_dir() / TESTS_CONFIG_FILE
    if path.exists():
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading tests config: {e}")
            return {}
    return {}

def get_metadata_test_names(config: Optional[Dict] = None) -> set:
    """Return TestName/CSV keys that are metadata-only (exclude from test selection)."""
    cfg = config or load_metadata_config()
    keys = set()
    for field in cfg.get("fields", []):
        if field.get("source") == "CSV" and field.get("key"):
            keys.add(field["key"])
    keys.update({"SerialNumber", "TSRID", "Station_ID", "Fixture_ID", "Test_Start_Time",
                 "Test_End_Time", "Operator", "TcsTestSuiteDuration", "OPID", "PartNumber",
                 "FW_Prodtest_Check", "Bootloader_Version_Check", "Boardloader_Version_Check",
                 "BLE_FW_Version", "Touch_Version_Check", "HWID_Check", "OTP_Batch_Read"})
    return keys


def load_metadata_config() -> Dict:
    """Load metadata configuration."""
    path = _config_dir() / META_CONFIG_FILE
    if path.exists():
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading metadata config: {e}")
            return {}
    return {}

def get_tests_options_from_config(data_columns: Optional[List[str]] = None, config: Optional[Dict] = None) -> List[str]:
    """
    Return tests defined in configuration.
    Accepts data_columns and config for compatibility with legacy callers.
    """
    if config is None:
        config = load_tests_config()

    options = []
    for category in config.get("tests", []):
        for test in category.get("tests", []):
            if "csv_key" in test:
                options.append(test["csv_key"])
            elif "test_name" in test:
                options.append(test["test_name"])
    
    unique_options = list(set(options))

    if data_columns:
        available_set = set(data_columns)
        filtered_options = [opt for opt in unique_options if opt in available_set]
        if filtered_options:
            return filtered_options

    return unique_options


def get_all_known_test_names_for_load(config: Optional[Dict] = None) -> List[str]:
    """
    Return all known test names from tests_config for selection before loading data.
    Metadata-only tests (get_metadata_test_names) are excluded. Sorted.
    """
    options = get_tests_options_from_config(data_columns=None, config=config)
    metadata_only = get_metadata_test_names()
    return sorted([t for t in options if t not in metadata_only])


def get_display_name_map(config: Optional[Dict] = None) -> Dict[str, str]:
    """
    Return csv_key -> display_name for UI.
    Priority: display_name > test_name > csv_key.
    Lets you override labels in tests_config.json (display_name field).
    """
    cfg = config or load_tests_config()
    result = {}
    for category in cfg.get("tests", []):
        for test in category.get("tests", []):
            raw = test.get("csv_key") or test.get("test_name")
            if not raw:
                continue
            display = test.get("display_name") or test.get("test_name") or raw
            result[raw] = str(display).strip()
    return result


def get_display_name(test_key: str, config: Optional[Dict] = None) -> str:
    """Return display name for csv_key/test_name."""
    display_map = get_display_name_map(config)
    return display_map.get(test_key, test_key)


def get_limits_from_tests_config(test_name: str, config: Optional[Dict] = None) -> Tuple[float, float, str]:
    """
    Return limits for a test as (min, max, unit).
    Format required for legacy variable expansion (min_spec, max_spec, unit).
    """
    if config is None:
        config = load_tests_config()
        
    norm_name = _normalize(test_name)
    
    for category in config.get("tests", []):
        for test in category.get("tests", []):
            # Match by csv_key or test_name
            is_match = False
            if "csv_key" in test and _normalize(test["csv_key"]) == norm_name:
                is_match = True
            elif "test_name" in test and _normalize(test["test_name"]) == norm_name:
                is_match = True
            
            if is_match:
                limits = test.get("limits", {})
                unit = test.get("unit", "")
                
                # Values with -inf/inf defaults for numeric comparison
                try:
                    min_val = float(limits.get("min", float("-inf")))
                except (ValueError, TypeError):
                    min_val = float("-inf")
                    
                try:
                    max_val = float(limits.get("max", float("inf")))
                except (ValueError, TypeError):
                    max_val = float("inf")
                
                return (min_val, max_val, unit)
                
    # If test not found, return open limits
    return (float("-inf"), float("inf"), "")

# DataFrame column -> field_name for detail.log versions
DETAIL_LOG_FIELD_MAP = {
    "FW_prodtest": "Firmware_Version",
    "Bootloader_version": "Bootloader_Version",
    "Boardloader_version": "Boardloader_Version",
    "HW_version": "Hardware_ID",
    "Touch_version": "Touch_Firmware_Version",
}

# Aliases: config key -> DataFrame column (data_loader uses different names)
COLUMN_ALIASES = {
    "SerialNumber": "SN",
    "Station_ID": "Station",
    "Test_Start_Time": "RawDate",
}

# Case-insensitive lookup for COLUMN_ALIASES
_COLUMN_ALIASES_CF: Dict[str, str] = {k.casefold(): v for k, v in COLUMN_ALIASES.items()}

# Test name aliases for metadata (local logs vs DB fatp*).
# Key = expected name from metadata_config (key),
# value = alternative TestName values in df (e.g. typos).
METADATA_TEST_ALIASES: Dict[str, List[str]] = {
    # Firmware prodtest version
    "FW_Prodtest_Check": ["FW_Podtest_Check", "FW_podtest_check", "fw_prodtest_check", "FW_Prodtest"],
    # Bootloader version
    "Bootloader_Version_Check": ["Bootloade_Vesio_Check", "bootloader_version_check", "Bootloader_Version"],
    # Boardloader version
    "Boardloader_Version_Check": ["Boadloade_Vesio_Check", "boardloader_version_check"],
    # Touch FW version
    "Touch_Version_Check": ["Touch_Vesio_Check", "touch_version_check", "Touch_FW_Version"],
    # BLE FW version
    "BLE_FW_Version": ["BLE_FW_Vesio", "ble_fw_version", "BLE_FW"],
    # HW / variants (DB often spells differently)
    "HWID_Check": ["HWID", "Hardware_ID", "hw_id_check", "hwid_check", "HW_Version", "HW_version"],
    "OTP_Batch_Read": ["OTP_Batch", "otp_batch_read", "OTP_Batch_Info"],
    "OTP_Variant_Info": ["OTP_Variant_Read", "otp_variant_read", "OTP_variant", "OTP_Variant"],
    "SerialNumber": ["serial_number", "Serial_Number", "device_sn_man"],
    "TSRID": ["tsrid", "TsrId"],
}

# Log file stem (casefold) → extra possible TestName (DB without log folder)
LOG_STEM_EXTRA_ALIASES: Dict[str, List[str]] = {
    "optiga_id_read": ["OPTIGA_ID_READ", "Optiga_ID", "OPTIGA_ID"],
    "tropic_id_read": ["Tropic_ID", "Tropic_ID_Read", "tropic_id"],
    "cpuid_read": ["CPU_ID", "CPU_ID_Read", "CPUID_READ"],
    "bt_mac_get": ["BLE_MAC_Address", "BLE_MAC", "BT_MAC_get"],
    "nfc_read": ["NFC_UID", "NFC_READ"],
    "otp_variant_read": ["OTP_Variant", "OTP_Variant_Read", "OTP_Variant_Info"],
}


def get_csv_parser_metadata_test_names(config: Optional[Dict] = None) -> set:
    """
    TestName rows that must not be dropped when parsing Pega CSV only because
    Value is non-numeric (hex ID, MAC, OTP variant, …).
    Derived from metadata_config (CSV keys + log stems + aliases).
    """
    names = set(get_metadata_test_names(config))
    for k in METADATA_TEST_ALIASES:
        names.add(k)
    for alts in METADATA_TEST_ALIASES.values():
        for a in alts:
            names.add(a)
    for alts in LOG_STEM_EXTRA_ALIASES.values():
        for a in alts:
            names.add(a)
    cfg = config or load_metadata_config()
    for field in cfg.get("fields", []):
        lf = field.get("log_file")
        if not lf:
            continue
        stem = Path(str(lf)).stem
        names.add(stem)
        for alt in LOG_STEM_EXTRA_ALIASES.get(stem.casefold(), []):
            names.add(alt)
    return names


def _normalize_alnum(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(s).strip().lower())


def _extract_value_by_testname_key(df: pd.DataFrame, key: str, use_value_raw: bool) -> Optional[Any]:
    """
    Find row by TestName / MeasurementName (case-insensitive, aliases, alnum match).
    Prefers latest Date when available.
    """
    key_norm = str(key).strip()
    if not key_norm:
        return None
    key_norm_cf = key_norm.casefold()
    n_key = _normalize_alnum(key_norm)

    if "TestName" not in df.columns:
        return None

    test_names = df["TestName"].astype(str).str.strip()
    test_cf = test_names.str.casefold()
    match = df[test_cf == key_norm_cf]

    if match.empty and key_norm in METADATA_TEST_ALIASES:
        for alt in METADATA_TEST_ALIASES[key_norm]:
            alt_cf = str(alt).strip().casefold()
            m = df[test_cf == alt_cf]
            if not m.empty:
                match = m
                break

    if match.empty:
        tn_alnum = test_names.map(_normalize_alnum)
        eq = tn_alnum == n_key
        if eq.any():
            match = df[eq]

    if match.empty and len(n_key) >= 6:
        tn_alnum = test_names.map(_normalize_alnum)
        sub = tn_alnum.map(lambda t: n_key in t or t in n_key)
        if sub.any():
            match = df[sub]

    if match.empty and "MeasurementName" in df.columns:
        mn = df["MeasurementName"].astype(str).str.strip().str.casefold()
        match = df[mn == key_norm_cf]
    if match.empty and "MeasurementName" in df.columns:
        mn_alnum = df["MeasurementName"].astype(str).map(_normalize_alnum)
        if (mn_alnum == n_key).any():
            match = df[mn_alnum == n_key]

    if match.empty:
        return None

    if "Date" in match.columns:
        try:
            match = match.sort_values("Date", ascending=False, na_position="last")
        except Exception:
            pass
    row = match.iloc[0]
    v = row.get("Value")
    if use_value_raw and (pd.isna(v) or str(v).strip() in ("", "nan")):
        v = row.get("ValueRaw")
    if pd.notna(v) and str(v).strip() and str(v).strip().lower() != "nan":
        return v
    return None


def _try_testname_keys(df: pd.DataFrame, keys: List[str], use_value_raw: bool) -> Optional[Any]:
    seen_cf = set()
    for k in keys:
        k = str(k).strip()
        if not k:
            continue
        cf = k.casefold()
        if cf in seen_cf:
            continue
        seen_cf.add(cf)
        v = _extract_value_by_testname_key(df, k, use_value_raw)
        if v is not None:
            return v
    return None


def _df_value_from_csv_key(df: pd.DataFrame, key: str, use_value_raw: bool) -> Optional[Any]:
    """Column / column alias / row by TestName for metadata key."""
    key_norm = str(key).strip()
    key_norm_cf = key_norm.casefold()
    col_map = {str(c).strip().casefold(): c for c in df.columns}

    if key_norm in df.columns:
        v = df.iloc[0][key_norm]
        if pd.notna(v) and str(v).strip():
            return v
    elif key_norm_cf in col_map:
        v = df.iloc[0][col_map[key_norm_cf]]
        if pd.notna(v) and str(v).strip():
            return v
    elif key_norm_cf in _COLUMN_ALIASES_CF:
        alias = _COLUMN_ALIASES_CF[key_norm_cf]
        if alias in df.columns:
            v = df.iloc[0][alias]
            if pd.notna(v) and str(v).strip():
                return v

    return _extract_value_by_testname_key(df, key_norm, use_value_raw)


def _find_log_file(log_folder: str, log_file: str) -> Optional[str]:
    """Find log file in folder or recursively in subfolders."""
    log_folder = Path(log_folder)
    if not log_folder.exists():
        return None
    log_lower = log_file.lower()
    # 1. Exact match in folder
    direct = log_folder / log_file
    if direct.exists():
        return str(direct)
    for f in log_folder.iterdir():
        if f.is_file() and f.name.lower() == log_lower:
            return str(f)
    # 2. Recursive search in subfolders
    try:
        for p in log_folder.rglob("*"):
            if p.is_file() and p.name.lower() == log_lower:
                return str(p)
    except OSError:
        pass
    return None


def extract_metadata_from_df(df: pd.DataFrame, config: Optional[Dict] = None, log_folder_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Extract metadata from df and optionally from log files.
    Priority: 1) columns from detail.log (data_loader adds them to rows), 2) metadata_config CSV/Log.
    Returns dict with field_name keys (e.g. Firmware_Version), order from metadata_config.
    """
    cfg = config or load_metadata_config()
    results = {}

    # 1) detail.log — data_loader adds FW_prodtest, Bootloader_version, ... to each row
    if df is not None and not df.empty:
        col_cf = {str(c).strip().casefold(): c for c in df.columns}
        for col, f_name in DETAIL_LOG_FIELD_MAP.items():
            actual = None
            if col in df.columns:
                actual = col
            elif col.casefold() in col_cf:
                actual = col_cf[col.casefold()]
            if actual is None:
                continue
            vals = df[actual].dropna().astype(str).unique()
            for v in vals:
                v = str(v).strip()
                if v and v.lower() not in ("nan", "none"):
                    results[f_name] = v
                    break

    fields = cfg.get("fields", [])
    use_value_raw = "ValueRaw" in df.columns if df is not None and not df.empty else False

    for field in fields:
        f_name = field.get("field_name")
        if f_name in results and results[f_name]:
            continue
        source = field.get("source", "CSV")
        key = field.get("key")

        # --- CSV sources ---
        if source == "CSV" and key and df is not None and not df.empty:
            val = _df_value_from_csv_key(df, str(key).strip(), use_value_raw)
            if val is not None:
                results[f_name] = str(val).strip()

        # --- Standalone .log files in folder + DF by stem (DB without logs folder) ---
        should_try_log = (source == "Log") or (
            field.get("fallback_source") == "Log" and (f_name not in results or not results[f_name])
        )
        if should_try_log:
            log_file = field.get("log_file")
            regex_pattern = field.get("regex")
            if log_folder_path and log_file and regex_pattern:
                target_path = _find_log_file(log_folder_path, log_file)
                if target_path:
                    try:
                        with open(target_path, "r", encoding="utf-8", errors="ignore") as f:
                            content = f.read()
                            m = re.search(regex_pattern, content, re.MULTILINE | re.IGNORECASE)
                            if m:
                                results[f_name] = (m.group(1) if m.groups() else m.group(0)).strip()
                    except Exception as e:
                        print(f"Warning: Failed to parse log {log_file} for field {f_name}: {e}")
            if df is not None and not df.empty and log_file:
                if f_name not in results or not results.get(f_name):
                    stem = Path(log_file).stem
                    keys_to_try = [stem]
                    keys_to_try.extend(LOG_STEM_EXTRA_ALIASES.get(stem.casefold(), []))
                    v = _try_testname_keys(df, keys_to_try, use_value_raw)
                    if v is not None:
                        results[f_name] = str(v).strip()

    # Order from metadata_config, filled fields only
    ordered = {}
    if fields:
        for field in fields:
            f_name = field.get("field_name")
            if f_name and f_name in results and results[f_name]:
                ordered[f_name] = results[f_name]
        for k, v in results.items():
            if k not in ordered:
                ordered[k] = v
        return ordered
    return results