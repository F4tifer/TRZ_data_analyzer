"""
Provisioning error code mapping from t3w1_provisioning (0 and NULL = no error).

Descriptions are loaded from data/provisioning_error_codes.json (generated from the Trezor wiki
via scripts/parse_provisioning_wiki_codes.py when a local wiki clone is available).
"""
from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd

_JSON_PATH = Path(__file__).resolve().parent.parent / "data" / "provisioning_error_codes.json"

_codes_cache: Optional[Dict[str, str]] = None


def _load_codes() -> Dict[str, str]:
    global _codes_cache
    if _codes_cache is not None:
        return _codes_cache
    _codes_cache = {}
    if _JSON_PATH.is_file():
        try:
            with open(_JSON_PATH, encoding="utf-8") as f:
                data = json.load(f)
            raw = data.get("codes") or {}
            _codes_cache = {str(k).strip(): str(v).strip() for k, v in raw.items() if str(v).strip()}
        except (OSError, json.JSONDecodeError, TypeError, ValueError):
            _codes_cache = {}
    return _codes_cache


def reload_provisioning_codes() -> Dict[str, str]:
    """Reload JSON from disk (e.g. after manual edit or script run)."""
    global _codes_cache
    _codes_cache = None
    return _load_codes()


def _finite_number(value: Any) -> Optional[float]:
    """float(value) if finite; else None (inf, nan, non-numeric)."""
    try:
        x = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(x):
        return None
    return x


def describe_provisioning_error(value: Any) -> Optional[str]:
    """Description only for error codes (≠ 0, not NULL); otherwise None."""
    if is_provisioning_no_error(value):
        return None
    codes = _load_codes()
    if not codes:
        return None
    key = _value_to_code_key(value)
    if key is None:
        return None
    return codes.get(key)


def _value_to_code_key(value: Any) -> Optional[str]:
    if isinstance(value, str):
        s = value.strip()
        if not s or s.upper() in ("NULL", "NONE"):
            return None
        x = _finite_number(s)
    else:
        x = _finite_number(value)
    if x is None:
        return None
    try:
        return str(int(x))
    except (OverflowError, ValueError):
        return None


def is_provisioning_no_error(value: Any) -> bool:
    """0, NULL, empty string and the string 'NULL' are not error codes."""
    if pd.isna(value):
        return True
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return True
        if s.upper() in ("NULL", "NONE"):
            return True
        x = _finite_number(s)
        if x is None:
            return False
        return int(x) == 0
    x = _finite_number(value)
    if x is None:
        return False
    try:
        return int(x) == 0
    except (OverflowError, ValueError):
        return False


def evaluate_provisioning_status(value: Any) -> str:
    """OK if 0/NULL; NOK if non-zero numeric code; N/A if not evaluable."""
    if pd.isna(value):
        return "OK"
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return "OK"
        if s.upper() in ("NULL", "NONE"):
            return "OK"
        x = _finite_number(s)
        if x is None:
            return "N/A"
        try:
            v = int(x)
        except (OverflowError, ValueError):
            return "N/A"
        return "OK" if v == 0 else "NOK"
    x = _finite_number(value)
    if x is None:
        return "N/A"
    try:
        v = int(x)
    except (OverflowError, ValueError):
        return "N/A"
    return "OK" if v == 0 else "NOK"
