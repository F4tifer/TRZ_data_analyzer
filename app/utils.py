"""
UI-independent helper functions: date (YYYYMMDD), filename extraction, safe key, units, logo.
"""
import os
import re
import base64
import hashlib
from typing import Optional

import pandas as pd


def parse_date_ymd_str(s: Optional[str]) -> Optional[int]:
    """Convert a YYYYMMDD string to int YYYYMMDD, otherwise None."""
    if not s:
        return None
    s = str(s).strip()
    if not s:
        return None
    if re.fullmatch(r"\d{8}", s):
        try:
            return int(s)
        except ValueError:
            return None
    return None


def extract_ymd_from_name(name: str) -> Optional[int]:
    """
    Extract a YYYYMMDD part from a file/folder name.
    Supports e.g.: 20251016, 20251016.zip, 20251016.tgz, 20251016.tar.gz
    """
    base = name
    lower = name.lower()
    if lower.endswith(".tar.gz"):
        base = name[:-7]
    elif lower.endswith(".tgz"):
        base = name[:-4]
    else:
        base = os.path.splitext(name)[0]
    if len(base) == 8 and base.isdigit():
        try:
            return int(base)
        except ValueError:
            return None
    return None


def safe_key(s: str) -> str:
    """Generate a safe hash key for Streamlit widgets."""
    return hashlib.md5(s.encode()).hexdigest()


def get_unit(df: pd.DataFrame, test_name: str) -> str:
    """Get the most frequent unit for the given test."""
    sub = df[df["TestName"] == test_name]
    if sub.empty:
        return ""
    if "Unit" in sub.columns:
        return str(sub["Unit"].mode().iloc[0]) if not sub["Unit"].mode().empty else ""
    return ""


def load_trezor_logo_b64() -> str:
    """Load trezor-symbol-white-rgb.png (next to project root) and return base64 for inline <img>."""
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    logo_path = os.path.join(base_dir, "trezor-symbol-white-rgb.png")
    try:
        with open(logo_path, "rb") as f:
            return base64.b64encode(f.read()).decode("utf-8")
    except Exception:
        return ""
