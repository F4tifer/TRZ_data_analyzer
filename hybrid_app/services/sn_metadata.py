"""
SN-based metadata lookup: same grouping logic as log_analyzer_streamlit (per tester / folder / DB source).
"""
from __future__ import annotations

import os
from typing import Any

import pandas as pd

from config_loader import extract_metadata_from_df, load_metadata_config
from hybrid_app.schemas import RunResult
from hybrid_app.services.detail_analysis import load_run_dataframe

# Labels aligned with Streamlit meta_front
_LBL_STATION = "Station / tester"
_LBL_SOURCE = "Tester type"
_LBL_OPERATION = "Station kind (Operation)"
_LBL_DATETIME = "Date / time"
_LBL_DEVICE_SN = "Device SN (device_sn)"


def _non_empty_str(value: Any) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    if not s or s.lower() in ("nan", "none"):
        return None
    return s


def _merge_metadata(meta_front: dict[str, Any], meta_extra: dict[str, Any], run: RunResult) -> dict[str, str]:
    """
    Merge metadata similarly to Streamlit detail view, but keep a safe fallback from run summary metadata.
    Priority: front fields -> SN-extracted fields -> run summary metadata (missing keys only).
    """
    merged: dict[str, str] = {}
    for src in (meta_front, meta_extra):
        for key, value in (src or {}).items():
            v = _non_empty_str(value)
            if v is not None:
                merged[str(key)] = v
    fallback = (run.summary.metadata if run.summary else {}) or {}
    for key, value in fallback.items():
        if str(key) in merged:
            continue
        v = _non_empty_str(value)
        if v is not None:
            merged[str(key)] = v
    return merged


def _sn_search_mask(df: pd.DataFrame, sn_search: str) -> pd.Series:
    """Match production SN (SN/device_sn_man) or device_sn from Device table (MySQL T3W1)."""
    q = (sn_search or "").strip()
    if not q:
        return pd.Series(False, index=df.index)
    m = df["SN"].astype(str).str.contains(q, case=False, na=False)
    if "device_sn" in df.columns:
        m = m | df["device_sn"].astype(str).str.contains(q, case=False, na=False)
    return m


def _meta_front_from_row(df_sub: pd.DataFrame) -> dict[str, str]:
    """Front fields like Streamlit _render_meta_block (first row)."""
    if df_sub.empty:
        return {}
    row0 = df_sub.iloc[0]
    out: dict[str, str] = {}
    if "Station" in df_sub.columns and pd.notna(row0.get("Station")):
        out[_LBL_STATION] = str(row0["Station"]).strip()
    if "Source" in df_sub.columns and pd.notna(row0.get("Source")):
        out.setdefault(_LBL_SOURCE, str(row0["Source"]).strip())
    if "Operation" in df_sub.columns and pd.notna(row0.get("Operation")) and str(row0.get("Operation", "")).strip():
        out[_LBL_OPERATION] = str(row0["Operation"]).strip()
    dt_val = row0.get("RawDate") or row0.get("Date")
    if pd.notna(dt_val) and str(dt_val).strip():
        out[_LBL_DATETIME] = str(dt_val).strip()
    if "device_sn" in df_sub.columns and pd.notna(row0.get("device_sn")):
        ds = str(row0["device_sn"]).strip()
        if ds and ds.lower() not in ("nan", "none"):
            out[_LBL_DEVICE_SN] = ds
    return out


def collect_sn_metadata_blocks(
    run_id: str,
    run: RunResult,
    sn_search: str,
) -> tuple[list[dict[str, Any]], str | None, str | None]:
    """
    Returns:
      - blocks: [{ "label": str, "meta": {key: val} }, ...]
      - sn_display: first matching SN string for title
      - error: user-facing error or None
    """
    sn_search = (sn_search or "").strip()
    if not sn_search:
        return [], None, "Enter serial number (SN) and press Search."

    df = load_run_dataframe(run_id, run.request)
    if df.empty:
        return [], None, "No data for this run (artifact missing or folder empty)."
    if "SN" not in df.columns:
        return [], None, "SN column is missing in data."

    df_sn = df[_sn_search_mask(df, sn_search)]
    if df_sn.empty:
        return [], None, f"No records found in loaded data for '{sn_search}'."

    sn_display = str(df_sn["SN"].iloc[0]).strip()
    meta_cfg = load_metadata_config()

    blocks: list[dict[str, Any]] = []
    path = run.request.path or ""

    # DB (SQL): groups by Source + Station — like Streamlit
    if "Source" in df_sn.columns:
        for (src_name, sta), df_sub in df_sn.groupby(["Source", "Station"], dropna=False):
            src_label = {
                "T3W1": "PCBA",
                "fatpsub": "SUB-ASSY",
                "fatprf": "RF BOX",
                "fatpfinal": "FINAL",
            }.get(str(src_name), str(src_name) or "—")
            sta_label = str(sta).strip() if sta is not None and str(sta).strip() else "—"
            lbl = f"{src_label} — {sta_label}"
            meta_extra = extract_metadata_from_df(df_sub, meta_cfg, log_folder_path=None) or {}
            meta_front = _meta_front_from_row(df_sub)
            merged = _merge_metadata(meta_front, meta_extra, run)
            blocks.append({"label": lbl, "meta": merged})
        return blocks, sn_display, None

    # Local / FTP: groups by _folder
    if "_folder" in df_sn.columns:
        folders = [str(f) for f in df_sn["_folder"].dropna().unique().tolist()]
        if not folders:
            df_sub = df_sn
            log_folder_sn = path if path else None
            meta_extra = extract_metadata_from_df(df_sub, meta_cfg, log_folder_path=log_folder_sn) or {}
            meta_front = _meta_front_from_row(df_sub)
            blocks.append(
                {
                    "label": "Full SN slice (no folder)",
                    "meta": _merge_metadata(meta_front, meta_extra, run),
                }
            )
            return blocks, sn_display, None
        for folder in folders:
            df_sub = df_sn[df_sn["_folder"].astype(str) == str(folder)]
            if df_sub.empty:
                continue
            row0 = df_sub.iloc[0]
            op = (
                str(row0.get("Operation", "")).strip()
                if "Operation" in df_sub.columns and pd.notna(row0.get("Operation"))
                else ""
            )
            sta = (
                str(row0.get("Station", "")).strip()
                if "Station" in df_sub.columns and pd.notna(row0.get("Station"))
                else ""
            )
            lbl = ((op or "—") + " — " + (sta or "—")).strip(" —").strip()
            if not lbl:
                lbl = str(folder).split(os.sep)[-1] if folder else f"#{len(blocks) + 1}"
            meta_extra = extract_metadata_from_df(df_sub, meta_cfg, log_folder_path=folder) or {}
            meta_front = _meta_front_from_row(df_sub)
            merged = _merge_metadata(meta_front, meta_extra, run)
            blocks.append({"label": lbl, "meta": merged})
        return blocks, sn_display, None

    # Single blob: one folder = run path
    df_sub = df_sn
    log_folder_sn = path if path else None
    meta_extra = extract_metadata_from_df(df_sub, meta_cfg, log_folder_path=log_folder_sn) or {}
    meta_front = _meta_front_from_row(df_sub)
    merged = _merge_metadata(meta_front, meta_extra, run)
    blocks.append({"label": "Full loaded run", "meta": merged})
    return blocks, sn_display, None


def collect_sn_rows_for_export(
    run_id: str,
    run: RunResult,
    sn_search: str,
) -> tuple[pd.DataFrame | None, str | None, str | None]:
    """
    Returns:
      - dataframe filtered to requested SN (all rows/tests/results)
      - sn_display: first matching SN for filename
      - error: user-facing error or None
    """
    sn_search = (sn_search or "").strip()
    if not sn_search:
        return None, None, "Enter serial number (SN) and press Search."

    df = load_run_dataframe(run_id, run.request)
    if df.empty:
        return None, None, "No data for this run (artifact missing or folder empty)."
    if "SN" not in df.columns:
        return None, None, "SN column is missing in data."

    df_sn = df[_sn_search_mask(df, sn_search)].copy()
    if df_sn.empty:
        return None, None, f"No records found in loaded data for '{sn_search}'."

    sn_display = str(df_sn["SN"].iloc[0]).strip()
    return df_sn, sn_display, None
