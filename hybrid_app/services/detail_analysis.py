"""
Shared logic for detailed analysis: same selection, limits, and Status as Streamlit/Django dashboard.
Used by HTMX detail partial and CSV export.
"""
from __future__ import annotations

import pandas as pd

from app.core_services import evaluate_status
from config_loader import get_limits_from_tests_config, load_tests_config
from data_loader import load_data
from hybrid_app.services.artifact_store import load_run_df

from hybrid_app.schemas import RunRequest, RunResult


def load_run_dataframe(run_id: str, request: RunRequest) -> pd.DataFrame:
    """Load cached artifact or fall back to filesystem (not for mysql — credentials are not stored)."""
    try:
        return load_run_df(run_id)
    except Exception:
        if getattr(request, "data_source", None) == "mysql":
            return pd.DataFrame()
        return load_data(
            request.path,
            date_from_ymd=request.date_from_ymd,
            date_to_ymd=request.date_to_ymd,
            lang="EN",
        )


def dataframe_full_run_with_simple_limits(run_id: str, request: RunRequest) -> tuple[pd.DataFrame | None, str | None]:
    """
    Legacy export: all rows, limits from tests_config only (no saved_limits / spread fallback).
    Matches previous /export behavior before selection parity.
    """
    df = load_run_dataframe(run_id, request)
    if df.empty:
        return None, "No data for this run."
    cfg = load_tests_config()
    limits: dict[str, tuple[float, float]] = {}
    for test_name in sorted(df["TestName"].dropna().unique().tolist()):
        lo, hi, _u = get_limits_from_tests_config(test_name, cfg)
        limits[test_name] = (float(lo), float(hi))
    out = df.copy()
    out["Status"] = out.apply(lambda row: evaluate_status(row, limits), axis=1)
    return out, None


def filter_selection_dataframe(
    run_id: str,
    run: RunResult,
    tests: list[str],
    stations: list[str],
) -> tuple[pd.DataFrame | None, list[str], str | None]:
    """
    Apply test + station filters. Returns (df_main, selected_tests, error_message).
    """
    if not run.summary:
        return None, [], "Run is not ready yet."

    selected_tests = tests if tests else (run.summary.tests[:5] if run.summary.tests else [])
    selected_stations = stations if stations else (run.summary.stations if run.summary.stations else [])
    if not selected_tests:
        return None, [], "No tests available for selection."

    df_full = load_run_dataframe(run_id, run.request)
    if df_full.empty:
        return None, selected_tests, "No data for this run."

    df_main = df_full[df_full["TestName"].isin(selected_tests)].copy()
    if selected_stations and "Station" in df_main.columns:
        df_main = df_main[df_main["Station"].isin(selected_stations)]

    if df_main.empty:
        return None, selected_tests, "Selection returned no rows."

    return df_main, selected_tests, None


def compute_limits_for_selection(
    df_main: pd.DataFrame,
    selected_tests: list[str],
    custom_limits: dict[str, tuple[float, float]] | None = None,
) -> dict[str, tuple[float, float]]:
    """
    LSL/USL per test from tests_config + spread fallback.
    Optional custom_limits are per-request/per-run overrides (do not persist globally).
    """
    tests_cfg = load_tests_config()
    limits: dict[str, tuple[float, float]] = {}
    custom_limits = custom_limits or {}

    for test in selected_tests:
        if test in custom_limits:
            limits[test] = custom_limits[test]
            continue

        vals = df_main[df_main["TestName"] == test]["Value"].dropna()
        if vals.empty:
            limits[test] = (0.0, 100.0)
            continue

        lo, hi, _unit = get_limits_from_tests_config(test, tests_cfg)
        lo_f = float(lo)
        hi_f = float(hi)

        if lo_f == float("-inf"):
            lo_f = float(vals.min()) - 0.2 * (float(vals.max()) - float(vals.min()) or 1.0)
        if hi_f == float("inf"):
            hi_f = float(vals.max()) + 0.2 * (float(vals.max()) - float(vals.min()) or 1.0)

        if lo_f == float("-inf") or hi_f == float("inf"):
            mn = float(vals.min())
            mx = float(vals.max())
            span = (mx - mn) or 1.0
            lo_f = mn - 0.1 * span
            hi_f = mx + 0.1 * span

        limits[test] = (lo_f, hi_f)

    return limits


def dataframe_for_selection(
    run_id: str,
    run: RunResult,
    tests: list[str],
    stations: list[str],
    custom_limits: dict[str, tuple[float, float]] | None = None,
) -> tuple[pd.DataFrame | None, str | None]:
    """
    Filter by tests + stations and compute limits from tests_config (with optional run-scoped overrides).
    """
    df_main, selected_tests, err = filter_selection_dataframe(run_id, run, tests, stations)
    if err:
        return None, err

    limits = compute_limits_for_selection(df_main, selected_tests, custom_limits=custom_limits)
    df_main = df_main.copy()
    df_main["Status"] = df_main.apply(lambda row: evaluate_status(row, limits), axis=1)
    return df_main, None
