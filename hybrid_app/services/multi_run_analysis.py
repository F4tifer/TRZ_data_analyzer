"""
Merge multiple run artifacts for cross-DB / cross-source analytics (Phase 2).
"""
from __future__ import annotations

import pandas as pd

from app.constants import STATION_COLORS
from app.core_services import apply_theme, evaluate_status
from hybrid_app.services.detail_analysis import compute_limits_for_selection, load_run_dataframe
from hybrid_app.services.session_store import RunStore
import plotly.express as px


MAX_DEFAULT_TESTS = 50


def _unit_group_columns(df: pd.DataFrame) -> list[str]:
    cols: list[str] = []
    if "RunId" in df.columns:
        cols.append("RunId")
    if "SN" in df.columns:
        cols.append("SN")
    elif "device_sn" in df.columns:
        cols.append("device_sn")
    if "Station" in df.columns:
        cols.append("Station")
    if "RawDate" in df.columns:
        cols.append("RawDate")
    elif "Date" in df.columns:
        cols.append("Date")
    if "Source" in df.columns:
        cols.append("Source")
    if "Operation" in df.columns:
        cols.append("Operation")
    return cols


def build_kpi_units(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty or "Status" not in df.columns:
        return pd.DataFrame()

    work = df.copy()
    group_cols = _unit_group_columns(work)
    if group_cols:
        work["__unit_key"] = work[group_cols].fillna("").astype(str).agg("||".join, axis=1)
    else:
        work["__unit_key"] = work.index.astype(str)

    unit_status = work.groupby("__unit_key", sort=False)["Status"].apply(
        lambda s: "NOK" if (s.astype(str) == "NOK").any() else "OK"
    )
    unit_rows: list[dict[str, object]] = []
    for unit_key, group in work.groupby("__unit_key", sort=False):
        nok_rows = group[group["Status"].astype(str) == "NOK"]
        rep = nok_rows.iloc[0] if not nok_rows.empty else group.iloc[0]
        unit_rows.append(
            {
                "unit_key": str(unit_key),
                "run_id": str(rep.get("RunId", "") or ""),
                "sn": str(rep.get("SN", "") or ""),
                "station": str(rep.get("Station", "") or ""),
                "status": str(unit_status.get(unit_key, "OK")),
            }
        )
    return pd.DataFrame(unit_rows)


def merge_run_dataframes(store: RunStore, run_ids: list[str]) -> tuple[pd.DataFrame | None, list[str], str | None]:
    """Concatenate cached dataframes with RunId / source labels. Skips invalid or empty runs."""
    frames: list[pd.DataFrame] = []
    skipped: list[str] = []
    for rid in run_ids:
        rid = str(rid).strip()
        if not rid:
            continue
        run = store.get(rid)
        if run is None or not run.summary:
            skipped.append(rid)
            continue
        df = load_run_dataframe(rid, run.request)
        if df is None or df.empty:
            skipped.append(rid)
            continue
        d = df.copy()
        d["RunId"] = rid
        d["RunCreatedAt"] = str(run.created_at)
        d["DataSource"] = str(run.request.data_source or "")
        d["DbProfile"] = str(getattr(run.request, "db_profile", "") or "")
        d["PathOrDb"] = str(run.request.path or "")
        frames.append(d)
    if not frames:
        return None, skipped, "No data loaded from selected runs."
    merged = pd.concat(frames, ignore_index=True)
    return merged, skipped, None


def select_tests_stations_for_merged(
    merged: pd.DataFrame,
    tests: list[str],
    stations: list[str],
) -> tuple[list[str], list[str], bool, str | None]:
    """
    Pick tests/stations for merged analysis.
    If tests is empty: use up to MAX_DEFAULT_TESTS distinct TestNames (sorted).
    Returns (selected_tests, selected_stations, default_truncated, error).
    """
    if "TestName" not in merged.columns:
        return [], [], False, "Merged data has no TestName column."

    all_tests = sorted(merged["TestName"].dropna().astype(str).unique().tolist())
    default_truncated = False
    if tests:
        selected_tests = [t for t in tests if t in set(all_tests)]
        if not selected_tests:
            return [], [], False, "None of the requested tests exist in merged data."
    else:
        if len(all_tests) > MAX_DEFAULT_TESTS:
            default_truncated = True
        selected_tests = all_tests[:MAX_DEFAULT_TESTS]

    all_stations: list[str] = []
    if "Station" in merged.columns:
        all_stations = sorted(merged["Station"].dropna().astype(str).unique().tolist())

    if stations:
        station_set = set(all_stations)
        selected_stations = [s for s in stations if s in station_set]
    else:
        selected_stations = all_stations

    return selected_tests, selected_stations, default_truncated, None


def evaluate_merged_selection(
    merged: pd.DataFrame,
    selected_tests: list[str],
    selected_stations: list[str],
) -> tuple[pd.DataFrame | None, str | None]:
    """Filter merged frame by tests/stations and compute Status."""
    df_main = merged[merged["TestName"].isin(selected_tests)].copy()
    if selected_stations and "Station" in df_main.columns:
        df_main = df_main[df_main["Station"].isin(selected_stations)]

    if df_main.empty:
        return None, "Selection returned no rows."

    limits_map = compute_limits_for_selection(df_main, selected_tests)
    df_main["Status"] = df_main.apply(lambda row: evaluate_status(row, limits_map), axis=1)
    return df_main, None


def build_charts_for_merged(df_main: pd.DataFrame) -> dict[str, str]:
    charts: dict[str, str] = {}
    failures = df_main[df_main["Status"] == "NOK"]
    if failures.empty:
        return charts
    fail_counts = failures["TestName"].value_counts().reset_index()
    fail_counts.columns = ["Test", "Count"]
    fig_pareto = px.bar(
        fail_counts,
        x="Count",
        y="Test",
        orientation="h",
        title="Merged Pareto of failures (selected runs)",
        color="Count",
        color_continuous_scale=["#EF4444", "#F97316", "#F59E0B", "#EAB308"],
    )
    apply_theme(fig_pareto)
    charts["pareto"] = fig_pareto.to_json()

    if "Station" in failures.columns and not failures["Station"].isna().all():
        fig_pie = px.pie(
            failures,
            names="Station",
            title="Merged failure distribution by station",
            color_discrete_sequence=STATION_COLORS,
        )
        apply_theme(fig_pie)
        charts["pie"] = fig_pie.to_json()
    return charts


def run_merged_pipeline(
    store: RunStore,
    run_ids: list[str],
    tests: list[str],
    stations: list[str],
) -> tuple[
    pd.DataFrame | None,
    list[str],
    list[str],
    list[str],
    str | None,
    bool,
    list[str],
    list[str],
    list[str],
]:
    """
    Single merge + select + evaluate. Returns:
      df_main, selected_tests, selected_stations, skipped, error,
      default_tests_truncated, available_tests, available_stations, run_ids_clean
    """
    run_ids_clean = [str(x).strip() for x in run_ids if str(x).strip()]
    if not run_ids_clean:
        return None, [], [], [], "Select at least one run.", False, [], [], []

    merged, skipped, err = merge_run_dataframes(store, run_ids_clean)
    if err:
        return None, [], [], skipped, err, False, [], [], run_ids_clean

    if "TestName" not in merged.columns:
        return None, [], [], skipped, "Merged data has no TestName column.", False, [], [], run_ids_clean

    available_tests = sorted(merged["TestName"].dropna().astype(str).unique().tolist())
    available_stations: list[str] = []
    if "Station" in merged.columns:
        available_stations = sorted(merged["Station"].dropna().astype(str).unique().tolist())

    selected_tests, selected_stations, default_truncated, err2 = select_tests_stations_for_merged(merged, tests, stations)
    if err2:
        return None, [], [], skipped, err2, default_truncated, available_tests, available_stations, run_ids_clean

    df_main, err3 = evaluate_merged_selection(merged, selected_tests, selected_stations)
    if err3 or df_main is None:
        return (
            None,
            selected_tests,
            selected_stations,
            skipped,
            err3 or "No data to analyze.",
            default_truncated,
            available_tests,
            available_stations,
            run_ids_clean,
        )

    return (
        df_main,
        selected_tests,
        selected_stations,
        skipped,
        None,
        default_truncated,
        available_tests,
        available_stations,
        run_ids_clean,
    )


def load_merged_evaluated_dataframe(
    store: RunStore,
    run_ids: list[str],
    tests: list[str],
    stations: list[str],
) -> tuple[pd.DataFrame | None, list[str], list[str], list[str], str | None, bool]:
    """
    Returns (df_main, selected_tests, selected_stations, skipped_run_ids, error, default_tests_truncated).
    df_main includes Status and RunId columns.
    """
    df_main, st, ss, skipped, err, trunc, _at, _as, _rid = run_merged_pipeline(store, run_ids, tests, stations)
    return df_main, st, ss, skipped, err, trunc


def compute_merged_context(
    store: RunStore,
    run_ids: list[str],
    tests: list[str],
    stations: list[str],
) -> dict:
    """
    Template context for multi-run analysis page / partial.
    Keys: error, charts, KPI counts, limits_rows, selected_*, available_*, skipped_runs, default_tests_truncated, max_tests_cap.
    """
    run_ids_clean = [str(x).strip() for x in run_ids if str(x).strip()]
    if not run_ids_clean:
        return {
            "error": "Select at least one run.",
            "charts": {},
            "top_failures": {},
            "limits_rows": [],
            "skipped_runs": [],
            "default_tests_truncated": False,
            "max_tests_cap": MAX_DEFAULT_TESTS,
            "available_tests": [],
            "available_stations": [],
            "run_ids": [],
        }

    df_main, selected_tests, selected_stations, skipped, err, default_truncated, available_tests, available_stations, run_ids_clean = (
        run_merged_pipeline(store, run_ids_clean, tests, stations)
    )
    if err or df_main is None:
        return {
            "error": err or "No data to analyze.",
            "charts": {},
            "top_failures": {},
            "limits_rows": [],
            "skipped_runs": skipped,
            "default_tests_truncated": default_truncated,
            "max_tests_cap": MAX_DEFAULT_TESTS,
            "available_tests": available_tests,
            "available_stations": available_stations,
            "selected_tests": selected_tests,
            "selected_stations": selected_stations,
            "run_ids": run_ids_clean,
        }

    limits_map = compute_limits_for_selection(df_main, selected_tests)
    limits_rows = [{"test": t, "lo": limits_map[t][0], "hi": limits_map[t][1]} for t in selected_tests]

    units = build_kpi_units(df_main)
    total_count = int(len(units))
    ok_count = int((units["status"] == "OK").sum()) if not units.empty else 0
    nok_count = int((units["status"] == "NOK").sum()) if not units.empty else 0
    yield_pct = round((ok_count / total_count * 100.0) if total_count else 0.0, 2)

    failures = df_main[df_main["Status"] == "NOK"]
    top_failures: dict[str, int] = {}
    if not failures.empty:
        top_failures = {str(k): int(v) for k, v in failures["TestName"].value_counts().head(10).items()}

    charts = build_charts_for_merged(df_main)

    return {
        "error": None,
        "charts": charts,
        "top_failures": top_failures,
        "limits_rows": limits_rows,
        "total_count": total_count,
        "ok_count": ok_count,
        "nok_count": nok_count,
        "yield_pct": yield_pct,
        "selected_tests": selected_tests,
        "selected_stations": selected_stations,
        "available_tests": available_tests,
        "available_stations": available_stations,
        "skipped_runs": skipped,
        "default_tests_truncated": default_truncated,
        "max_tests_cap": MAX_DEFAULT_TESTS,
        "run_ids": run_ids_clean,
    }
