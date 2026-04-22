"""
Extra Plotly charts for hybrid run detail: histogram, time trend, correlation (Streamlit parity).
"""
from __future__ import annotations

import pandas as pd
import plotly.express as px

from app.constants import STATION_COLORS
from app.core_services import apply_theme, evaluate_status
from hybrid_app.schemas import RunResult
from hybrid_app.services.detail_analysis import compute_limits_for_selection, filter_selection_dataframe


def build_extra_charts_from_dataframe(
    df_main: pd.DataFrame,
    selected_tests: list[str],
    limits_map: dict[str, tuple[float, float]],
    chart_kind: str,
    param: str,
    corr_x: str,
    corr_y: str,
) -> dict:
    """
    Same outputs as build_extra_charts_context, but uses an already-filtered dataframe + limits
    (e.g. merged multi-run analysis).
    """
    kind = (chart_kind or "hist").lower().strip()
    if kind == "hist":
        return _histogram(df_main, param, limits_map, selected_tests)
    if kind == "trend":
        return _trend(df_main, param, limits_map, selected_tests)
    if kind == "corr":
        return _correlation(df_main, corr_x, corr_y, limits_map, selected_tests)
    if kind in ("scatter", "scatter_stations"):
        return _scatter_stations(df_main, limits_map, selected_tests)
    return {"error": f"Unknown chart type: {chart_kind!r}.", "scatter_charts": None}


def build_extra_charts_context(
    run_id: str,
    run: RunResult,
    tests: list[str],
    stations: list[str],
    chart_kind: str,
    param: str,
    corr_x: str,
    corr_y: str,
    custom_limits: dict[str, tuple[float, float]] | None = None,
) -> dict:
    """
    Returns template context: error, chart_json (optional), pearson (optional), hist_stats (optional).
    """
    df_main, selected_tests, err = filter_selection_dataframe(run_id, run, tests, stations)
    if err:
        return {"error": err, "scatter_charts": None}

    limits_map = compute_limits_for_selection(df_main, selected_tests, custom_limits=custom_limits)
    return build_extra_charts_from_dataframe(
        df_main, selected_tests, limits_map, chart_kind, param, corr_x, corr_y
    )


def _pick_param(param: str, selected_tests: list[str]) -> str | None:
    p = (param or "").strip()
    if p and p in selected_tests:
        return p
    if selected_tests:
        return selected_tests[0]
    return None


def _histogram(
    df_main: pd.DataFrame,
    param: str,
    limits_map: dict[str, tuple[float, float]],
    selected_tests: list[str],
) -> dict:
    test = _pick_param(param, selected_tests)
    if not test:
        return {"error": "Select at least one test in the filter above.", "scatter_charts": None}
    hist_data = df_main[df_main["TestName"] == test]
    if hist_data.empty:
        return {"error": f"No data for test '{test}' in the current selection.", "scatter_charts": None}

    title = f"Distribution: {test}"
    if "Station" in hist_data.columns and hist_data["Station"].notna().any():
        fig = px.histogram(
            hist_data,
            x="Value",
            color="Station",
            barmode="overlay",
            nbins=50,
            title=title,
            color_discrete_sequence=STATION_COLORS,
        )
    else:
        fig = px.histogram(hist_data, x="Value", nbins=50, title=title)

    if test in limits_map:
        lo, hi = limits_map[test]
        fig.add_vline(x=lo, line_dash="dash", line_color="#EF4444", line_width=2, annotation_text="LSL")
        fig.add_vline(x=hi, line_dash="dash", line_color="#EF4444", line_width=2, annotation_text="USL")

    apply_theme(fig)
    values = pd.to_numeric(hist_data["Value"], errors="coerce").dropna()
    stats: dict[str, float] = {}
    if not values.empty:
        stats = {
            "mean": float(values.mean()),
            "std": float(values.std()) if len(values) > 1 else 0.0,
            "min": float(values.min()),
            "max": float(values.max()),
        }
    return {
        "error": None,
        "chart_json": fig.to_json(),
        "pearson": None,
        "hist_stats": stats,
        "scatter_charts": None,
    }


def _trend(
    df_main: pd.DataFrame,
    param: str,
    limits_map: dict[str, tuple[float, float]],
    selected_tests: list[str],
) -> dict:
    test = _pick_param(param, selected_tests)
    if not test:
        return {"error": "Select at least one test in the filter above.", "scatter_charts": None}
    trend_data = df_main[df_main["TestName"] == test].copy()
    if trend_data.empty:
        return {"error": f"No data for test '{test}' in the current selection.", "scatter_charts": None}

    if "Date" not in trend_data.columns:
        return {"error": "Date column is missing - cannot render trend chart.", "scatter_charts": None}

    trend_data["ParsedDate"] = pd.to_datetime(trend_data["Date"], errors="coerce")
    trend_data = trend_data.dropna(subset=["ParsedDate"]).sort_values("ParsedDate")
    if trend_data.empty:
        return {"error": "No rows left for trend chart after date parsing.", "scatter_charts": None}

    hover = ["SN"] if "SN" in trend_data.columns else []
    if "Station" in trend_data.columns:
        fig = px.scatter(
            trend_data,
            x="ParsedDate",
            y="Value",
            color="Station",
            title=f"Trend over time: {test}",
            hover_data=hover,
            color_discrete_sequence=STATION_COLORS,
        )
    else:
        fig = px.scatter(
            trend_data,
            x="ParsedDate",
            y="Value",
            title=f"Trend over time: {test}",
            hover_data=hover,
        )

    if test in limits_map:
        lo, hi = limits_map[test]
        fig.add_hline(y=lo, line_dash="dash", line_color="#EF4444", line_width=2, annotation_text="LSL")
        fig.add_hline(y=hi, line_dash="dash", line_color="#EF4444", line_width=2, annotation_text="USL")

    apply_theme(fig)
    return {
        "error": None,
        "chart_json": fig.to_json(),
        "pearson": None,
        "hist_stats": None,
        "scatter_charts": None,
    }


def _correlation(
    df_main: pd.DataFrame,
    corr_x: str,
    corr_y: str,
    limits_map: dict[str, tuple[float, float]],
    selected_tests: list[str],
) -> dict:
    x = (corr_x or "").strip()
    y = (corr_y or "").strip()
    if not x or not y:
        return {"error": "Select both tests for correlation (X and Y).", "scatter_charts": None}
    if x == y:
        return {"error": "Select two different tests for X and Y axis.", "scatter_charts": None}
    if x not in selected_tests or y not in selected_tests:
        return {
            "error": "Both tests must be present in selected tests (multi-select above).",
            "scatter_charts": None,
        }

    sub = df_main[df_main["TestName"].isin([x, y])]
    if sub.empty:
        return {"error": "No data found for selected tests.", "scatter_charts": None}

    try:
        pivot_idx = ["SN", "Station"]
        if "RunId" in sub.columns:
            pivot_idx = ["RunId", "SN", "Station"]
        pivot_data = sub.pivot_table(index=pivot_idx, columns="TestName", values="Value").dropna().reset_index()
    except Exception:
        pivot_data = pd.DataFrame()

    if pivot_data.empty or x not in pivot_data.columns or y not in pivot_data.columns:
        return {
            "error": "Not enough paired rows (SN + station) for both tests - try a different pair or broader selection.",
            "scatter_charts": None,
        }

    fig = px.scatter(
        pivot_data,
        x=x,
        y=y,
        color="Station" if "Station" in pivot_data.columns else None,
        title=f"Correlation: {x} vs {y}",
        color_discrete_sequence=STATION_COLORS,
    )

    if x in limits_map:
        lo, hi = limits_map[x]
        fig.add_vline(x=lo, line_dash="dot", line_color="rgba(239, 68, 68, 0.6)", line_width=2)
        fig.add_vline(x=hi, line_dash="dot", line_color="rgba(239, 68, 68, 0.6)", line_width=2)
    if y in limits_map:
        lo, hi = limits_map[y]
        fig.add_hline(y=lo, line_dash="dot", line_color="rgba(239, 68, 68, 0.6)", line_width=2)
        fig.add_hline(y=hi, line_dash="dot", line_color="rgba(239, 68, 68, 0.6)", line_width=2)

    apply_theme(fig)
    pearson = float(pivot_data[x].corr(pivot_data[y])) if len(pivot_data) > 1 else None
    return {
        "error": None,
        "chart_json": fig.to_json(),
        "pearson": pearson,
        "hist_stats": None,
        "scatter_charts": None,
    }


def _scatter_stations(
    df_main: pd.DataFrame,
    limits_map: dict[str, tuple[float, float]],
    selected_tests: list[str],
) -> dict:
    """Scatter index vs Value per station and selected test (Streamlit tab Scatter)."""
    if "Station" not in df_main.columns:
        return {"error": "Station column is missing in data.", "scatter_charts": None}

    df_work = df_main.copy()
    df_work["Status"] = df_work.apply(lambda row: evaluate_status(row, limits_map), axis=1)

    scatter_charts: list[dict[str, str]] = []
    for station in sorted(df_work["Station"].dropna().unique().tolist(), key=str):
        station_str = str(station)
        station_data = df_work[df_work["Station"] == station]
        for test in selected_tests:
            test_data = station_data[station_data["TestName"] == test]
            if test_data.empty:
                continue
            hover_cols = [c for c in ("SN", "RawDate") if c in test_data.columns]
            hover_kw = {"hover_data": hover_cols} if hover_cols else {}
            fig = px.scatter(
                test_data,
                x=list(range(len(test_data))),
                y="Value",
                color="Status",
                color_discrete_map={"OK": "#10B981", "NOK": "#EF4444", "N/A": "#6B7280"},
                title=f"{station_str} — {test}",
                height=350,
                **hover_kw,
            )
            if test in limits_map:
                lo, hi = limits_map[test]
                fig.add_hline(
                    y=lo,
                    line_dash="dash",
                    line_color="#EF4444",
                    line_width=2,
                    annotation_text="LSL",
                    annotation_position="right",
                )
                fig.add_hline(
                    y=hi,
                    line_dash="dash",
                    line_color="#EF4444",
                    line_width=2,
                    annotation_text="USL",
                    annotation_position="right",
                )
            apply_theme(fig)
            scatter_charts.append(
                {
                    "station": station_str,
                    "test": test,
                    "figure_json": fig.to_json(),
                }
            )

    if not scatter_charts:
        return {
            "error": "No scatter points available for selected filters (station x test).",
            "scatter_charts": None,
        }
    return {
        "error": None,
        "chart_json": None,
        "pearson": None,
        "hist_stats": None,
        "scatter_charts": scatter_charts,
    }
