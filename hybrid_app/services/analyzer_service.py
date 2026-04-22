from __future__ import annotations

from typing import Any

import pandas as pd
import plotly.express as px

from app.constants import STATION_COLORS
from app.core_services import apply_theme, evaluate_status
from config_loader import extract_metadata_from_df, get_limits_from_tests_config, load_tests_config
from data_loader import load_data
from hybrid_app.schemas import (
    MANUFACTURING_DEFAULT_BASE_URL,
    MANUFACTURING_DEFAULT_MYSQL_DB,
    RunRequest,
    RunSummary,
)
from hybrid_app.services.artifact_store import save_run_df


def _ymd_to_sql_start(ymd: int | None) -> str | None:
    if ymd is None:
        return None
    s = str(ymd).zfill(8)
    return f"{s[:4]}-{s[4:6]}-{s[6:8]} 00:00:00"


def _ymd_to_sql_end(ymd: int | None) -> str | None:
    if ymd is None:
        return None
    s = str(ymd).zfill(8)
    return f"{s[:4]}-{s[4:6]}-{s[6:8]} 23:59:59"


def _load_mysql_dataframe(request: RunRequest, password: str) -> pd.DataFrame:
    from app import db_search

    profile = getattr(request, "db_profile", None) or "pegatron"
    if profile == "manufacturing":
        default_db = MANUFACTURING_DEFAULT_MYSQL_DB
        default_url = MANUFACTURING_DEFAULT_BASE_URL
    else:
        default_db = "pegatron-db"
        default_url = "https://pegatron-db.corp.sldev.cz/"
    base = (request.db_base_url or "").strip() or default_url
    db_search.configure_connection(
        base,
        request.db_username or "",
        password,
        default_db=default_db,
        db_profile=profile,
    )
    try:
        row_limit = int(getattr(request, "mysql_row_limit", None) or 500_000)
        row_limit = max(1, min(row_limit, 20_000_000))
        df = db_search.search_generic(
            date_from=_ymd_to_sql_start(request.date_from_ymd),
            date_to=_ymd_to_sql_end(request.date_to_ymd),
            limit=row_limit,
        )
    finally:
        db_search.clear_connection()
    return df


def _build_default_limits(df: pd.DataFrame) -> dict[str, tuple[float, float]]:
    cfg = load_tests_config()
    limits: dict[str, tuple[float, float]] = {}
    for test_name in sorted(df["TestName"].dropna().unique().tolist()):
        lo, hi, _unit = get_limits_from_tests_config(test_name, cfg)
        limits[test_name] = (float(lo), float(hi))
    return limits


def _unit_group_columns(df: pd.DataFrame) -> list[str]:
    cols: list[str] = []
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


def _build_unit_status_frame(df: pd.DataFrame) -> pd.DataFrame:
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
    out = unit_status.reset_index(name="UnitStatus")
    return out


def run_analysis(
    run_id: str,
    request: RunRequest,
    *,
    db_password: str | None = None,
) -> tuple[RunSummary, list[str], dict[str, Any]]:
    if request.data_source == "mysql":
        if not db_password:
            raise ValueError("Missing password for database connection.")
        df = _load_mysql_dataframe(request, db_password)
        if not df.empty and "Value" in df.columns:
            df = df.copy()
            if "ValueRaw" not in df.columns:
                df["ValueRaw"] = df["Value"].map(
                    lambda x: str(x).strip() if pd.notna(x) and str(x).strip() not in ("", "nan") else ""
                )
            df["Value"] = pd.to_numeric(df["Value"], errors="coerce")
    else:
        load_path = (
            request.upload_extracted_path
            if request.data_source == "upload" and (request.upload_extracted_path or "").strip()
            else request.path
        )
        df = load_data(
            load_path,
            date_from_ymd=request.date_from_ymd,
            date_to_ymd=request.date_to_ymd,
            lang="EN",
        )
    if df.empty:
        raise ValueError("No data found for selected path/date range.")

    # Cache raw dataframe so later "recompute" doesn't re-read all logs again.
    save_run_df(run_id, df)

    limits = _build_default_limits(df)
    df = df.copy()
    df["Status"] = df.apply(lambda row: evaluate_status(row, limits), axis=1)

    unit_df = _build_unit_status_frame(df)
    total_rows = int(len(unit_df))
    ok_rows = int((unit_df["UnitStatus"] == "OK").sum()) if not unit_df.empty else 0
    nok_rows = int((unit_df["UnitStatus"] == "NOK").sum()) if not unit_df.empty else 0
    yield_pct = (ok_rows / total_rows * 100.0) if total_rows else 0.0

    tests = sorted(df["TestName"].dropna().astype(str).unique().tolist())
    stations = sorted(df["Station"].dropna().astype(str).unique().tolist()) if "Station" in df.columns else []

    warning_messages: list[str] = []
    if request.date_from_ymd or request.date_to_ymd:
        warning_messages.append("Date filtering is active; make sure station clocks are synchronized.")

    top_failures = (
        df[df["Status"] == "NOK"]["TestName"].value_counts().head(10).to_dict()
        if nok_rows
        else {}
    )

    log_folder = (
        request.upload_extracted_path
        if request.data_source == "upload" and (request.upload_extracted_path or "").strip()
        else request.path
    )
    metadata = {
        str(k): str(v)
        for k, v in extract_metadata_from_df(df, log_folder_path=log_folder).items()
        if v is not None
    }

    summary = RunSummary(
        total_rows=total_rows,
        ok_rows=ok_rows,
        nok_rows=nok_rows,
        yield_pct=round(yield_pct, 2),
        tests=tests,
        stations=stations,
        top_failures={str(k): int(v) for k, v in top_failures.items()},
        metadata=metadata,
    )
    # Charts for detail view (Pareto + station distribution)
    charts: dict[str, str] = {}
    failures = df[df["Status"] == "NOK"]
    if not failures.empty:
        fail_counts = failures["TestName"].value_counts().reset_index()
        fail_counts.columns = ["Test", "Count"]
        fig_pareto = px.bar(
            fail_counts,
            x="Count",
            y="Test",
            orientation="h",
            title="Pareto of failures",
            color="Count",
            color_continuous_scale=["#EF4444", "#F97316", "#F59E0B", "#EAB308"],
        )
        apply_theme(fig_pareto)
        charts["pareto"] = fig_pareto.to_json()

        if "Station" in failures.columns:
            fig_pie = px.pie(
                failures,
                names="Station",
                title="Failure distribution by station",
                color_discrete_sequence=STATION_COLORS,
            )
            apply_theme(fig_pie)
            charts["pie"] = fig_pie.to_json()

    summary.charts = charts
    payload = {"top_failures": summary.top_failures, "charts": charts, "metadata": metadata}
    return summary, warning_messages, payload
