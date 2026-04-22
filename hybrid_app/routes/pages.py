from datetime import datetime
import pandas as pd
from fastapi import APIRouter, BackgroundTasks, File, Form, HTTPException, Query, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse, Response, StreamingResponse
from fastapi.templating import Jinja2Templates

from hybrid_app.schemas import (
    MANUFACTURING_DEFAULT_BASE_URL,
    RunRequest,
    mysql_display_path_from_base_url,
)
from hybrid_app.services.pending_db_password import stash as stash_db_password
from hybrid_app.services.analyzer_service import run_analysis
from hybrid_app.services.upload_archive import cleanup_uploaded_archive, extract_uploaded_archive
from hybrid_app.services.session_store import RunStore
from hybrid_app.services.detail_analysis import (
    compute_limits_for_selection,
    dataframe_for_selection,
    dataframe_full_run_with_simple_limits,
    filter_selection_dataframe,
    load_run_dataframe,
)
from hybrid_app.services.selection_facets import build_selection_facets
from hybrid_app.services.extra_charts import build_extra_charts_context, build_extra_charts_from_dataframe
from hybrid_app.services.sn_metadata import collect_sn_metadata_blocks, collect_sn_rows_for_export
from hybrid_app.services.multi_run_analysis import compute_merged_context, load_merged_evaluated_dataframe
from hybrid_app.services.mysql_sql_preview import build_mysql_load_sql_preview
from hybrid_app.routes.page_contracts import hx_trigger_header, normalize_query_values
from hybrid_app.routes.page_handlers.dashboard_auth import (
    dashboard_kpis_partial_page,
    dashboard_page,
    index_page,
    login_get_page,
    login_post_page,
    logout_page,
    runs_partial_page,
)
from app.constants import STATION_COLORS
from app.core_services import (
    apply_theme,
    calculate_ai_limits,
    evaluate_status,
)

import plotly.express as px


def _parse_limits_form(form) -> dict[str, tuple[float, float]]:
    out: dict[str, tuple[float, float]] = {}
    i = 0
    while True:
        t = form.get(f"limit_test_{i}")
        if t is None:
            break
        lo_s = form.get(f"limit_lsl_{i}")
        hi_s = form.get(f"limit_usl_{i}")
        if lo_s is not None and hi_s is not None and str(lo_s).strip() != "" and str(hi_s).strip() != "":
            try:
                out[str(t)] = (float(lo_s), float(hi_s))
            except (TypeError, ValueError):
                pass
        i += 1
    return out


def _parse_limit_test_names(form) -> list[str]:
    names: list[str] = []
    i = 0
    while True:
        t = form.get(f"limit_test_{i}")
        if t is None:
            break
        names.append(str(t))
        i += 1
    return names


_SESSION_LIMITS_KEY = "run_limits_overrides"


def _get_session_run_limits(request: Request, run_id: str) -> dict[str, tuple[float, float]]:
    raw = request.session.get(_SESSION_LIMITS_KEY, {})
    by_run = raw.get(str(run_id), {}) if isinstance(raw, dict) else {}
    out: dict[str, tuple[float, float]] = {}
    if not isinstance(by_run, dict):
        return out
    for test, pair in by_run.items():
        try:
            if isinstance(pair, (list, tuple)) and len(pair) == 2:
                out[str(test)] = (float(pair[0]), float(pair[1]))
        except (TypeError, ValueError):
            continue
    return out


def _set_session_run_limits(request: Request, run_id: str, limits: dict[str, tuple[float, float]]) -> None:
    raw = request.session.get(_SESSION_LIMITS_KEY, {})
    by_run: dict[str, dict[str, list[float]]]
    if isinstance(raw, dict):
        by_run = raw
    else:
        by_run = {}
    by_run[str(run_id)] = {str(t): [float(lo), float(hi)] for t, (lo, hi) in limits.items()}
    request.session[_SESSION_LIMITS_KEY] = by_run


def _reset_session_run_limits(request: Request, run_id: str, tests: list[str] | None = None) -> None:
    raw = request.session.get(_SESSION_LIMITS_KEY, {})
    if not isinstance(raw, dict):
        return
    key = str(run_id)
    current = raw.get(key)
    if not isinstance(current, dict):
        return
    if not tests:
        raw.pop(key, None)
        request.session[_SESSION_LIMITS_KEY] = raw
        return
    test_set = {str(t) for t in tests}
    for t in list(current.keys()):
        if str(t) in test_set:
            current.pop(t, None)
    if current:
        raw[key] = current
    else:
        raw.pop(key, None)
    request.session[_SESSION_LIMITS_KEY] = raw


def _resolve_pagination(page: int, page_size: int, total: int) -> tuple[int, int, int, int]:
    page_size = max(5, min(100, page_size))
    total_pages = max(1, (total + page_size - 1) // page_size) if total > 0 else 1
    page = min(max(1, page), total_pages)
    offset = (page - 1) * page_size
    return page, page_size, total_pages, offset


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


def _build_kpi_units(df: pd.DataFrame) -> pd.DataFrame:
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

    rows: list[dict[str, object]] = []
    for unit_key, group in work.groupby("__unit_key", sort=False):
        nok_rows = group[group["Status"].astype(str) == "NOK"]
        rep = nok_rows.iloc[0] if not nok_rows.empty else group.iloc[0]
        error_desc = str(rep.get("ProvisioningErrorDescription", "") or "").strip()
        error_info = ""
        if error_desc:
            error_info = f"{rep.get('Value')} - {error_desc}"
        rows.append(
            {
                "unit_key": str(unit_key),
                "run_id": str(rep.get("RunId", "") or ""),
                "sn": str(rep.get("SN", "") or ""),
                "station": str(rep.get("Station", "") or ""),
                "test": str(rep.get("TestName", "") or ""),
                "value": rep.get("Value"),
                "status": str(unit_status.get(unit_key, "OK")),
                "error_info": error_info,
                "raw_date": str(rep.get("RawDate", "") or rep.get("Date", "") or ""),
            }
        )
    return pd.DataFrame(rows)


def _with_unit_key(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()
    group_cols = _unit_group_columns(work)
    if group_cols:
        work["__unit_key"] = work[group_cols].fillna("").astype(str).agg("||".join, axis=1)
    else:
        work["__unit_key"] = work.index.astype(str)
    return work


def _format_measurement_time(row: pd.Series) -> str:
    """Best-effort timestamp string for KPI drilldown rows (RawDate / Date / raw_date)."""
    if row is None or not isinstance(row, pd.Series) or row.index.size == 0:
        return ""
    for key in ("RawDate", "Date", "raw_date", "date"):
        if key not in row.index:
            continue
        val = row[key]
        if val is None:
            continue
        try:
            if pd.isna(val):
                continue
        except (TypeError, ValueError):
            pass
        if isinstance(val, pd.Timestamp):
            return val.strftime("%Y-%m-%d %H:%M:%S")
        if isinstance(val, datetime):
            return val.strftime("%Y-%m-%d %H:%M:%S")
        if hasattr(val, "strftime"):
            try:
                return val.strftime("%Y-%m-%d %H:%M:%S")
            except (AttributeError, TypeError, ValueError):
                pass
        s = str(val).strip()
        if s and s.lower() not in ("nan", "nat", "none", "null"):
            return s
    return ""


def _compute_full_run_unit_overview(run_id: str, run: RunResult) -> dict[str, float] | None:
    if not run.summary:
        return None
    try:
        df = load_run_dataframe(run_id, run.request)
        if df.empty:
            return None
        selected_tests = sorted(df["TestName"].dropna().astype(str).unique().tolist()) if "TestName" in df.columns else []
        if not selected_tests:
            return None
        limits_map = compute_limits_for_selection(df, selected_tests)
        df_eval = df.copy()
        df_eval["Status"] = df_eval.apply(lambda row: evaluate_status(row, limits_map), axis=1)
        units = _build_kpi_units(df_eval)
        total_count = int(len(units))
        ok_count = int((units["status"] == "OK").sum()) if not units.empty else 0
        nok_count = int((units["status"] == "NOK").sum()) if not units.empty else 0
        yield_pct = round((ok_count / total_count * 100.0) if total_count else 0.0, 2)
        return {
            "total_rows": total_count,
            "ok_rows": ok_count,
            "nok_rows": nok_count,
            "yield_pct": yield_pct,
        }
    except Exception:
        return None


def _run_job(run_id: str, request: RunRequest, store: RunStore) -> None:
    try:
        store.set_running(run_id)
        db_pw = None
        if request.data_source == "mysql":
            from hybrid_app.services.pending_db_password import pop as pop_db_password

            db_pw = pop_db_password(run_id)
        summary, warnings, extra = run_analysis(run_id, request, db_password=db_pw)
        store.set_success(run_id, summary, warnings, extra)
    except Exception as exc:  # noqa: BLE001
        store.set_error(run_id, str(exc))
    finally:
        if request.data_source == "upload":
            cleanup_uploaded_archive(request.upload_extracted_path)


def build_pages_router(store: RunStore, templates: Jinja2Templates) -> APIRouter:
    router = APIRouter(tags=["pages"])

    @router.get("/login", response_class=HTMLResponse)
    def login_get(request: Request) -> HTMLResponse:
        return login_get_page(request=request, templates=templates)

    @router.post("/login", response_model=None)
    async def login_post(
        request: Request,
        username: str = Form(...),
        password: str = Form(...),
    ) -> RedirectResponse | HTMLResponse:
        return login_post_page(
            request=request,
            templates=templates,
            username=username,
            password=password,
        )

    @router.post("/logout")
    def logout_route(request: Request) -> RedirectResponse:
        return logout_page(request=request)

    @router.get("/", response_class=HTMLResponse)
    def index(request: Request) -> HTMLResponse:
        return index_page(request=request, templates=templates, store=store)

    @router.get("/dashboard", response_class=HTMLResponse)
    def dashboard(
        request: Request,
        status: str = "all",
        q: str = "",
        page: int = 1,
        page_size: int = 20,
    ) -> HTMLResponse:
        return dashboard_page(
            request=request,
            templates=templates,
            store=store,
            status=status,
            q=q,
            page=page,
            page_size=page_size,
        )

    # Literal /runs/multi-* paths must be registered before /runs/{run_id}, otherwise
    # paths like /runs/multi-analysis are matched as run_id="multi-analysis".

    @router.get("/runs/multi-sn", response_class=HTMLResponse)
    def multi_run_sn_lookup(
        request: Request,
        run_ids: list[str] = Query(default=[]),
        sn: str = Query(default=""),
    ) -> HTMLResponse:
        run_ids_clean = normalize_query_values(run_ids)
        sn_q = (sn or "").strip()
        if not run_ids_clean:
            return templates.TemplateResponse(
                request=request,
                name="partials/multi_sn_results.html",
                context={
                    "error": "Select at least one run.",
                    "rows": [],
                    "sn": sn_q,
                    "selected_runs": 0,
                    "meta": {"selected_runs": 0, "matched_runs": 0, "truncated": False},
                },
                status_code=400,
            )
        if not sn_q:
            return templates.TemplateResponse(
                request=request,
                name="partials/multi_sn_results.html",
                context={
                    "error": "Enter SN to search.",
                    "rows": [],
                    "sn": "",
                    "selected_runs": len(run_ids_clean),
                    "meta": {"selected_runs": len(run_ids_clean), "matched_runs": 0, "truncated": False},
                },
                status_code=400,
            )

        preview_limit_per_run = 1000
        rows: list[dict[str, str]] = []
        skipped: list[str] = []
        truncated = False
        for rid in run_ids_clean:
            run = store.get(rid)
            if run is None or not run.summary:
                skipped.append(rid)
                continue
            df_sn, _sn_display, err = collect_sn_rows_for_export(rid, run, sn_q)
            if err or df_sn is None or df_sn.empty:
                continue
            if len(df_sn) > preview_limit_per_run:
                truncated = True
            for _, row in df_sn.head(preview_limit_per_run).iterrows():
                rows.append(
                    {
                        "run_id": rid,
                        "created_at": str(run.created_at),
                        "db_profile": str(getattr(run.request, "db_profile", "") or ""),
                        "source_type": str(getattr(run.request, "data_source", "") or ""),
                        "source_path": str(getattr(run.request, "path", "") or ""),
                        "station": str(row.get("Station", "") or ""),
                        "test": str(row.get("TestName", "") or ""),
                        "value": str(row.get("Value", "") or ""),
                        "status": str(row.get("Status", "") or ""),
                        "date": str(row.get("RawDate", "") or row.get("Date", "") or ""),
                    }
                )

        return templates.TemplateResponse(
            request=request,
            name="partials/multi_sn_results.html",
            context={
                "error": None if rows else "No matching SN rows found across selected runs.",
                "rows": rows,
                "sn": sn_q,
                "selected_runs": len(run_ids_clean),
                "matched_runs": len({r["run_id"] for r in rows}),
                "skipped_runs": skipped,
                "truncated": truncated,
                "meta": {
                    "selected_runs": len(run_ids_clean),
                    "matched_runs": len({r["run_id"] for r in rows}),
                    "truncated": truncated,
                },
            },
        )

    @router.get("/runs/multi-sn-export")
    def multi_run_sn_export(
        run_ids: list[str] = Query(default=[]),
        sn: str = Query(default=""),
    ) -> StreamingResponse:
        run_ids_clean = normalize_query_values(run_ids)
        sn_q = (sn or "").strip()
        if not run_ids_clean:
            raise HTTPException(status_code=400, detail="Select at least one run.")
        if not sn_q:
            raise HTTPException(status_code=400, detail="Enter SN to search.")

        export_rows: list[dict[str, str]] = []
        for rid in run_ids_clean:
            run = store.get(rid)
            if run is None or not run.summary:
                continue
            df_sn, _sn_display, err = collect_sn_rows_for_export(rid, run, sn_q)
            if err or df_sn is None or df_sn.empty:
                continue
            for _, row in df_sn.iterrows():
                export_rows.append(
                    {
                        "RunId": rid,
                        "RunCreatedAt": str(run.created_at),
                        "DataSource": str(getattr(run.request, "data_source", "") or ""),
                        "DbProfile": str(getattr(run.request, "db_profile", "") or ""),
                        "PathOrDb": str(getattr(run.request, "path", "") or ""),
                        "Station": str(row.get("Station", "") or ""),
                        "Source": str(row.get("Source", "") or ""),
                        "Operation": str(row.get("Operation", "") or ""),
                        "SN": str(row.get("SN", "") or ""),
                        "DeviceSN": str(row.get("device_sn", "") or ""),
                        "TestName": str(row.get("TestName", "") or ""),
                        "Value": str(row.get("Value", "") or ""),
                        "Status": str(row.get("Status", "") or ""),
                        "RawDate": str(row.get("RawDate", "") or ""),
                        "Date": str(row.get("Date", "") or ""),
                        "ProvisioningErrorDescription": str(row.get("ProvisioningErrorDescription", "") or ""),
                    }
                )
        if not export_rows:
            raise HTTPException(status_code=400, detail="No matching SN rows found across selected runs.")

        csv_bytes = pd.DataFrame(export_rows).to_csv(index=False).encode("utf-8")
        now = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        filename = f"Trezor_MultiRun_SN_{sn_q.replace(' ', '_')}_{now}.csv"
        return StreamingResponse(
            iter([csv_bytes]),
            media_type="text/csv; charset=utf-8",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    @router.get("/runs/multi-analysis", response_class=HTMLResponse)
    def multi_run_analysis_page(
        request: Request,
        run_ids: list[str] = Query(default=[]),
        tests: list[str] = Query(default=[]),
        stations: list[str] = Query(default=[]),
    ) -> HTMLResponse:
        ctx = compute_merged_context(
            store,
            normalize_query_values(run_ids),
            normalize_query_values(tests),
            normalize_query_values(stations),
        )
        return templates.TemplateResponse(
            request=request,
            name="multi_run_analysis.html",
            context=ctx,
        )

    @router.get("/runs/multi-kpi-sn-list", response_class=HTMLResponse)
    def multi_run_kpi_sn_list(
        request: Request,
        run_ids: list[str] = Query(default=[]),
        tests: list[str] = Query(default=[]),
        stations: list[str] = Query(default=[]),
        bucket: str = Query(default="total"),
        unit_key: str = Query(default=""),
    ) -> HTMLResponse:
        df_main, selected_tests, selected_stations, _skipped, err, _trunc = load_merged_evaluated_dataframe(
            store,
            normalize_query_values(run_ids),
            normalize_query_values(tests),
            normalize_query_values(stations),
        )
        if err or df_main is None:
            return templates.TemplateResponse(
                request=request,
                name="partials/multi_kpi_sn_list.html",
                context={
                    "error": err or "No data.",
                    "rows": [],
                    "bucket": bucket,
                    "title": "",
                    "total_rows": 0,
                    "selected_tests_count": 0,
                    "selected_stations_count": 0,
                    "truncated": False,
                    "meta": {
                        "bucket": bucket,
                        "total_rows": 0,
                        "selected_tests_count": 0,
                        "selected_stations_count": 0,
                        "truncated": False,
                    },
                },
                status_code=400,
            )

        units = _build_kpi_units(df_main)
        b = (bucket or "total").lower()
        if b == "ok":
            df_view = units[units["status"] == "OK"]
            title = "OK units (merged)"
        elif b == "nok":
            df_view = units[units["status"] == "NOK"]
            title = "NOK units (merged)"
        else:
            df_view = units
            title = "All units (merged)"

        unit_key_q = (unit_key or "").strip()
        detail_mode = bool(unit_key_q)
        max_rows = 500
        rows = []
        if detail_mode:
            detail_df = _with_unit_key(df_main)
            detail_df = detail_df[detail_df["__unit_key"].astype(str) == unit_key_q]
            for _, row in detail_df.head(max_rows).iterrows():
                error_desc = str(row.get("ProvisioningErrorDescription", "") or "").strip()
                error_info = ""
                if error_desc:
                    error_info = f"{row.get('Value')} - {error_desc}"
                rows.append(
                    {
                        "run_id": str(row.get("RunId", "") or ""),
                        "sn": str(row.get("SN", "") or ""),
                        "station": str(row.get("Station", "") or ""),
                        "test": str(row.get("TestName", "") or ""),
                        "time_display": _format_measurement_time(row),
                        "value": row.get("Value"),
                        "status": str(row.get("Status", "") or ""),
                        "error_info": error_info,
                        "unit_key": str(row.get("__unit_key", "") or ""),
                    }
                )
            title = f"{title} - unit detail"
        else:
            for _, row in df_view.head(max_rows).iterrows():
                rows.append(
                    {
                        "run_id": str(row.get("run_id", "") or ""),
                        "sn": str(row.get("sn", "") or ""),
                        "station": str(row.get("station", "") or ""),
                        "test": str(row.get("test", "") or ""),
                        "time_display": _format_measurement_time(row),
                        "value": row.get("value"),
                        "status": str(row.get("status", "") or ""),
                        "error_info": str(row.get("error_info", "") or ""),
                        "unit_key": str(row.get("unit_key", "") or ""),
                    }
                )

        return templates.TemplateResponse(
            request=request,
            name="partials/multi_kpi_sn_list.html",
            context={
                "error": None,
                "rows": rows,
                "bucket": b,
                "title": title,
                "total_rows": int(len(df_view)),
                "truncated": int(len(df_view)) > max_rows,
                "selected_tests_count": len(selected_tests),
                "selected_stations_count": len(selected_stations),
                "detail_mode": detail_mode,
                "meta": {
                    "bucket": b,
                    "total_rows": int(len(df_view)),
                    "selected_tests_count": len(selected_tests),
                    "selected_stations_count": len(selected_stations),
                    "truncated": int(len(df_view)) > max_rows,
                },
            },
        )

    @router.get("/runs/multi-failure-sn-list", response_class=HTMLResponse)
    def multi_run_failure_sn_list(
        request: Request,
        run_ids: list[str] = Query(default=[]),
        tests: list[str] = Query(default=[]),
        stations: list[str] = Query(default=[]),
        test: str = Query(default=""),
    ) -> HTMLResponse:
        if not (test or "").strip():
            return templates.TemplateResponse(
                request=request,
                name="partials/multi_failure_sn_list.html",
                context={
                    "error": "Missing test name.",
                    "rows": [],
                    "test": "",
                    "total_rows": 0,
                    "truncated": False,
                    "meta": {"test": "", "total_rows": 0, "truncated": False},
                },
                status_code=400,
            )

        df_main, _selected_tests, _selected_stations, _skipped, err, _trunc = load_merged_evaluated_dataframe(
            store,
            normalize_query_values(run_ids),
            normalize_query_values(tests),
            normalize_query_values(stations),
        )
        if err or df_main is None:
            return templates.TemplateResponse(
                request=request,
                name="partials/multi_failure_sn_list.html",
                context={
                    "error": err or "No data.",
                    "rows": [],
                    "test": str(test),
                    "total_rows": 0,
                    "truncated": False,
                    "meta": {"test": str(test), "total_rows": 0, "truncated": False},
                },
                status_code=400,
            )

        df_view = df_main[(df_main["Status"] == "NOK") & (df_main["TestName"].astype(str) == str(test))]
        max_rows = 500
        rows = []
        for _, row in df_view.head(max_rows).iterrows():
            error_desc = str(row.get("ProvisioningErrorDescription", "") or "").strip()
            error_info = ""
            if error_desc:
                error_info = f"{row.get('Value')} - {error_desc}"
            rows.append(
                {
                    "run_id": str(row.get("RunId", "") or ""),
                    "sn": str(row.get("SN", "") or ""),
                    "station": str(row.get("Station", "") or ""),
                    "value": row.get("Value"),
                    "status": str(row.get("Status", "") or ""),
                    "raw_date": str(row.get("RawDate", "") or row.get("Date", "") or ""),
                    "error_info": error_info,
                }
            )

        return templates.TemplateResponse(
            request=request,
            name="partials/multi_failure_sn_list.html",
            context={
                "error": None,
                "rows": rows,
                "test": str(test),
                "total_rows": int(len(df_view)),
                "truncated": int(len(df_view)) > max_rows,
                "meta": {
                    "test": str(test),
                    "total_rows": int(len(df_view)),
                    "truncated": int(len(df_view)) > max_rows,
                },
            },
        )

    @router.get("/runs/multi-analysis-export")
    def multi_run_analysis_export(
        run_ids: list[str] = Query(default=[]),
        tests: list[str] = Query(default=[]),
        stations: list[str] = Query(default=[]),
    ) -> StreamingResponse:
        df_main, _st, _ss, _sk, err, _tr = load_merged_evaluated_dataframe(
            store,
            normalize_query_values(run_ids),
            normalize_query_values(tests),
            normalize_query_values(stations),
        )
        if err or df_main is None:
            raise HTTPException(status_code=400, detail=err or "No data for export.")

        preferred_cols = [
            "RunId",
            "RunCreatedAt",
            "DataSource",
            "DbProfile",
            "PathOrDb",
            "Station",
            "Source",
            "Operation",
            "SN",
            "device_sn",
            "TestName",
            "Value",
            "ValueRaw",
            "Status",
            "RawDate",
            "Date",
            "Unit",
            "LowerLimit",
            "UpperLimit",
            "Origin",
            "ProvisioningErrorDescription",
        ]
        ordered_cols = [c for c in preferred_cols if c in df_main.columns]
        remaining_cols = [c for c in df_main.columns if c not in ordered_cols]
        export_cols = ordered_cols + remaining_cols

        csv_bytes = df_main[export_cols].to_csv(index=False).encode("utf-8")
        now = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        filename = f"Trezor_MultiRun_Analysis_{now}.csv"
        return StreamingResponse(
            iter([csv_bytes]),
            media_type="text/csv; charset=utf-8",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    @router.get("/runs/multi-charts-extra", response_class=HTMLResponse)
    def multi_run_charts_extra(
        request: Request,
        run_ids: list[str] = Query(default=[]),
        tests: list[str] = Query(default=[]),
        stations: list[str] = Query(default=[]),
        chart_kind: str = Query(default="hist"),
        param: str = Query(default=""),
        corr_x: str = Query(default=""),
        corr_y: str = Query(default=""),
    ) -> HTMLResponse:
        df_main, selected_tests, _selected_stations, _skipped, err, _trunc = load_merged_evaluated_dataframe(
            store,
            normalize_query_values(run_ids),
            normalize_query_values(tests),
            normalize_query_values(stations),
        )
        if err or df_main is None:
            return templates.TemplateResponse(
                request=request,
                name="partials/run_extra_charts.html",
                context={"error": err or "No data.", "scatter_charts": None},
                status_code=400,
            )
        limits_map = compute_limits_for_selection(df_main, selected_tests)
        ctx = build_extra_charts_from_dataframe(
            df_main,
            selected_tests,
            limits_map,
            chart_kind,
            param,
            corr_x,
            corr_y,
        )
        return templates.TemplateResponse(
            request=request,
            name="partials/run_extra_charts.html",
            context=ctx,
        )

    @router.get("/runs/{run_id}/mysql-sql", response_class=HTMLResponse)
    def run_mysql_sql_preview(
        request: Request,
        run_id: str,
        return_: str | None = Query(default=None, alias="return"),
    ) -> HTMLResponse:
        run = store.get(run_id)
        if run is None:
            raise HTTPException(status_code=404, detail="Run not found.")
        back_to_dashboard = return_ == "/dashboard"
        back_href = "/dashboard" if back_to_dashboard else f"/runs/{run_id}"
        back_label = "← Back to dashboard" if back_to_dashboard else "← Back to detail"
        if run.request.data_source != "mysql":
            return templates.TemplateResponse(
                request=request,
                name="mysql_sql.html",
                context={
                    "run_id": run_id,
                    "sql_text": "",
                    "not_mysql": True,
                    "back_href": back_href,
                    "back_label": back_label,
                },
            )
        text = build_mysql_load_sql_preview(run.request)
        return templates.TemplateResponse(
            request=request,
            name="mysql_sql.html",
            context={
                "run_id": run_id,
                "sql_text": text,
                "not_mysql": False,
                "back_href": back_href,
                "back_label": back_label,
            },
        )

    @router.get("/runs/{run_id}", response_class=HTMLResponse)
    def run_detail(request: Request, run_id: str) -> HTMLResponse:
        run = store.get(run_id)
        if run is None:
            return templates.TemplateResponse(
                request=request,
                name="run_detail.html",
                context={"run": None, "facets": None},
                status_code=404,
            )
        # Opening run detail starts from global defaults again (no persisted per-run user overrides).
        _reset_session_run_limits(request, run_id)
        facets: dict | None = None
        overview_summary: dict[str, float] | None = None
        if run.summary:
            try:
                df = load_run_dataframe(run_id, run.request)
                if not df.empty:
                    facets = build_selection_facets(df)
                    if not facets.get("kinds"):
                        facets = None
            except Exception:
                facets = None
            overview_summary = _compute_full_run_unit_overview(run_id, run)
        return templates.TemplateResponse(
            request=request,
            name="run_detail.html",
            context={"run": run, "facets": facets, "overview_summary": overview_summary},
        )

    @router.get("/runs/{run_id}/metadata-sn", response_class=HTMLResponse)
    def run_metadata_sn(
        request: Request,
        run_id: str,
        sn: str = Query(default=""),
    ) -> HTMLResponse:
        run = store.get(run_id)
        if run is None:
            return templates.TemplateResponse(
                request=request,
                name="partials/sn_metadata_results.html",
            context={"error": "Run does not exist.", "blocks": [], "sn_display": None},
                status_code=404,
            )
        if not run.summary:
            return templates.TemplateResponse(
                request=request,
                name="partials/sn_metadata_results.html",
                context={
                    "error": "Run analysis is not finished yet.",
                    "blocks": [],
                    "sn_display": None,
                },
            )
        blocks, sn_display, err = collect_sn_metadata_blocks(run_id, run, sn)
        return templates.TemplateResponse(
            request=request,
            name="partials/sn_metadata_results.html",
            context={
                "error": err,
                "blocks": blocks,
                "sn_display": sn_display,
                "sn_query": (sn or "").strip(),
                "run_id": run_id,
            },
        )

    @router.get("/runs/{run_id}/metadata-sn-export")
    def run_metadata_sn_export(
        run_id: str,
        sn: str = Query(default=""),
    ) -> StreamingResponse:
        run = store.get(run_id)
        if run is None or not run.summary:
            raise HTTPException(status_code=404, detail="Run not found or not finished.")
        sn_clean = (sn or "").strip()
        if not sn_clean:
            raise HTTPException(status_code=400, detail="Enter serial number (SN) and press Search.")

        try:
            df_sn, sn_display, err = collect_sn_rows_for_export(run_id, run, sn_clean)
        except Exception:
            raise HTTPException(
                status_code=400,
                detail="Unable to prepare SN export for the selected run.",
            ) from None
        if err:
            raise HTTPException(status_code=400, detail=err)
        if df_sn is None or df_sn.empty:
            raise HTTPException(status_code=400, detail="No rows found for selected SN.")

        preferred_cols = [
            "Station",
            "Source",
            "Operation",
            "SN",
            "device_sn",
            "TestName",
            "Value",
            "ValueRaw",
            "Status",
            "RawDate",
            "Date",
            "Unit",
            "LowerLimit",
            "UpperLimit",
            "Origin",
            "ProvisioningErrorDescription",
        ]
        ordered_cols = [c for c in preferred_cols if c in df_sn.columns]
        remaining_cols = [c for c in df_sn.columns if c not in ordered_cols]
        export_cols = ordered_cols + remaining_cols

        csv_bytes = df_sn[export_cols].to_csv(index=False).encode("utf-8")
        now = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        safe_sn = (sn_display or sn_clean or "sn").replace(" ", "_")
        filename = f"Trezor_SN_{safe_sn}_{now}.csv"
        return StreamingResponse(
            iter([csv_bytes]),
            media_type="text/csv; charset=utf-8",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    @router.get("/runs/{run_id}/detail", response_class=HTMLResponse)
    def run_detail_results(
        request: Request,
        run_id: str,
        tests: list[str] = Query(default=[]),
        stations: list[str] = Query(default=[]),
    ) -> HTMLResponse:
        try:
            tests = normalize_query_values(tests)
            stations = normalize_query_values(stations)
            run = store.get(run_id)
            if run is None:
                return templates.TemplateResponse(
                    request=request,
                    name="partials/run_detail_results.html",
                    context={"error": "Run not found."},
                    status_code=404,
                )

            if not run.summary:
                return templates.TemplateResponse(
                    request=request,
                    name="partials/run_detail_results.html",
                    context={"error": "Run is not ready yet."},
                    status_code=409,
                )

            df_main, selected_tests, err_msg = filter_selection_dataframe(run_id, run, tests, stations)
            if err_msg:
                return templates.TemplateResponse(
                    request=request,
                    name="partials/run_detail_results.html",
                    context={"error": err_msg},
                    status_code=400,
                )
            run_limits = _get_session_run_limits(request, run_id)
            limits_map = compute_limits_for_selection(df_main, selected_tests, custom_limits=run_limits)
            df_main = df_main.copy()
            df_main["Status"] = df_main.apply(lambda row: evaluate_status(row, limits_map), axis=1)
            limits_rows = [
                {"test": t, "lo": limits_map[t][0], "hi": limits_map[t][1]} for t in selected_tests
            ]
            units = _build_kpi_units(df_main)
            total_count = int(len(units))
            ok_count = int((units["status"] == "OK").sum()) if not units.empty else 0
            nok_count = int((units["status"] == "NOK").sum()) if not units.empty else 0
            yield_pct = round((ok_count / total_count * 100.0) if total_count else 0.0, 2)

            failures = df_main[df_main["Status"] == "NOK"]
            top_failures = (
                failures["TestName"].value_counts().head(10).to_dict() if not failures.empty else {}
            )
            top_failures = {str(k): int(v) for k, v in top_failures.items()}

            charts: dict[str, str] = {}
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

                if "Station" in failures.columns and not failures["Station"].isna().all():
                    fig_pie = px.pie(
                        failures,
                        names="Station",
                        title="Failure distribution by station",
                        color_discrete_sequence=STATION_COLORS,
                    )
                    apply_theme(fig_pie)
                    charts["pie"] = fig_pie.to_json()

            return templates.TemplateResponse(
                request=request,
                name="partials/run_detail_results.html",
                context={
                    "error": None,
                    "run_id": run_id,
                    "selected_tests": selected_tests,
                    "selected_stations": stations,
                    "limits_rows": limits_rows,
                    "total_count": total_count,
                    "ok_count": ok_count,
                    "nok_count": nok_count,
                    "yield_pct": yield_pct,
                    "top_failures": top_failures,
                    "charts": charts,
                },
            )
        except Exception as exc:  # noqa: BLE001
            return templates.TemplateResponse(
                request=request,
                name="partials/run_detail_results.html",
                context={"error": f"Internal error: {exc!s}"},
                status_code=500,
            )

    @router.get("/runs/{run_id}/analysis", response_class=HTMLResponse)
    def run_analysis_page(
        request: Request,
        run_id: str,
        tests: list[str] = Query(default=[]),
        stations: list[str] = Query(default=[]),
    ) -> HTMLResponse:
        try:
            tests = normalize_query_values(tests)
            stations = normalize_query_values(stations)
            run = store.get(run_id)
            if run is None:
                return templates.TemplateResponse(
                    request=request,
                    name="run_analysis.html",
                    context={"run": None, "error": "Run not found."},
                    status_code=404,
                )
            if not run.summary:
                return templates.TemplateResponse(
                    request=request,
                    name="run_analysis.html",
                    context={"run": run, "error": "Run is not ready yet."},
                    status_code=409,
                )

            df_main, selected_tests, err_msg = filter_selection_dataframe(run_id, run, tests, stations)
            if err_msg:
                return templates.TemplateResponse(
                    request=request,
                    name="run_analysis.html",
                    context={"run": run, "error": err_msg},
                    status_code=400,
                )
            run_limits = _get_session_run_limits(request, run_id)
            limits_map = compute_limits_for_selection(df_main, selected_tests, custom_limits=run_limits)
            df_main = df_main.copy()
            df_main["Status"] = df_main.apply(lambda row: evaluate_status(row, limits_map), axis=1)
            limits_rows = [{"test": t, "lo": limits_map[t][0], "hi": limits_map[t][1]} for t in selected_tests]
            units = _build_kpi_units(df_main)
            total_count = int(len(units))
            ok_count = int((units["status"] == "OK").sum()) if not units.empty else 0
            nok_count = int((units["status"] == "NOK").sum()) if not units.empty else 0
            yield_pct = round((ok_count / total_count * 100.0) if total_count else 0.0, 2)

            failures = df_main[df_main["Status"] == "NOK"]
            top_failures = failures["TestName"].value_counts().head(10).to_dict() if not failures.empty else {}
            top_failures = {str(k): int(v) for k, v in top_failures.items()}

            charts: dict[str, str] = {}
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
                if "Station" in failures.columns and not failures["Station"].isna().all():
                    fig_pie = px.pie(
                        failures,
                        names="Station",
                        title="Failure distribution by station",
                        color_discrete_sequence=STATION_COLORS,
                    )
                    apply_theme(fig_pie)
                    charts["pie"] = fig_pie.to_json()

            return templates.TemplateResponse(
                request=request,
                name="run_analysis.html",
                context={
                    "run": run,
                    "error": None,
                    "run_id": run_id,
                    "selected_tests": selected_tests,
                    "selected_stations": stations,
                    "limits_rows": limits_rows,
                    "total_count": total_count,
                    "ok_count": ok_count,
                    "nok_count": nok_count,
                    "yield_pct": yield_pct,
                    "top_failures": top_failures,
                    "charts": charts,
                },
            )
        except Exception as exc:  # noqa: BLE001
            return templates.TemplateResponse(
                request=request,
                name="run_analysis.html",
                context={"run": None, "error": f"Internal error: {exc!s}"},
                status_code=500,
            )

    @router.get("/runs/{run_id}/kpi-sn-list", response_class=HTMLResponse)
    def run_kpi_sn_list(
        request: Request,
        run_id: str,
        bucket: str = Query(default="total"),
        tests: list[str] = Query(default=[]),
        stations: list[str] = Query(default=[]),
        unit_key: str = Query(default=""),
    ) -> HTMLResponse:
        tests = normalize_query_values(tests)
        stations = normalize_query_values(stations)
        run = store.get(run_id)
        if run is None or not run.summary:
            return templates.TemplateResponse(
                request=request,
                name="partials/kpi_sn_list.html",
                context={
                    "error": "Run does not exist or is not finished yet.",
                    "rows": [],
                    "bucket": bucket,
                    "run_id": run_id,
                },
                status_code=404,
            )

        df_main, selected_tests, err_msg = filter_selection_dataframe(run_id, run, tests, stations)
        if err_msg:
            return templates.TemplateResponse(
                request=request,
                name="partials/kpi_sn_list.html",
                context={"error": err_msg, "rows": [], "bucket": bucket, "run_id": run_id},
                status_code=400,
            )

        run_limits = _get_session_run_limits(request, run_id)
        limits_map = compute_limits_for_selection(df_main, selected_tests, custom_limits=run_limits)
        df_main = df_main.copy()
        df_main["Status"] = df_main.apply(lambda row: evaluate_status(row, limits_map), axis=1)
        units = _build_kpi_units(df_main)

        b = (bucket or "total").lower()
        if b == "ok":
            df_view = units[units["status"] == "OK"]
            title = "OK units"
        elif b == "nok":
            df_view = units[units["status"] == "NOK"]
            title = "NOK units"
        else:
            df_view = units
            title = "All units"

        unit_key_q = (unit_key or "").strip()
        detail_mode = bool(unit_key_q)
        max_rows = 500
        rows = []
        if detail_mode:
            detail_df = _with_unit_key(df_main)
            detail_df = detail_df[detail_df["__unit_key"].astype(str) == unit_key_q]
            for _, row in detail_df.head(max_rows).iterrows():
                error_desc = str(row.get("ProvisioningErrorDescription", "") or "").strip()
                error_info = ""
                if error_desc:
                    error_info = f"{row.get('Value')} - {error_desc}"
                rows.append(
                    {
                        "sn": str(row.get("SN", "") or ""),
                        "station": str(row.get("Station", "") or ""),
                        "test": str(row.get("TestName", "") or ""),
                        "time_display": _format_measurement_time(row),
                        "value": row.get("Value"),
                        "status": str(row.get("Status", "") or ""),
                        "error_info": error_info,
                        "unit_key": str(row.get("__unit_key", "") or ""),
                    }
                )
            title = f"{title} - unit detail"
        else:
            for _, row in df_view.head(max_rows).iterrows():
                rows.append(
                    {
                        "sn": str(row.get("sn", "") or ""),
                        "station": str(row.get("station", "") or ""),
                        "test": str(row.get("test", "") or ""),
                        "time_display": _format_measurement_time(row),
                        "value": row.get("value"),
                        "status": str(row.get("status", "") or ""),
                        "error_info": str(row.get("error_info", "") or ""),
                        "unit_key": str(row.get("unit_key", "") or ""),
                    }
                )

        return templates.TemplateResponse(
            request=request,
            name="partials/kpi_sn_list.html",
            context={
                "error": None,
                "run_id": run_id,
                "rows": rows,
                "bucket": b,
                "title": title,
                "total_rows": int(len(df_view)),
                "truncated": int(len(df_view)) > max_rows,
                "selected_tests_count": len(selected_tests),
                "selected_stations_count": len(stations),
                "detail_mode": detail_mode,
            },
        )

    @router.get("/runs/{run_id}/failure-sn-list", response_class=HTMLResponse)
    def run_failure_sn_list(
        request: Request,
        run_id: str,
        test: str = Query(default=""),
        tests: list[str] = Query(default=[]),
        stations: list[str] = Query(default=[]),
    ) -> HTMLResponse:
        tests = normalize_query_values(tests)
        stations = normalize_query_values(stations)
        run = store.get(run_id)
        if run is None or not run.summary:
            return templates.TemplateResponse(
                request=request,
                name="partials/failure_sn_list.html",
                context={"error": "Run does not exist or is not finished yet.", "rows": []},
                status_code=404,
            )

        if not (test or "").strip():
            return templates.TemplateResponse(
                request=request,
                name="partials/failure_sn_list.html",
                context={"error": "Missing test name.", "rows": []},
                status_code=400,
            )

        df_main, selected_tests, err_msg = filter_selection_dataframe(run_id, run, tests, stations)
        if err_msg:
            return templates.TemplateResponse(
                request=request,
                name="partials/failure_sn_list.html",
                context={"error": err_msg, "rows": []},
                status_code=400,
            )

        run_limits = _get_session_run_limits(request, run_id)
        limits_map = compute_limits_for_selection(df_main, selected_tests, custom_limits=run_limits)
        df_main = df_main.copy()
        df_main["Status"] = df_main.apply(lambda row: evaluate_status(row, limits_map), axis=1)

        df_view = df_main[(df_main["Status"] == "NOK") & (df_main["TestName"].astype(str) == str(test))]
        max_rows = 500
        rows = []
        for _, row in df_view.head(max_rows).iterrows():
            error_desc = str(row.get("ProvisioningErrorDescription", "") or "").strip()
            error_info = ""
            if error_desc:
                error_info = f"{row.get('Value')} - {error_desc}"
            rows.append(
                {
                    "sn": str(row.get("SN", "") or ""),
                    "station": str(row.get("Station", "") or ""),
                    "value": row.get("Value"),
                    "status": str(row.get("Status", "") or ""),
                    "raw_date": str(row.get("RawDate", "") or row.get("Date", "") or ""),
                    "error_info": error_info,
                }
            )

        return templates.TemplateResponse(
            request=request,
            name="partials/failure_sn_list.html",
            context={
                "error": None,
                "rows": rows,
                "test": str(test),
                "total_rows": int(len(df_view)),
                "truncated": int(len(df_view)) > max_rows,
            },
        )

    @router.get("/runs/{run_id}/charts-extra", response_class=HTMLResponse)
    def run_charts_extra(
        request: Request,
        run_id: str,
        tests: list[str] = Query(default=[]),
        stations: list[str] = Query(default=[]),
        chart_kind: str = Query(default="hist"),
        param: str = Query(default=""),
        corr_x: str = Query(default=""),
        corr_y: str = Query(default=""),
    ) -> HTMLResponse:
        tests = normalize_query_values(tests)
        stations = normalize_query_values(stations)
        run = store.get(run_id)
        if run is None:
            return templates.TemplateResponse(
                request=request,
                name="partials/run_extra_charts.html",
                context={"error": "Run does not exist.", "scatter_charts": None},
                status_code=404,
            )
        if not run.summary:
            return templates.TemplateResponse(
                request=request,
                name="partials/run_extra_charts.html",
                context={"error": "Run analysis is not finished yet.", "scatter_charts": None},
                status_code=409,
            )
        ctx = build_extra_charts_context(
            run_id,
            run,
            tests,
            stations,
            chart_kind,
            param,
            corr_x,
            corr_y,
            custom_limits=_get_session_run_limits(request, run_id),
        )
        return templates.TemplateResponse(
            request=request,
            name="partials/run_extra_charts.html",
            context=ctx,
        )

    @router.post("/runs/{run_id}/limits", response_class=HTMLResponse)
    async def save_run_limits(request: Request, run_id: str) -> HTMLResponse:
        if store.get(run_id) is None:
            return HTMLResponse(
                "<p class='callout callout-error'>Run does not exist.</p>",
                status_code=404,
            )
        form = await request.form()
        parsed = _parse_limits_form(form)
        if not parsed:
            return HTMLResponse(
                "<p class='muted'>No valid LSL/USL values to save. "
                "Fill both numbers for tests you want to override.</p>"
            )
        merged = _get_session_run_limits(request, run_id)
        merged.update(parsed)
        _set_session_run_limits(request, run_id, merged)
        return HTMLResponse(
            "<p class='muted'>Limits updated for this run only. "
            "Other users and other runs still use global defaults.</p>",
            headers=hx_trigger_header(
                events=["limits-saved"],
                toast_level="success",
                toast_message="Limits updated for this run.",
            ),
        )

    @router.post("/runs/{run_id}/limits/reset", response_class=HTMLResponse)
    async def reset_run_limits(request: Request, run_id: str) -> HTMLResponse:
        if store.get(run_id) is None:
            return HTMLResponse(
                "<p class='callout callout-error'>Run does not exist.</p>",
                status_code=404,
            )
        form = await request.form()
        tests = _parse_limit_test_names(form)
        _reset_session_run_limits(request, run_id, tests if tests else None)
        return HTMLResponse(
            "<p class='muted'>Run-specific limit overrides were removed. "
            "Press <strong>Compute</strong> to apply global defaults again.</p>",
            headers=hx_trigger_header(
                toast_level="info",
                toast_message="Run-specific limits were reset.",
            ),
        )

    @router.post("/runs/{run_id}/limits/ai", response_class=HTMLResponse)
    async def ai_limits_run(request: Request, run_id: str) -> HTMLResponse:
        run = store.get(run_id)
        if run is None:
            return HTMLResponse(
                "<p class='callout callout-error'>Run does not exist.</p>",
                status_code=404,
            )
        if not run.summary:
            return HTMLResponse(
                "<p class='callout callout-error'>Analysis is not finished yet.</p>",
                status_code=409,
            )
        form = await request.form()
        tests = normalize_query_values([str(x) for x in form.getlist("tests")])
        stations = normalize_query_values([str(x) for x in form.getlist("stations")])
        method_label = str(form.get("ai_method") or "IQR")
        df_main, selected_tests, err = filter_selection_dataframe(run_id, run, tests, stations)
        if err:
            return HTMLResponse(
                f"<p class='callout callout-error'>{err}</p>",
                status_code=400,
            )
        merged = _get_session_run_limits(request, run_id)
        for test in selected_tests:
            vals = df_main[df_main["TestName"] == test]["Value"].dropna()
            lo, hi = calculate_ai_limits(vals, method_label)
            merged[test] = (float(lo), float(hi))
        _set_session_run_limits(request, run_id, merged)
        return HTMLResponse(
            "<p class='muted'>AI limits were applied only to this run. "
            "Press <strong>Compute</strong>.</p>",
            headers=hx_trigger_header(
                toast_level="success",
                toast_message="AI limits applied to this run.",
            ),
        )

    @router.get("/runs/{run_id}/export")
    def run_export(
        request: Request,
        run_id: str,
        export_type: str = "detail",
        mode: str = Query(
            default="full",
            description="full = entire run (legacy limits); selection = same filters as detail analysis",
        ),
        status_filter: str = Query(
            default="all",
            description="all | ok | nok - filters rows by Status column",
        ),
        tests: list[str] = Query(default=[]),
        stations: list[str] = Query(default=[]),
    ) -> StreamingResponse:
        tests = normalize_query_values(tests)
        stations = normalize_query_values(stations)
        run = store.get(run_id)
        if run is None or run.summary is None:
            raise HTTPException(status_code=404, detail="Run not found or not finished.")
        if mode not in ("full", "selection"):
            raise HTTPException(status_code=400, detail="mode must be 'full' or 'selection'.")
        et = (export_type or "detail").lower()
        if et not in ("detail", "simple", "summary"):
            raise HTTPException(
                status_code=400,
                detail="export_type must be 'detail', 'simple', or 'summary'.",
            )
        sf = (status_filter or "all").lower()
        if sf not in ("all", "ok", "nok"):
            raise HTTPException(
                status_code=400,
                detail="status_filter must be 'all', 'ok', or 'nok'.",
            )

        if mode == "selection":
            df, err_msg = dataframe_for_selection(
                run_id,
                run,
                tests,
                stations,
                custom_limits=_get_session_run_limits(request, run_id),
            )
        else:
            df, err_msg = dataframe_full_run_with_simple_limits(run_id, run.request)

        if err_msg:
            raise HTTPException(status_code=400, detail=err_msg)
        if df is None or df.empty:
            raise HTTPException(status_code=400, detail="No data for this run.")
        if "Status" not in df.columns:
            raise HTTPException(status_code=500, detail="Export dataframe missing Status column.")

        if sf == "ok":
            df = df.loc[df["Status"] == "OK"].copy()
        elif sf == "nok":
            df = df.loc[df["Status"] == "NOK"].copy()

        if df.empty:
            raise HTTPException(
                status_code=400,
                detail="No rows match the selected status filter (OK/NOK).",
            )

        now = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        sf_tag = sf if sf != "all" else "all"
        if et == "summary":
            summary = (
                df.groupby(["Station", "TestName", "Status"])
                .size()
                .reset_index(name="Count")
            )
            csv_bytes = summary.to_csv(index=False).encode("utf-8")
            filename = f"Trezor_Souhrn_{sf_tag}_{now}.csv"
        elif et == "simple":
            simple_cols = [
                c
                for c in [
                    "Station",
                    "SN",
                    "TestName",
                    "Value",
                    "Status",
                ]
                if c in df.columns
            ]
            csv_bytes = df[simple_cols].to_csv(index=False).encode("utf-8")
            filename = f"Trezor_Jednoduchy_{sf_tag}_{now}.csv"
        else:
            detail_cols = [
                c
                for c in [
                    "Station",
                    "Source",
                    "Operation",
                    "SN",
                    "device_sn",
                    "TestName",
                    "Value",
                    "Status",
                    "RawDate",
                    "Date",
                    "Unit",
                    "LowerLimit",
                    "UpperLimit",
                    "Origin",
                    "ProvisioningErrorDescription",
                ]
                if c in df.columns
            ]
            csv_bytes = df[detail_cols].to_csv(index=False).encode("utf-8")
            filename = f"Trezor_Detailni_{sf_tag}_{now}.csv"

        return StreamingResponse(
            iter([csv_bytes]),
            media_type="text/csv; charset=utf-8",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    @router.post("/runs")
    def submit_run(
        data_source: str = Form(default="files"),
        path: str = Form(default=""),
        upload_archive: UploadFile | None = File(default=None),
        db_base_url: str = Form(default="https://pegatron-db.corp.sldev.cz/"),
        db_profile: str = Form(default="pegatron"),
        db_username: str = Form(default=""),
        db_password: str = Form(default=""),
        date_from_ymd: str = Form(default=""),
        date_to_ymd: str = Form(default=""),
        mysql_row_limit: str = Form(default="500000"),
        background: BackgroundTasks = None,  # type: ignore[assignment]
    ) -> RedirectResponse:
        def _parse_mysql_row_limit(raw: str) -> int:
            s = (raw or "").strip()
            if not s:
                return 500_000
            try:
                v = int(s)
            except ValueError:
                return 500_000
            return max(1, min(v, 20_000_000))

        if data_source not in ("files", "mysql", "upload"):
            raise HTTPException(status_code=400, detail="Invalid data source.")
        if data_source == "mysql":
            if db_profile == "manufacturing":
                base = (db_base_url or "").strip() or MANUFACTURING_DEFAULT_BASE_URL
            else:
                base = (db_base_url or "").strip() or "https://pegatron-db.corp.sldev.cz/"
            user = (db_username or "").strip()
            if not user:
                raise HTTPException(status_code=400, detail="Enter database username.")
            if not (db_password or "").strip():
                raise HTTPException(status_code=400, detail="Enter database password.")
            if db_profile not in ("pegatron", "manufacturing"):
                raise HTTPException(status_code=400, detail="Invalid database profile.")
            display_path = mysql_display_path_from_base_url(
                base, db_profile=db_profile if db_profile in ("pegatron", "manufacturing") else None
            )
            req = RunRequest(
                path=display_path,
                data_source="mysql",
                db_base_url=base,
                db_username=user,
                db_profile=db_profile,
                date_from_ymd=int(date_from_ymd) if date_from_ymd.strip() else None,
                date_to_ymd=int(date_to_ymd) if date_to_ymd.strip() else None,
                lang="EN",
                mysql_row_limit=_parse_mysql_row_limit(mysql_row_limit),
            )
            run = store.create(req)
            stash_db_password(run.id, db_password)
        elif data_source == "upload":
            if upload_archive is None or not (upload_archive.filename or "").strip():
                raise HTTPException(status_code=400, detail="Select archive file (.zip/.tar/.tar.gz/.tgz).")
            try:
                extracted_path, display_path = extract_uploaded_archive(upload_archive)
            except ValueError as e:
                raise HTTPException(status_code=400, detail=str(e))
            req = RunRequest(
                path=display_path,
                data_source="upload",
                upload_extracted_path=extracted_path,
                upload_original_name=(upload_archive.filename or "").strip() or None,
                date_from_ymd=int(date_from_ymd) if date_from_ymd.strip() else None,
                date_to_ymd=int(date_to_ymd) if date_to_ymd.strip() else None,
                lang="EN",
            )
            run = store.create(req)
        else:
            if not (path or "").strip():
                raise HTTPException(status_code=400, detail="Enter path to logs folder.")
            req = RunRequest(
                path=path.strip(),
                data_source="files",
                date_from_ymd=int(date_from_ymd) if date_from_ymd.strip() else None,
                date_to_ymd=int(date_to_ymd) if date_to_ymd.strip() else None,
                lang="EN",
            )
            run = store.create(req)
        if background is not None:
            background.add_task(_run_job, run.id, req, store)
        return RedirectResponse(url="/dashboard", status_code=303)

    @router.get("/partials/dashboard-kpis", response_class=HTMLResponse)
    def dashboard_kpis_partial(request: Request) -> HTMLResponse:
        return dashboard_kpis_partial_page(request=request, templates=templates, store=store)

    @router.get("/partials/runs", response_class=HTMLResponse)
    def runs_partial(
        request: Request,
        status: str = "all",
        q: str = "",
        page: int = 1,
        page_size: int = 20,
        pagination_base: str = "/dashboard",
    ) -> HTMLResponse:
        return runs_partial_page(
            request=request,
            templates=templates,
            store=store,
            status=status,
            q=q,
            page=page,
            page_size=page_size,
            pagination_base=pagination_base,
        )

    @router.delete("/runs/{run_id}")
    def delete_run_view(run_id: str) -> Response:
        run = store.get(run_id)
        if run is None:
            raise HTTPException(status_code=404, detail="Run not found.")
        if not store.delete(run_id):
            raise HTTPException(status_code=404, detail="Run not found.")
        return Response(status_code=204)

    return router
