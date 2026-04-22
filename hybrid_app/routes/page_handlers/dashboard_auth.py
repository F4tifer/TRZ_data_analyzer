from urllib.parse import quote_plus

from fastapi import Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from hybrid_app.auth import SESSION_USER_KEY, auth_enabled, verify_credentials
from hybrid_app.services.session_store import RunStore


def _resolve_pagination(page: int, page_size: int, total: int) -> tuple[int, int, int, int]:
    page_size = max(5, min(100, page_size))
    total_pages = max(1, (total + page_size - 1) // page_size) if total > 0 else 1
    page = min(max(1, page), total_pages)
    offset = (page - 1) * page_size
    return page, page_size, total_pages, offset


def login_get_page(request: Request, templates: Jinja2Templates) -> HTMLResponse:
    if not auth_enabled():
        return RedirectResponse("/", status_code=303)
    if request.session.get(SESSION_USER_KEY):
        return RedirectResponse("/", status_code=303)
    return templates.TemplateResponse(
        request=request,
        name="login.html",
        context={"error": None},
    )


def login_post_page(
    request: Request, templates: Jinja2Templates, username: str, password: str
) -> RedirectResponse | HTMLResponse:
    if not auth_enabled():
        return RedirectResponse("/", status_code=303)
    if verify_credentials(username, password):
        request.session[SESSION_USER_KEY] = username
        return RedirectResponse("/", status_code=303)
    return templates.TemplateResponse(
        request=request,
        name="login.html",
        context={"error": "Invalid login."},
        status_code=401,
    )


def logout_page(request: Request) -> RedirectResponse:
    request.session.clear()
    if auth_enabled():
        return RedirectResponse("/login", status_code=303)
    return RedirectResponse("/", status_code=303)


def index_page(request: Request, templates: Jinja2Templates, store: RunStore) -> HTMLResponse:
    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={},
    )


def dashboard_page(
    request: Request,
    templates: Jinja2Templates,
    store: RunStore,
    status: str,
    q: str,
    page: int,
    page_size: int,
) -> HTMLResponse:
    total = store.count_runs(status, q or None)
    page, page_size, total_pages, offset = _resolve_pagination(page, page_size, total)
    runs = store.list_recent(status=status, query=q or None, limit=page_size, offset=offset)
    return templates.TemplateResponse(
        request=request,
        name="dashboard.html",
        context={
            "runs": runs,
            "status": status,
            "q": q,
            "q_encoded": quote_plus(q or ""),
            "page": page,
            "page_size": page_size,
            "total": total,
            "total_pages": total_pages,
            "stats": store.dashboard_stats(),
            "pagination_base": "/dashboard",
            "enable_multi_select": True,
        },
    )


def dashboard_kpis_partial_page(
    request: Request, templates: Jinja2Templates, store: RunStore
) -> HTMLResponse:
    st = store.dashboard_stats()
    by = st.get("by_status") or {}
    return templates.TemplateResponse(
        request=request,
        name="partials/dashboard_kpis.html",
        context={
            "total_runs": st.get("total", 0),
            "success_runs": by.get("success", 0),
            "failed_runs": by.get("failed", 0),
        },
    )


def runs_partial_page(
    request: Request,
    templates: Jinja2Templates,
    store: RunStore,
    status: str,
    q: str,
    page: int,
    page_size: int,
    pagination_base: str,
) -> HTMLResponse:
    total = store.count_runs(status, q or None)
    page, page_size, total_pages, offset = _resolve_pagination(page, page_size, total)
    runs = store.list_recent(status=status, query=q or None, limit=page_size, offset=offset)
    return templates.TemplateResponse(
        request=request,
        name="partials/runs_table.html",
        context={
            "runs": runs,
            "page": page,
            "page_size": page_size,
            "total": total,
            "total_pages": total_pages,
            "status": status,
            "q": q,
            "q_encoded": quote_plus(q or ""),
            "pagination_base": pagination_base,
            "enable_multi_select": pagination_base == "/dashboard",
        },
    )
