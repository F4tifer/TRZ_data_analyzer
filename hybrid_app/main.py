from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware

from hybrid_app.auth import AuthMiddleware, auth_enabled, session_secret
from hybrid_app.db import init_db
from hybrid_app.routes.api import build_api_router
from hybrid_app.routes.pages import build_pages_router
from hybrid_app.services.session_store import RunStore
from hybrid_app.settings import STATIC_DIR, TEMPLATES_DIR
from app_version import APP_VERSION


def create_app() -> FastAPI:
    init_db()
    app = FastAPI(title="Trezor Log Analyzer - Hybrid")
    templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
    templates.env.globals["auth_enabled"] = auth_enabled
    templates.env.globals["app_version"] = APP_VERSION
    run_store = RunStore()

    # Last added runs first: SessionMiddleware must populate request.session before AuthMiddleware.
    app.add_middleware(AuthMiddleware)
    app.add_middleware(SessionMiddleware, secret_key=session_secret(), same_site="lax")

    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
    app.include_router(build_pages_router(run_store, templates))
    app.include_router(build_api_router(run_store))
    return app


app = create_app()
