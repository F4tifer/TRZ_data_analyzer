"""
Optional session login: set ANALYZER_AUTH_USER + ANALYZER_AUTH_PASSWORD (and ANALYZER_SESSION_SECRET).
If ANALYZER_AUTH_USER is unset, auth is disabled (open access).
"""
from __future__ import annotations

import os
import secrets
from typing import Callable

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import RedirectResponse, Response

SESSION_USER_KEY = "hybrid_user"


def auth_enabled() -> bool:
    user = (os.getenv("ANALYZER_AUTH_USER") or "").strip()
    pw = (os.getenv("ANALYZER_AUTH_PASSWORD") or "").strip()
    return bool(user and pw)


def session_secret() -> str:
    s = (os.getenv("ANALYZER_SESSION_SECRET") or "").strip()
    if len(s) >= 16:
        return s
    return secrets.token_hex(32)


def verify_credentials(username: str, password: str) -> bool:
    if not auth_enabled():
        return True
    u = (os.getenv("ANALYZER_AUTH_USER") or "").strip()
    p = (os.getenv("ANALYZER_AUTH_PASSWORD") or "").strip()
    return secrets.compare_digest(username or "", u) and secrets.compare_digest(password or "", p)


def is_logged_in(request: Request) -> bool:
    if not auth_enabled():
        return True
    return bool(request.session.get(SESSION_USER_KEY))


PUBLIC_PREFIXES: tuple[str, ...] = (
    "/static",
    "/favicon.ico",
    "/login",
    "/api/health",
)


def _is_public(path: str) -> bool:
    if path in ("/login", "/api/health"):
        return True
    return path.startswith(PUBLIC_PREFIXES)


class AuthMiddleware(BaseHTTPMiddleware):
    """Redirect unauthenticated users to /login when auth is enabled."""

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        if not auth_enabled():
            return await call_next(request)
        path = request.url.path
        if _is_public(path):
            return await call_next(request)
        if is_logged_in(request):
            return await call_next(request)
        return RedirectResponse(url="/login", status_code=303)
