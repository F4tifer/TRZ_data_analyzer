"""Transient DB passwords for hybrid runs (not persisted; cleared after job starts)."""

from __future__ import annotations

import threading

_lock = threading.Lock()
_pending: dict[str, str] = {}


def stash(run_id: str, password: str) -> None:
    with _lock:
        _pending[run_id] = password


def pop(run_id: str) -> str | None:
    with _lock:
        return _pending.pop(run_id, None)
