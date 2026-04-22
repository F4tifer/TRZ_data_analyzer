from typing import Literal

from fastapi import APIRouter, BackgroundTasks, HTTPException
from fastapi.responses import Response
from pydantic import BaseModel, Field

from hybrid_app.schemas import (
    MANUFACTURING_DEFAULT_BASE_URL,
    RunRequest,
    mysql_display_path_from_base_url,
)
from hybrid_app.services.analyzer_service import run_analysis
from hybrid_app.services.pending_db_password import pop as pop_db_password
from hybrid_app.services.pending_db_password import stash as stash_db_password
from hybrid_app.services.session_store import RunStore


def _run_job(run_id: str, request: RunRequest, store: RunStore) -> None:
    try:
        store.set_running(run_id)
        db_pw = None
        if request.data_source == "mysql":
            db_pw = pop_db_password(run_id)
        summary, warnings, extra = run_analysis(run_id, request, db_password=db_pw)
        store.set_success(run_id, summary, warnings, extra)
    except Exception as exc:  # noqa: BLE001
        store.set_error(run_id, str(exc))


class RunCreateBody(BaseModel):
    path: str = Field(default="", description="Log folder or ignored for mysql.")
    data_source: Literal["files", "mysql"] = "files"
    db_base_url: str | None = Field(default=None, description="e.g. https://pegatron-db.corp.sldev.cz/")
    db_profile: Literal["pegatron", "manufacturing"] = "pegatron"
    db_username: str | None = None
    db_password: str | None = Field(default=None, description="Only for mysql; not persisted.")
    date_from_ymd: int | None = None
    date_to_ymd: int | None = None
    mysql_row_limit: int = Field(default=500_000, ge=1, le=20_000_000)


def build_api_router(store: RunStore) -> APIRouter:
    router = APIRouter(prefix="/api", tags=["api"])

    @router.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @router.get("/runs")
    def list_runs(status: str | None = None, q: str | None = None) -> list[dict]:
        return [run.model_dump(mode="json") for run in store.list_recent(status=status, query=q)]

    @router.get("/runs/{run_id}")
    def get_run(run_id: str) -> dict:
        run = store.get(run_id)
        if run is None:
            raise HTTPException(status_code=404, detail="Run not found.")
        return run.model_dump(mode="json")

    @router.post("/runs")
    def create_run(body: RunCreateBody, background: BackgroundTasks) -> dict:
        if body.data_source == "mysql":
            user = (body.db_username or "").strip()
            if not user:
                raise HTTPException(status_code=400, detail="db_username required for mysql.")
            if not (body.db_password or "").strip():
                raise HTTPException(status_code=400, detail="db_password required for mysql.")
            if body.db_profile == "manufacturing":
                base = (body.db_base_url or "").strip() or MANUFACTURING_DEFAULT_BASE_URL
            else:
                base = (body.db_base_url or "").strip() or "https://pegatron-db.corp.sldev.cz/"
            display_path = mysql_display_path_from_base_url(
                base, db_profile=body.db_profile if body.db_profile in ("pegatron", "manufacturing") else None
            )
            req = RunRequest(
                path=display_path,
                data_source="mysql",
                db_base_url=base,
                db_username=user,
                db_profile=body.db_profile,
                date_from_ymd=body.date_from_ymd,
                date_to_ymd=body.date_to_ymd,
                lang="EN",
                mysql_row_limit=body.mysql_row_limit,
            )
            run = store.create(req)
            stash_db_password(run.id, body.db_password or "")
        else:
            if not (body.path or "").strip():
                raise HTTPException(status_code=400, detail="path required for files.")
            req = RunRequest(
                path=body.path.strip(),
                data_source="files",
                date_from_ymd=body.date_from_ymd,
                date_to_ymd=body.date_to_ymd,
                lang="EN",
            )
            run = store.create(req)
        background.add_task(_run_job, run.id, req, store)
        result = store.get(run.id)
        if result is None:
            raise HTTPException(status_code=500, detail="Failed to create run.")
        return {"run": result.model_dump(mode="json")}

    @router.delete("/runs/{run_id}")
    def delete_run(run_id: str) -> Response:
        run = store.get(run_id)
        if run is None:
            raise HTTPException(status_code=404, detail="Run not found.")
        if run.status not in ("success", "failed"):
            raise HTTPException(
                status_code=400,
                detail="Only finished runs (success or failed) can be deleted.",
            )
        if not store.delete(run_id):
            raise HTTPException(status_code=404, detail="Run not found.")
        return Response(status_code=204)

    return router
