from __future__ import annotations

from datetime import datetime
from uuid import uuid4

from sqlalchemy import func

from hybrid_app.db import RunRecord, get_session
from hybrid_app.services.artifact_store import delete_run_artifact
from hybrid_app.schemas import RunRequest, RunResult, RunSummary


class RunStore:
    """DB-backed run store (SQLite by default, configurable via ANALYZER_DB_URL)."""

    def create(self, request: RunRequest) -> RunResult:
        run_id = uuid4().hex
        created = datetime.utcnow()
        record = RunRecord(
            id=run_id,
            created_at=created,
            status="queued",
            path=request.path,
            data_source=request.data_source,
            db_base_url=request.db_base_url,
            db_username=request.db_username,
            db_profile=getattr(request, "db_profile", None) or "pegatron",
            date_from_ymd=request.date_from_ymd,
            date_to_ymd=request.date_to_ymd,
            lang="EN",
            mysql_row_limit=(request.mysql_row_limit if request.data_source == "mysql" else None),
            summary=None,
            warnings=[],
            error=None,
        )
        with get_session() as session:
            session.add(record)
            session.commit()
        return RunResult(id=run_id, created_at=created, status="queued", request=request)

    def set_running(self, run_id: str) -> None:
        with get_session() as session:
            record = session.get(RunRecord, run_id)
            if record is None:
                return
            record.status = "running"
            session.commit()

    def set_success(
        self, run_id: str, summary: RunSummary, warnings: list[str] | None = None, extra: dict | None = None
    ) -> None:
        with get_session() as session:
            record = session.get(RunRecord, run_id)
            if record is None:
                return
            record.status = "success"
            payload = summary.model_dump(mode="json")
            if extra and "charts" in extra:
                payload["charts"] = extra["charts"]
            if extra and "metadata" in extra:
                payload["metadata"] = extra["metadata"]
            record.summary = payload
            record.warnings = warnings or []
            session.commit()

    def set_error(self, run_id: str, error: str) -> None:
        with get_session() as session:
            record = session.get(RunRecord, run_id)
            if record is None:
                return
            record.status = "failed"
            record.error = error
            session.commit()

    def _to_model(self, record: RunRecord) -> RunResult:
        mrl = getattr(record, "mysql_row_limit", None)
        req = RunRequest(
            path=record.path,
            data_source=getattr(record, "data_source", None) or "files",
            db_base_url=getattr(record, "db_base_url", None),
            db_username=getattr(record, "db_username", None),
            db_profile=getattr(record, "db_profile", None) or "pegatron",
            date_from_ymd=record.date_from_ymd,
            date_to_ymd=record.date_to_ymd,
            lang="EN",
            mysql_row_limit=int(mrl) if mrl is not None else 500_000,
        )
        summary: RunSummary | None = None
        if record.summary:
            summary = self._summary_from_db(record.summary)
        return RunResult(
            id=record.id,
            created_at=record.created_at,
            status=record.status,
            request=req,
            summary=summary,
            warnings=record.warnings or [],
            error=record.error,
        )

    @staticmethod
    def _summary_from_db(raw: dict) -> RunSummary | None:
        """Be tolerant to older rows / partial JSON so opening a run does not 500."""
        defaults: dict = {
            "total_rows": 0,
            "ok_rows": 0,
            "nok_rows": 0,
            "yield_pct": 0.0,
            "tests": [],
            "stations": [],
            "top_failures": {},
            "charts": {},
            "metadata": {},
        }
        merged = {**defaults, **raw}
        for key in ("top_failures", "charts", "metadata"):
            if merged.get(key) is None:
                merged[key] = {}
        try:
            return RunSummary.model_validate(merged)
        except Exception:
            return None

    def _filtered_query(self, session, status: str | None, query: str | None):
        q = session.query(RunRecord)
        if status and status != "all":
            q = q.filter(RunRecord.status == status)
        if query:
            q = q.filter(RunRecord.path.ilike(f"%{query}%"))
        return q

    def count_runs(self, status: str | None = None, query: str | None = None) -> int:
        with get_session() as session:
            q = self._filtered_query(session, status, query)
            return int(q.count())

    def list_recent(
        self,
        status: str | None = None,
        query: str | None = None,
        *,
        limit: int = 20,
        offset: int = 0,
    ) -> list[RunResult]:
        with get_session() as session:
            q = self._filtered_query(session, status, query)
            records = q.order_by(RunRecord.created_at.desc()).limit(limit).offset(offset).all()
        return [self._to_model(r) for r in records]

    def get(self, run_id: str) -> RunResult | None:
        with get_session() as session:
            record = session.get(RunRecord, run_id)
            if record is None:
                return None
        return self._to_model(record)

    def delete(self, run_id: str) -> bool:
        """Remove run from DB and delete cached artifact if present."""
        with get_session() as session:
            record = session.get(RunRecord, run_id)
            if record is None:
                return False
            session.delete(record)
            session.commit()
        delete_run_artifact(run_id)
        return True

    def dashboard_stats(self) -> dict[str, int]:
        """Counts of runs by status (all rows in DB)."""
        with get_session() as session:
            rows = (
                session.query(RunRecord.status, func.count(RunRecord.id))
                .group_by(RunRecord.status)
                .all()
            )
            total = session.query(func.count(RunRecord.id)).scalar() or 0
        by_status = {str(s): int(c) for s, c in rows}
        return {"total": int(total), "by_status": by_status}
