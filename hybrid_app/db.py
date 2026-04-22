from __future__ import annotations

import os
from datetime import datetime

from sqlalchemy import JSON, DateTime, Integer, String, Text, create_engine, inspect, text
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column


class Base(DeclarativeBase):
    pass


def get_database_url() -> str:
    return os.getenv("ANALYZER_DB_URL", "sqlite:///./analyzer_meta.db")


engine = create_engine(get_database_url(), future=True)


class RunRecord(Base):
    __tablename__ = "analysis_runs"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), index=True)
    status: Mapped[str] = mapped_column(String(32), index=True)

    path: Mapped[str] = mapped_column(Text)
    data_source: Mapped[str] = mapped_column(String(16), default="files")
    db_base_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    db_username: Mapped[str | None] = mapped_column(Text, nullable=True)
    db_profile: Mapped[str] = mapped_column(String(32), default="pegatron")
    date_from_ymd: Mapped[int | None] = mapped_column(Integer, nullable=True)
    date_to_ymd: Mapped[int | None] = mapped_column(Integer, nullable=True)
    lang: Mapped[str] = mapped_column(String(8), default="EN")
    mysql_row_limit: Mapped[int | None] = mapped_column(Integer, nullable=True)

    summary: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    warnings: Mapped[list | None] = mapped_column(JSON, nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)


def _migrate_analysis_runs() -> None:
    """Add columns introduced after first deploy (SQLite)."""
    try:
        insp = inspect(engine)
    except Exception:
        return
    if not insp.has_table("analysis_runs"):
        return
    cols = {c["name"] for c in insp.get_columns("analysis_runs")}
    alters: list[str] = []
    if "data_source" not in cols:
        alters.append("ALTER TABLE analysis_runs ADD COLUMN data_source VARCHAR(16) DEFAULT 'files'")
    if "db_base_url" not in cols:
        alters.append("ALTER TABLE analysis_runs ADD COLUMN db_base_url TEXT")
    if "db_username" not in cols:
        alters.append("ALTER TABLE analysis_runs ADD COLUMN db_username TEXT")
    if "db_profile" not in cols:
        alters.append("ALTER TABLE analysis_runs ADD COLUMN db_profile VARCHAR(32) DEFAULT 'pegatron'")
    if "mysql_row_limit" not in cols:
        alters.append("ALTER TABLE analysis_runs ADD COLUMN mysql_row_limit INTEGER DEFAULT 500000")
    if not alters:
        return
    with engine.begin() as conn:
        for stmt in alters:
            conn.execute(text(stmt))


def init_db() -> None:
    Base.metadata.create_all(bind=engine)
    _migrate_analysis_runs()


def get_session() -> Session:
    return Session(bind=engine, future=True)

