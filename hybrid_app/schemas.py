from datetime import datetime
from typing import Literal
from urllib.parse import urlparse

from pydantic import BaseModel, Field

# PCB / manufacturing profile: actual default MySQL DB name on manufacturing host.
MANUFACTURING_DEFAULT_MYSQL_DB = "t3w1"
MANUFACTURING_DEFAULT_BASE_URL = f"https://manufacturing-db.corp.sldev.cz/{MANUFACTURING_DEFAULT_MYSQL_DB}"


def mysql_display_path_from_base_url(
    base_url: str,
    *,
    db_profile: Literal["pegatron", "manufacturing"] | None = None,
) -> str:
    """
    Stable path label for runs sourced from MySQL (stored in RunRecord.path).
    If URL has no DB path, use profile-specific default DB (not always pegatron-db).
    """
    if db_profile == "manufacturing":
        fallback_url = MANUFACTURING_DEFAULT_BASE_URL
        default_host = "manufacturing-db.corp.sldev.cz"
        default_db = MANUFACTURING_DEFAULT_MYSQL_DB
    else:
        fallback_url = "https://pegatron-db.corp.sldev.cz/"
        default_host = "pegatron-db"
        default_db = "pegatron-db"

    raw = (base_url or "").strip()
    p = urlparse(raw or fallback_url)
    host = p.hostname or default_host
    path_db = (p.path or "").strip("/")
    db = path_db if path_db else default_db
    return f"mysql://{host}/{db}"


class RunRequest(BaseModel):
    path: str = Field(..., description="Path to local folder with logs, or mysql://host/db for DB runs.")
    data_source: Literal["files", "mysql", "upload"] = Field(
        default="files",
        description="files = load_data(path); upload = load_data(temp extracted path); mysql = app.db_search",
    )
    upload_extracted_path: str | None = Field(
        default=None,
        description="Temporary server path used for uploaded archive extraction.",
    )
    upload_original_name: str | None = Field(
        default=None,
        description="Original uploaded archive filename for display/debug.",
    )
    db_base_url: str | None = Field(
        default=None,
        description="e.g. https://pegatron-db.corp.sldev.cz/ (host + optional DB path)",
    )
    db_username: str | None = Field(default=None, description="MySQL user (not stored with password).")
    db_profile: Literal["pegatron", "manufacturing"] = Field(
        default="pegatron",
        description="pegatron = pegatron-db + FATP; manufacturing = MySQL DB t3w1 na manufacturing hostu (end_test).",
    )
    date_from_ymd: int | None = Field(default=None, description="Inclusive from date in YYYYMMDD.")
    date_to_ymd: int | None = Field(default=None, description="Inclusive to date in YYYYMMDD.")
    lang: Literal["EN"] = Field(default="EN", description="Fixed application language.")
    mysql_row_limit: int = Field(
        default=500_000,
        ge=1,
        le=20_000_000,
        description="Max rows per DB query (T3W1 + FATP) for mysql source; app.db_search.search_generic limit.",
    )


class RunSummary(BaseModel):
    total_rows: int
    ok_rows: int
    nok_rows: int
    yield_pct: float
    tests: list[str]
    stations: list[str]
    top_failures: dict[str, int] = Field(default_factory=dict)
    charts: dict[str, str] = Field(default_factory=dict)
    metadata: dict[str, str] = Field(default_factory=dict)


class RunResult(BaseModel):
    id: str
    created_at: datetime
    status: str
    request: RunRequest
    summary: RunSummary | None = None
    warnings: list[str] = Field(default_factory=list)
    error: str | None = None
