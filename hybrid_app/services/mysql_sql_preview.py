"""
Text SQL preview for MySQL loading (app.db_search.search_generic).
Does not connect to DB; test columns from ict/functional/provisioning are expanded
from INFORMATION_SCHEMA during runtime.
"""

from __future__ import annotations

from hybrid_app.schemas import RunRequest


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


def build_mysql_load_sql_preview(req: RunRequest, *, limit: int | None = None) -> str:
    """Use the same parameters as hybrid analyzer search_generic(date_from=..., date_to=..., limit=...)."""
    if limit is None:
        limit = int(getattr(req, "mysql_row_limit", None) or 500_000)
    limit = max(1, min(int(limit), 20_000_000))
    profile = req.db_profile or "pegatron"
    date_from = _ymd_to_sql_start(req.date_from_ymd)
    date_to = _ymd_to_sql_end(req.date_to_ymd)

    tc = "r.end_test" if profile == "manufacturing" else "r.start_test"
    if profile == "manufacturing":
        run_dt_expr = "COALESCE(r.end_test, r.start_test)"
        order_t3 = "ORDER BY r.db_server_time DESC, r.t3w1_run_id DESC"
    else:
        run_dt_expr = "r.start_test"
        order_t3 = "ORDER BY r.start_test DESC, r.t3w1_run_id DESC"

    where_parts: list[str] = []
    if date_from:
        where_parts.append(f"{tc} >= %s   -- {date_from!r}")
    if date_to:
        where_parts.append(f"{tc} <= %s   -- {date_to!r}")
    where_block = ("WHERE\n    " + "\n    AND ".join(where_parts) + "\n") if where_parts else ""

    t3w1 = f"""-- =============================================================================
-- [1] T3W1 — wide query; columns i.* / f.* / p.* are expanded at runtime (INFORMATION_SCHEMA),
--     then transformed from wide to long format in Python.
--     Profil: {profile}
-- =============================================================================
SELECT
    r.t3w1_run_id,
    {run_dt_expr} AS _run_start,
    r.db_server_time AS _run_db_time,
    d.device_sn_man AS _sn,
    d.device_sn AS _device_sn,
    t.description AS _station,
    /* + i.`...` AS `ict__...`, f.`...` AS `func__...`, p.`...` AS `prov__...` */
FROM t3w1_run r
JOIN device d ON d.device_id = r.device_id
LEFT JOIN tester t ON t.tester_id = r.tester_id
LEFT JOIN t3w1_ict i ON i.t3w1_run_id = r.t3w1_run_id
LEFT JOIN t3w1_functional f ON f.t3w1_run_id = r.t3w1_run_id
LEFT JOIN t3w1_provisioning p ON p.t3w1_run_id = r.t3w1_run_id
{where_block}{order_t3}
LIMIT %s   -- {limit}
"""

    chunks: list[str] = [t3w1.rstrip()]

    if profile == "manufacturing":
        chunks.append(
            "-- (Manufacturing profile typically uses only query [1]; FATP part is not executed.)"
        )
        return "\n\n".join(chunks)

    fatp_where = ["1=1"]
    if date_from:
        fatp_where.append("m.Date >= %s")
    if date_to:
        fatp_where.append("m.Date <= %s")
    fatp_where_sql = " AND ".join(fatp_where)

    fatp = f"""-- =============================================================================
-- [2] FATP (Pegatron) — FROM body is UNION ALL over fatpfinal / fatprf / fatpsub
--     + join to tester; exact SQL text: app.db_search._build_fatp_base_select().
-- =============================================================================
SELECT m.Station, m.Source, m.TestName, m.Value, m.Unit, m.SN, m.Date, m.RawDate,
       m.File, m.LowerLimit, m.UpperLimit
FROM (
  /* ... UNION ALL SELECT ... FROM fatpfinal | fatprf | fatpsub ... */
) AS m
WHERE {fatp_where_sql}
ORDER BY m.Date DESC
LIMIT %s   -- {limit}
"""

    chunks.append(fatp.rstrip())
    chunks.append("-- Final app result = pandas.concat(T3W1 long, FATP rows).")
    return "\n\n".join(chunks)
