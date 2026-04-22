"""
SQL search for manufacturing / test data (MySQL/MariaDB).

Uses the MySQL driver (PyMySQL) directly — no HTTP API:
- connection: host, port, database, username, password
- this module builds SQL and returns results as `pandas.DataFrame`

Module API:
- configure_connection(base_url, username, password, *, default_db="pegatron-db", default_port=3306, db_profile="pegatron")
  - `base_url` may be a hostname (`pegatron-db.corp.sldev.cz`) or full URL (`https://pegatron-db.corp.sldev.cz`);
    host, port and database name are parsed from it.
  - `db_profile="manufacturing"`: T3W1 time filters use `r.end_test`, order `db_server_time DESC` (PCB / manufacturing-db),
    no merge with FATP tables.
- search_by_serial(...)
- search_by_date_range(...)
- search_by_station_and_test(...)
- search_generic(...)

All search functions return a `pandas.DataFrame` with columns compatible with the rest of the app:
- Station, TestName, Value, Unit, SN, Date, RawDate, File, LowerLimit, UpperLimit
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlparse

import pandas as pd

from app.provisioning_error_codes import describe_provisioning_error, is_provisioning_no_error


@dataclass
class DBConnectionConfig:
    """MySQL/MariaDB connection settings."""

    host: str
    port: int
    database: str
    username: str
    password: str
    #: pegatron = FATP + T3W1 (time r.start_test); manufacturing = PCB T3W1 (time r.end_test, order db_server_time)
    db_profile: str = "pegatron"


_DB_CONFIG: Optional[DBConnectionConfig] = None


def clear_connection() -> None:
    """Clear stored connection and caches (e.g. after hybrid analysis completes)."""
    global _DB_CONFIG, _T3W1_MELT_COLUMNS, _DB_CAPS, _LAST_RUN_STATS
    _DB_CONFIG = None
    _T3W1_MELT_COLUMNS = None
    _DB_CAPS = None
    _LAST_RUN_STATS = None

# Cached melt columns (t3w1_ict, t3w1_functional, t3w1_provisioning). Cleared by configure_connection().
_T3W1_MELT_COLUMNS: Optional[Dict[str, List[str]]] = None

# Last run diagnostics for UI (SQL row count, melt columns, long rows).
_LAST_RUN_STATS: Optional[Dict[str, Any]] = None

# Detected DB capabilities (presence of t3w1_* and fatp* tables).
_DB_CAPS: Optional[Dict[str, Any]] = None


def configure_connection(
    base_url: str,
    username: str,
    password: str,
    *,
    default_db: str = "pegatron-db",
    default_port: int = 3306,
    db_profile: str = "pegatron",
) -> None:
    """
    Set global connection configuration for this module.

    Example (e.g. at app startup):
    >>> from app import db_search
    >>> db_search.configure_connection(
    ...     base_url="https://pegatron-db.corp.sldev.cz",
    ...     username="user",
    ...     password="secret",
    ... )
    """
    parsed = urlparse(base_url)
    if parsed.scheme:
        host = parsed.hostname or ""
        port = parsed.port or default_port
        db_name = parsed.path.lstrip("/") or default_db
    else:
        host = base_url
        port = default_port
        db_name = default_db

    if not host:
        raise DBConnectionError("Host for DB connection is not valid.")

    global _DB_CONFIG, _T3W1_MELT_COLUMNS, _DB_CAPS
    _DB_CONFIG = DBConnectionConfig(
        host=host,
        port=port,
        database=db_name,
        username=username,
        password=password,
        db_profile=db_profile if db_profile in ("pegatron", "manufacturing") else "pegatron",
    )
    _T3W1_MELT_COLUMNS = None
    _DB_CAPS = None


class DBConnectionError(RuntimeError):
    """Connection error or query service failure."""


class DBQueryError(RuntimeError):
    """SQL execution error (syntax, permissions, etc.)."""


def _ensure_config() -> DBConnectionConfig:
    if _DB_CONFIG is None:
        raise DBConnectionError(
            "DB connection is not configured. Call configure_connection() first."
        )
    return _DB_CONFIG


def _t3w1_run_time_col_sql() -> str:
    """Time column for T3W1 filters: manufacturing uses end of test (PCB)."""
    cfg = _ensure_config()
    if cfg.db_profile == "manufacturing":
        return "r.end_test"
    return "r.start_test"


def _execute_sql(sql: str, params: Optional[Iterable[Any]] = None) -> List[Dict[str, Any]]:
    """
    Run SQL against MySQL/MariaDB and return rows as a list of dicts.
    """
    cfg = _ensure_config()

    try:
        import pymysql  # type: ignore
    except ImportError as exc:
        raise DBConnectionError(
            "The 'pymysql' package is required for MySQL/MariaDB. "
            "Install it (pip install pymysql)."
        ) from exc

    try:
        conn = pymysql.connect(
            host=cfg.host,
            port=cfg.port,
            user=cfg.username,
            password=cfg.password,
            database=cfg.database,
            cursorclass=pymysql.cursors.DictCursor,
            charset="utf8mb4",
            connect_timeout=15,
        )
    except Exception as exc:
        err_msg = str(exc)
        is_timeout = (
            getattr(exc, "args", ()) and exc.args[0] == 2003 and "timed out" in err_msg.lower()
        )
        if is_timeout:
            raise DBConnectionError(
                "Cannot connect to DB (timeout). Check network (VPN) and server availability."
            ) from exc
        raise DBConnectionError(f"Cannot connect to DB: {exc!r}") from exc

    try:
        with conn.cursor() as cur:
            cur.execute(sql, tuple(params) if params is not None else None)
            rows = cur.fetchall()
    except Exception as exc:
        raise DBQueryError(f"Error executing SQL query: {exc!r}\nSQL: {sql}") from exc
    finally:
        try:
            conn.close()
        except Exception:
            pass

    # rows is already a list of dicts (DictCursor)
    return list(rows)


# Columns skipped during melt (PK, FK, timestamps from child tables).
_T3W1_SKIP_COLUMNS = frozenset({"t3w1_ict_id", "t3w1_run_id", "db_server_time", "t3w1_functional_id", "t3w1_provisioning_id"})


def _safe_float(x: Any) -> Optional[float]:
    """Convert value to float; return None on error."""
    if x is None:
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _provisioning_desc_with_fallback(value: Any) -> Optional[str]:
    """
    Resolve provisioning error text from mapping table.
    If code is non-zero but missing in JSON mapping, return explicit fallback text.
    """
    if is_provisioning_no_error(value):
        return None
    desc = describe_provisioning_error(value)
    if desc:
        return desc
    num = _safe_float(value)
    if num is None:
        return "Unknown provisioning error code: n/a"
    try:
        code = int(num)
    except (TypeError, ValueError, OverflowError):
        return "Unknown provisioning error code: n/a"
    return f"Unknown provisioning error code: {code}"


def _get_db_caps() -> Dict[str, Any]:
    """
    Detect which tables exist in the current DB (t3w1_*, fatp*, tester).
    Used to choose T3W1 vs FATP queries (manufacturing-db vs pegatron-db differ).
    """
    global _DB_CAPS
    if _DB_CAPS is not None:
        return _DB_CAPS

    cfg = _ensure_config()
    table_names = [
        "t3w1_run",
        "device",
        "t3w1_ict",
        "t3w1_functional",
        "t3w1_provisioning",
        "fatpfinal",
        "fatprf",
        "fatpsub",
        "tester",
    ]
    placeholders = ", ".join("%s" for _ in table_names)
    sql = (
        "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
        f"WHERE TABLE_SCHEMA = %s AND TABLE_NAME IN ({placeholders})"
    )
    try:
        rows = _execute_sql(sql, [cfg.database] + table_names)
    except DBQueryError:
        _DB_CAPS = {"has_t3w1": False, "has_fatp": False, "tables": set()}
        return _DB_CAPS

    present = {
        (r.get("TABLE_NAME") or r.get("table_name") or "").lower()
        for r in rows
    }
    has_t3w1 = "t3w1_run" in present and "device" in present
    has_fatp = any(t in present for t in ("fatpfinal", "fatprf", "fatpsub"))
    _DB_CAPS = {
        "has_t3w1": has_t3w1,
        "has_fatp": has_fatp,
        "tables": present,
    }
    return _DB_CAPS


def _get_t3w1_melt_columns() -> Dict[str, List[str]]:
    """
    For each child table (t3w1_ict, t3w1_functional, t3w1_provisioning), return column names
    to expand into long format (everything except PK/FK/db_server_time).
    Result is cached in _T3W1_MELT_COLUMNS.
    """
    global _T3W1_MELT_COLUMNS
    if _T3W1_MELT_COLUMNS is not None:
        return _T3W1_MELT_COLUMNS
    caps = _get_db_caps()
    if not caps.get("has_t3w1"):
        _T3W1_MELT_COLUMNS = {}
        return _T3W1_MELT_COLUMNS
    tables = ["t3w1_ict", "t3w1_functional", "t3w1_provisioning"]
    placeholders = ", ".join("%s" for _ in tables)
    sql = (
        "SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
        f"WHERE TABLE_SCHEMA = %s AND TABLE_NAME IN ({placeholders}) ORDER BY TABLE_NAME, ORDINAL_POSITION"
    )
    cfg = _ensure_config()
    try:
        rows = _execute_sql(sql, [cfg.database] + tables)
    except Exception:
        _T3W1_MELT_COLUMNS = {t: [] for t in tables}
        raise
    by_table: Dict[str, List[str]] = {t: [] for t in tables}
    for r in rows:
        # PyMySQL/MySQL may return keys in different cases (TABLE_NAME vs table_name)
        table = (r.get("TABLE_NAME") or r.get("table_name") or "")
        col = (r.get("COLUMN_NAME") or r.get("column_name") or "")
        table_lower = str(table).lower() if table else ""
        if col not in _T3W1_SKIP_COLUMNS and table_lower in by_table:
            by_table[table_lower].append(col)
    # Fallback: if INFORMATION_SCHEMA returned nothing, try SHOW COLUMNS
    if not any(by_table.values()):
        for t in tables:
            try:
                show_rows = _execute_sql(f"SHOW COLUMNS FROM `{t}`", None)
                for r in show_rows:
                    col = (r.get("Field") or r.get("field") or "")
                    if col and col not in _T3W1_SKIP_COLUMNS:
                        by_table[t].append(col)
            except Exception:
                pass
    _T3W1_MELT_COLUMNS = by_table
    return by_table


def _build_t3w1_select_and_from(
    melt_columns: Dict[str, List[str]],
) -> str:
    """
    Build SQL prefix: SELECT ... FROM t3w1_run r JOIN device d ... LEFT JOIN t3w1_ict i ...
    with column aliases ict__, func__, prov__ for later melt.
    """
    cfg = _ensure_config()
    if cfg.db_profile == "manufacturing":
        run_dt = "COALESCE(r.end_test, r.start_test)"
    else:
        run_dt = "r.start_test"
    selects = [
        "r.t3w1_run_id",
        f"{run_dt} AS _run_start",
        "r.db_server_time AS _run_db_time",
        "d.device_sn_man AS _sn",
        "d.device_sn AS _device_sn",
        "t.description AS _station",
    ]
    if melt_columns.get("t3w1_ict"):
        for c in melt_columns["t3w1_ict"]:
            selects.append(f"i.`{c}` AS `ict__{c}`")
    if melt_columns.get("t3w1_functional"):
        for c in melt_columns["t3w1_functional"]:
            selects.append(f"f.`{c}` AS `func__{c}`")
    if melt_columns.get("t3w1_provisioning"):
        for c in melt_columns["t3w1_provisioning"]:
            selects.append(f"p.`{c}` AS `prov__{c}`")
    select_clause = ",\n    ".join(selects)
    return (
        "SELECT\n    " + select_clause + "\n"
        "FROM t3w1_run r\n"
        "JOIN device d ON d.device_id = r.device_id\n"
        "LEFT JOIN tester t ON t.tester_id = r.tester_id\n"
        "LEFT JOIN t3w1_ict i ON i.t3w1_run_id = r.t3w1_run_id\n"
        "LEFT JOIN t3w1_functional f ON f.t3w1_run_id = r.t3w1_run_id\n"
        "LEFT JOIN t3w1_provisioning p ON p.t3w1_run_id = r.t3w1_run_id\n"
    )


def _wide_t3w1_to_long(
    rows: List[Dict[str, Any]],
    melt_columns: Dict[str, List[str]],
) -> List[Dict[str, Any]]:
    """
    Convert wide rows (one run = one row) to long format:
    each ict__*, func__*, prov__* column → one output row with TestName, Value, SN, Date, Station.
    """
    out: List[Dict[str, Any]] = []
    prefix_to_name: List[Tuple[str, str, str]] = []
    if melt_columns.get("t3w1_ict"):
        for c in melt_columns["t3w1_ict"]:
            prefix_to_name.append((f"ict__{c}", c, "t3w1_ict"))
    if melt_columns.get("t3w1_functional"):
        for c in melt_columns["t3w1_functional"]:
            prefix_to_name.append((f"func__{c}", c, "t3w1_functional"))
    if melt_columns.get("t3w1_provisioning"):
        for c in melt_columns["t3w1_provisioning"]:
            prefix_to_name.append((f"prov__{c}", c, "t3w1_provisioning"))
    for row in rows:
        sn = row.get("_sn")
        station = row.get("_station")
        dt = row.get("_run_start") or row.get("_run_db_time")
        raw_dt = None
        if dt is not None:
            try:
                raw_dt = dt.strftime("%Y-%m-%d %H:%M:%S") if hasattr(dt, "strftime") else str(dt)
            except Exception:
                raw_dt = str(dt)
        dev_sn = row.get("_device_sn")
        if prefix_to_name:
            for alias, test_name, origin in prefix_to_name:
                val = row.get(alias)
                prov_desc = _provisioning_desc_with_fallback(val) if origin == "t3w1_provisioning" else None
                out.append({
                    "Station": station,
                    "Source": "T3W1",
                    "TestName": test_name,
                    "Value": val if val is None else (float(val) if isinstance(val, (int, float)) else str(val)),
                    "Unit": None,
                    "SN": sn,
                    "device_sn": dev_sn,
                    "Date": dt,
                    "RawDate": raw_dt,
                    "File": None,
                    "LowerLimit": None,
                    "UpperLimit": None,
                    "Origin": origin,
                    "ProvisioningErrorDescription": prov_desc,
                })
        else:
            # No ict/functional/provisioning columns — emit at least one row per run so UI does not show 0
            run_id = row.get("t3w1_run_id")
            out.append({
                "Station": station,
                "Source": "T3W1",
                "TestName": "_run",
                "Value": run_id,
                "Unit": None,
                "SN": sn,
                "device_sn": dev_sn,
                "Date": dt,
                "RawDate": raw_dt,
                "File": None,
                "LowerLimit": None,
                "UpperLimit": None,
                "Origin": None,
                "ProvisioningErrorDescription": None,
            })
    return out


def _run_t3w1_search(
    conditions: List[str],
    values: List[Any],
    limit: int,
) -> pd.DataFrame:
    """
    Run T3W1 query (run+device+tester+ict+functional+provisioning),
    apply WHERE, LIMIT, melt wide result to long, return DataFrame.
    """
    global _LAST_RUN_STATS
    _LAST_RUN_STATS = None
    cfg = _ensure_config()
    caps = _get_db_caps()
    if not caps.get("has_t3w1"):
        _LAST_RUN_STATS = {
            "database": cfg.database,
            "raw_rows": 0,
            "melt_columns": {},
            "long_rows": 0,
            "first_row_keys": [],
        }
        return _rows_to_dataframe([])

    melt = _get_t3w1_melt_columns()
    melt_counts = {k: len(v) for k, v in melt.items()}
    base = _build_t3w1_select_and_from(melt)
    where = "\nWHERE " + " AND ".join(conditions) if conditions else ""
    if cfg.db_profile == "manufacturing":
        order_clause = "ORDER BY r.db_server_time DESC, r.t3w1_run_id DESC"
    else:
        order_clause = "ORDER BY r.start_test DESC, r.t3w1_run_id DESC"
    sql = f"{base}{where}\n{order_clause}\nLIMIT %s"
    params = values + [int(limit)]
    rows = _execute_sql(sql, params)
    long_rows = _wide_t3w1_to_long(rows, melt)
    _LAST_RUN_STATS = {
        "database": cfg.database,
        "raw_rows": len(rows),
        "melt_columns": melt_counts,
        "long_rows": len(long_rows),
        "first_row_keys": list(rows[0].keys()) if rows else [],
    }
    return _rows_to_dataframe(long_rows)


def _rows_to_dataframe(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Convert DB rows to a DataFrame compatible with the rest of the app.

    Expects SQL SELECT to return at least these aliases:
    - Station      AS Station
    - TestName     AS TestName
    - Value        AS Value
    - Unit         AS Unit
    - SerialNumber AS SN
    - Timestamp    AS Date
    Optional:
    - RawDate      AS RawDate
    - FileName     AS File
    """
    if not rows:
        return pd.DataFrame(
            columns=[
                "Station",
                "Source",
                "TestName",
                "Value",
                "Unit",
                "SN",
                "device_sn",
                "Date",
                "RawDate",
                "File",
                "LowerLimit",
                "UpperLimit",
                "Origin",
                "ProvisioningErrorDescription",
            ]
        )
    df = pd.DataFrame(rows)
    for col in [
        "Station",
        "Source",
        "TestName",
        "Value",
        "Unit",
        "SN",
        "device_sn",
        "Date",
        "RawDate",
        "File",
        "LowerLimit",
        "UpperLimit",
        "Origin",
        "ProvisioningErrorDescription",
    ]:
        if col not in df.columns:
            df[col] = None
    df = df[
        [
            "Station",
            "Source",
            "TestName",
            "Value",
            "Unit",
            "SN",
            "device_sn",
            "Date",
            "RawDate",
            "File",
            "LowerLimit",
            "UpperLimit",
            "Origin",
            "ProvisioningErrorDescription",
        ]
    ]
    return df


# FATP table names used for dynamic UNION when filtering by sources
FATP_TABLE_NAMES = ("fatpfinal", "fatprf", "fatpsub")


def _build_fatp_base_select(fatp_tables: Optional[List[str]] = None) -> str:
    """
    Base SELECT for fatpfinal/fatprf/fatpsub — same shape as other paths.
    fatp_tables: which tables to include (None = all three).
    """
    tables = fatp_tables if fatp_tables else list(FATP_TABLE_NAMES)
    if not tables:
        return "SELECT 1 AS _empty WHERE 1=0"
    cols = (
        "    serial_number,\n"
        "    operation,\n"
        "    tester_id,\n"
        "    test_name,\n"
        "    measurement_name,\n"
        "    value,\n"
        "    units,\n"
        "    lower_limit,\n"
        "    upper_limit,\n"
        "    start_datetime,\n"
        "    pega_code\n"
    )
    parts = []
    for tbl in tables:
        if tbl not in FATP_TABLE_NAMES:
            continue
        parts.append(
            f"    SELECT\n"
            f"      '{tbl}' AS SourceTable,\n"
            f"{cols}\n"
            f"    FROM {tbl}"
        )
    if not parts:
        return "SELECT 1 AS _empty WHERE 1=0"
    union_inner = "\n    UNION ALL\n".join(parts)
    return (
        "SELECT\n"
        "  m.Station,\n"
        "  m.Source,\n"
        "  m.TestName,\n"
        "  m.Value,\n"
        "  m.Unit,\n"
        "  m.SN,\n"
        "  m.Date,\n"
        "  m.RawDate,\n"
        "  m.File,\n"
        "  m.LowerLimit,\n"
        "  m.UpperLimit\n"
        "FROM (\n"
        "  SELECT\n"
        "    COALESCE(t.description, f.operation) AS Station,\n"
        "    f.test_name AS TestName,\n"
        "    NULLIF(f.value, '') AS Value,\n"
        "    f.units AS Unit,\n"
        "    f.serial_number AS SN,\n"
        "    f.start_datetime AS Date,\n"
        "    DATE_FORMAT(f.start_datetime, '%%Y-%%m-%%d %%H:%%i:%%s') AS RawDate,\n"
        "    f.pega_code AS File,\n"
        "    CAST(NULLIF(f.lower_limit, '') AS DECIMAL(20,8)) AS LowerLimit,\n"
        "    CAST(NULLIF(f.upper_limit, '') AS DECIMAL(20,8)) AS UpperLimit,\n"
        "    f.SourceTable AS Source\n"
        "  FROM (\n"
        + union_inner
        + "\n  ) AS f\n"
        "  LEFT JOIN tester AS t ON t.tester_id = f.tester_id\n"
        ") AS m\n"
    )


def _run_fatp_search(
    conditions: List[str],
    values: List[Any],
    limit: int,
    *,
    fatp_tables: Optional[List[str]] = None,
    test_names: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    Query fatpfinal/fatprf/fatpsub with the same aliases as T3W1 long-form output.
    fatp_tables: tables to include (None = all). test_names: TestName IN (...) filter.
    """
    caps = _get_db_caps()
    if not caps.get("has_fatp"):
        return _rows_to_dataframe([])
    if fatp_tables is not None and len(fatp_tables) == 0:
        return _rows_to_dataframe([])

    base = _build_fatp_base_select(fatp_tables) + "WHERE 1=1\n"
    params: List[Any] = list(values)
    if conditions:
        base += "  AND " + "\n  AND ".join(conditions) + "\n"
    if test_names:
        placeholders = ", ".join(["%s"] * len(test_names))
        base += f"  AND m.TestName IN ({placeholders})\n"
        params.extend(test_names)
    sql = base + "ORDER BY m.Date DESC\nLIMIT %s"
    params.append(int(limit))
    rows = _execute_sql(sql, params)
    return _rows_to_dataframe(rows)


def search_by_serial(
    serial_number: str,
    *,
    limit: int = 10_000,
) -> pd.DataFrame:
    """
    Search by serial number (SN) in T3W1 and fatpfinal/fatprf/fatpsub.
    """
    df_t3w1 = _run_t3w1_search(
        conditions=["(d.device_sn_man = %s OR d.device_sn = %s)"],
        values=[serial_number, serial_number],
        limit=limit,
    )
    cfg = _ensure_config()
    if cfg.db_profile == "manufacturing":
        return df_t3w1
    df_fatp = _run_fatp_search(
        conditions=["m.SN = %s"],
        values=[serial_number],
        limit=limit,
    )
    if df_t3w1.empty:
        return df_fatp
    if df_fatp.empty:
        return df_t3w1
    return pd.concat([df_t3w1, df_fatp], ignore_index=True)


def search_by_date_range(
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    *,
    station: Optional[str] = None,
    limit: int = 50_000,
) -> pd.DataFrame:
    """
    Search by time range (r.start_test) and optional station (t.description).

    Expects ISO-like date strings, e.g. \"2026-02-01\" or \"2026-02-01 00:00:00\".
    """
    tc = _t3w1_run_time_col_sql()
    conditions: List[str] = []
    values: List[Any] = []
    if date_from:
        conditions.append(f"{tc} >= %s")
        values.append(date_from)
    if date_to:
        conditions.append(f"{tc} <= %s")
        values.append(date_to)
    if station:
        conditions.append("t.description = %s")
        values.append(station)
    df_t3w1 = _run_t3w1_search(conditions=conditions, values=values, limit=limit)

    cfg = _ensure_config()
    if cfg.db_profile == "manufacturing":
        return df_t3w1

    fatp_conditions: List[str] = []
    fatp_values: List[Any] = []
    if date_from:
        fatp_conditions.append("m.Date >= %s")
        fatp_values.append(date_from)
    if date_to:
        fatp_conditions.append("m.Date <= %s")
        fatp_values.append(date_to)
    if station:
        fatp_conditions.append("m.Station = %s")
        fatp_values.append(station)
    df_fatp = _run_fatp_search(conditions=fatp_conditions, values=fatp_values, limit=limit)

    if df_t3w1.empty:
        return df_fatp
    if df_fatp.empty:
        return df_t3w1
    return pd.concat([df_t3w1, df_fatp], ignore_index=True)


def search_by_station_and_test(
    station: str,
    test_name: str,
    *,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    limit: int = 50_000,
) -> pd.DataFrame:
    """
    Search for a given station and test (TestName filter applied after melt).
    """
    tc = _t3w1_run_time_col_sql()
    conditions: List[str] = ["t.description = %s"]
    values: List[Any] = [station]
    if date_from:
        conditions.append(f"{tc} >= %s")
        values.append(date_from)
    if date_to:
        conditions.append(f"{tc} <= %s")
        values.append(date_to)
    df_t3w1 = _run_t3w1_search(conditions=conditions, values=values, limit=limit)
    if test_name and not df_t3w1.empty:
        df_t3w1 = df_t3w1.loc[df_t3w1["TestName"].astype(str) == str(test_name)]

    cfg = _ensure_config()
    if cfg.db_profile == "manufacturing":
        return df_t3w1

    fatp_conditions: List[str] = ["m.Station = %s", "m.TestName = %s"]
    fatp_values: List[Any] = [station, test_name]
    if date_from:
        fatp_conditions.append("m.Date >= %s")
        fatp_values.append(date_from)
    if date_to:
        fatp_conditions.append("m.Date <= %s")
        fatp_values.append(date_to)
    df_fatp = _run_fatp_search(conditions=fatp_conditions, values=fatp_values, limit=limit)

    if df_t3w1.empty:
        return df_fatp
    if df_fatp.empty:
        return df_t3w1
    return pd.concat([df_t3w1, df_fatp], ignore_index=True)


def search_generic(
    *,
    serial_number: Optional[str] = None,
    station: Optional[str] = None,
    test_name: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    min_value: Optional[float] = None,
    max_value: Optional[float] = None,
    limit: int = 50_000,
    sources: Optional[List[str]] = None,
    test_names: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    Generic search — combines filters (SN, station, date, test, value bounds)
    over T3W1 and fatpfinal/fatprf/fatpsub.
    sources: restrict to these sources (e.g. ["T3W1", "fatpsub"]). None = all.
    test_names: restrict to these tests (multiple). None / empty = all.
    """
    run_t3w1 = sources is None or "T3W1" in sources
    fatp_tables: Optional[List[str]] = None
    if sources is not None:
        fatp_tables = [t for t in FATP_TABLE_NAMES if t in sources]

    # T3W1 path — SN/station/date in SQL; test+values after melt to long format.
    conditions: List[str] = []
    values: List[Any] = []
    if serial_number:
        conditions.append("(d.device_sn_man = %s OR d.device_sn = %s)")
        values.append(serial_number)
        values.append(serial_number)
    if station:
        conditions.append("t.description = %s")
        values.append(station)
    tc = _t3w1_run_time_col_sql()
    if date_from:
        conditions.append(f"{tc} >= %s")
        values.append(date_from)
    if date_to:
        conditions.append(f"{tc} <= %s")
        values.append(date_to)

    df_t3w1 = _rows_to_dataframe([])
    if run_t3w1:
        df_t3w1 = _run_t3w1_search(conditions=conditions, values=values, limit=limit)
    if not df_t3w1.empty:
        if test_name:
            df_t3w1 = df_t3w1.loc[df_t3w1["TestName"].astype(str) == str(test_name)]
        if test_names:
            test_set = set(str(t) for t in test_names)
            df_t3w1 = df_t3w1.loc[df_t3w1["TestName"].astype(str).isin(test_set)]
        if min_value is not None:
            mn = float(min_value)
            df_t3w1 = df_t3w1.loc[
                df_t3w1["Value"].apply(
                    lambda x: x is not None and _safe_float(x) is not None and _safe_float(x) >= mn
                )
            ]
        if max_value is not None:
            mx = float(max_value)
            df_t3w1 = df_t3w1.loc[
                df_t3w1["Value"].apply(
                    lambda x: x is None or _safe_float(x) is None or _safe_float(x) <= mx
                )
            ]

    cfg = _ensure_config()
    if cfg.db_profile == "manufacturing":
        return df_t3w1

    # FATP path — filters in SQL (pegatron; manufacturing-db often has no FATP).
    fatp_conditions: List[str] = []
    fatp_values: List[Any] = []
    if serial_number:
        fatp_conditions.append("m.SN = %s")
        fatp_values.append(serial_number)
    if station:
        fatp_conditions.append("m.Station = %s")
        fatp_values.append(station)
    if test_name:
        fatp_conditions.append("m.TestName = %s")
        fatp_values.append(test_name)
    if date_from:
        fatp_conditions.append("m.Date >= %s")
        fatp_values.append(date_from)
    if date_to:
        fatp_conditions.append("m.Date <= %s")
        fatp_values.append(date_to)
    if min_value is not None:
        fatp_conditions.append("m.Value >= %s")
        fatp_values.append(float(min_value))
    if max_value is not None:
        fatp_conditions.append("m.Value <= %s")
        fatp_values.append(float(max_value))
    df_fatp = _run_fatp_search(
        conditions=fatp_conditions,
        values=fatp_values,
        limit=limit,
        fatp_tables=fatp_tables,
        test_names=test_names if test_names else None,
    )

    if df_t3w1.empty:
        return df_fatp
    if df_fatp.empty:
        return df_t3w1
    return pd.concat([df_t3w1, df_fatp], ignore_index=True)


def get_last_run_stats() -> Optional[Dict[str, Any]]:
    """
    Return diagnostics from the last T3W1 search (raw_rows, melt_columns, long_rows).
    Use when results are empty to see which stage dropped data.
    """
    return _LAST_RUN_STATS


__all__ = [
    "configure_connection",
    "clear_connection",
    "search_by_serial",
    "search_by_date_range",
    "search_by_station_and_test",
    "search_generic",
    "get_last_run_stats",
    "DBConnectionError",
    "DBQueryError",
]

