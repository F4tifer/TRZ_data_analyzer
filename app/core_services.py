"""
Shared business logic used by analytics frontends.
This module intentionally avoids UI-specific dependencies.
"""
import os
import re
import socket
import logging
import zipfile
import tarfile
import shutil
import tempfile
import json
from concurrent.futures import ThreadPoolExecutor as _ThreadPoolExecutor
from typing import List, Tuple, Optional

import pandas as pd
import numpy as np
from ftplib import FTP

from config_loader import load_metadata_config

from app.constants import HAS_SKLEARN, HAS_PARAMIKO
from data_loader import load_data as _fs_load_data
from app import db_search
from app.provisioning_error_codes import evaluate_provisioning_status

if HAS_SKLEARN:
    from sklearn.ensemble import IsolationForest
else:
    IsolationForest = None

# Absolute path to `limits_config.json` in repository root.
_BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_TARGET_LIMIT_SHEETS = ("FATP SUB FCT Test", "RF", "FATP Final FCT Test")
_TESTER_LIMITS_ENV = "ANALYZER_TESTER_LIMITS_PATH"
_TESTER_LIMITS_CACHE: dict = {"path": None, "mtime": None, "data": {"by_tester": {}, "by_test": {}}}


def _norm_key(value: object) -> str:
    s = str(value or "").strip().lower()
    if not s:
        return ""
    return re.sub(r"\s+", " ", s)


def _tester_aliases(tester_name: str) -> set[str]:
    n = _norm_key(tester_name)
    aliases = {n}
    if "rf" in n:
        aliases.update({"rf", "rf tester"})
    if "sub" in n:
        aliases.update({"fatp sub fct test", "sub fct", "sub-assy", "sub assy"})
    if "final" in n or n == "ft":
        aliases.update({"fatp final fct test", "final fct", "ft", "fatp final fct"})
    return {a for a in aliases if a}


def _to_float_or_none(value: object) -> Optional[float]:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value).strip().replace(",", ".")
    if not s:
        return None
    try:
        return float(s)
    except Exception:
        return None


def _load_limits_from_csv(path: str) -> dict:
    df = pd.read_csv(path)
    cols = {c.lower().strip(): c for c in df.columns}
    tester_col = cols.get("tester_sheet") or cols.get("tester")
    test_col = cols.get("test_name") or cols.get("test")
    lo_col = cols.get("lower_limit") or cols.get("lower")
    hi_col = cols.get("upper_limit") or cols.get("upper")
    if not (tester_col and test_col and lo_col and hi_col):
        return {"by_tester": {}, "by_test": {}}
    by_tester: dict[str, dict[str, tuple[float, float]]] = {}
    by_test: dict[str, tuple[float, float]] = {}
    for _, row in df.iterrows():
        lo = _to_float_or_none(row.get(lo_col))
        hi = _to_float_or_none(row.get(hi_col))
        if lo is None or hi is None:
            continue
        test_norm = _norm_key(row.get(test_col))
        tester_norm = _norm_key(row.get(tester_col))
        if not test_norm or not tester_norm:
            continue
        for alias in _tester_aliases(tester_norm):
            by_tester.setdefault(alias, {})[test_norm] = (lo, hi)
        by_test[test_norm] = by_test.get(test_norm, (lo, hi))
    return {"by_tester": by_tester, "by_test": by_test}


def _load_limits_from_xlsx(path: str) -> dict:
    by_tester: dict[str, dict[str, tuple[float, float]]] = {}
    by_test: dict[str, tuple[float, float]] = {}
    for sheet in _TARGET_LIMIT_SHEETS:
        try:
            df = pd.read_excel(path, sheet_name=sheet, header=None)
        except Exception:
            continue
        for _, row in df.iterrows():
            test_name = row.iloc[0] if len(row) > 0 else None
            lo = _to_float_or_none(row.iloc[6] if len(row) > 6 else None)
            hi = _to_float_or_none(row.iloc[7] if len(row) > 7 else None)
            if lo is None or hi is None:
                continue
            test_norm = _norm_key(test_name)
            if not test_norm or test_norm in {"status", "dark room", "sub fct", "final fct"}:
                continue
            for alias in _tester_aliases(sheet):
                by_tester.setdefault(alias, {})[test_norm] = (lo, hi)
            by_test[test_norm] = by_test.get(test_norm, (lo, hi))
    return {"by_tester": by_tester, "by_test": by_test}


def _resolve_tester_limits_path() -> Optional[str]:
    env_path = (os.getenv(_TESTER_LIMITS_ENV, "") or "").strip()
    if env_path and os.path.isfile(env_path):
        return env_path
    for rel in ("ts7_limits_extracted.csv", "tester_limits.csv", "tester_limits.xlsx"):
        p = os.path.join(_BASE_DIR, rel)
        if os.path.isfile(p):
            return p
    return None


def _load_tester_test_limits() -> dict:
    path = _resolve_tester_limits_path()
    if not path:
        return {"by_tester": {}, "by_test": {}}
    try:
        mtime = os.path.getmtime(path)
    except OSError:
        return {"by_tester": {}, "by_test": {}}
    if _TESTER_LIMITS_CACHE.get("path") == path and _TESTER_LIMITS_CACHE.get("mtime") == mtime:
        return _TESTER_LIMITS_CACHE.get("data", {"by_tester": {}, "by_test": {}})
    data = {"by_tester": {}, "by_test": {}}
    try:
        ext = os.path.splitext(path)[1].lower()
        if ext == ".csv":
            data = _load_limits_from_csv(path)
        elif ext in (".xlsx", ".xlsm", ".xls"):
            data = _load_limits_from_xlsx(path)
    except Exception:
        data = {"by_tester": {}, "by_test": {}}
    _TESTER_LIMITS_CACHE["path"] = path
    _TESTER_LIMITS_CACHE["mtime"] = mtime
    _TESTER_LIMITS_CACHE["data"] = data
    return data


def _row_tester_keys(row) -> list[str]:
    keys: list[str] = []
    for col in ("Operation", "Source", "Tester", "Station"):
        if col in row.index:
            n = _norm_key(row[col])
            if n:
                keys.extend(list(_tester_aliases(n)))
    # unique preserve order
    out: list[str] = []
    seen = set()
    for k in keys:
        if k in seen:
            continue
        seen.add(k)
        out.append(k)
    return out


def _get_ftp_logger() -> logging.Logger:
    logger = logging.getLogger("trezor_ftp")
    if logger.handlers:
        return logger
    logger.setLevel(logging.DEBUG)
    log_dir = os.path.expanduser("~/trezor_log_analyzer_logs")
    os.makedirs(log_dir, exist_ok=True)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    for level, name in [(logging.DEBUG, "ftp_debug"), (logging.INFO, "ftp_info"), (logging.ERROR, "ftp_error")]:
        h = logging.FileHandler(os.path.join(log_dir, f"{name}.log"), encoding="utf-8")
        h.setLevel(level)
        h.setFormatter(fmt)
        logger.addHandler(h)
    return logger


def _ymd_to_date_str(ymd: Optional[int]) -> Optional[str]:
    """Helper: convert int YYYYMMDD to ISO-like date string YYYY-MM-DD."""
    if ymd is None:
        return None
    s = str(ymd)
    if len(s) == 8 and s.isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    return None


def load_data_from_db(
    db_url: str,
    db_user: str,
    db_pass: str,
    date_from_ymd: Optional[int],
    date_to_ymd: Optional[int],
    *,
    limit: int = 50_000,
    sources: Optional[List[str]] = None,
    test_names: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    Load measurements from SQL DB via app.db_search, returning unified DataFrame.
    sources: restrict to these (e.g. ["T3W1", "fatpsub"]). None = all.
    test_names: restrict to these test names. None / empty = all.
    """
    date_from_str = _ymd_to_date_str(date_from_ymd)
    date_to_str = _ymd_to_date_str(date_to_ymd)

    if not (db_url and db_user and db_pass):
        raise ValueError("DB URL / user / password is not fully specified.")
    db_search.configure_connection(db_url, db_user, db_pass)
    return db_search.search_generic(
        date_from=date_from_str,
        date_to=date_to_str,
        limit=int(limit) if limit and limit > 0 else 50_000,
        sources=sources,
        test_names=test_names if test_names else None,
    )


def load_data_unified(
    source_kind: str,
    *,
    path: Optional[str],
    date_from_ymd: Optional[int],
    date_to_ymd: Optional[int],
    db_url: Optional[str],
    db_user: Optional[str],
    db_pass: Optional[str],
    db_limit: int = 50_000,
    debug_log_path: Optional[str] = None,
    lang: str = "EN",
    load_sources: Optional[List[str]] = None,
    load_tests: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    Unified data access layer for UI:

    - For filesystem sources (local folder / FTP synced folder):
      uses data_loader.load_data, then filters by load_sources (Source) and load_tests (TestName) if given.
    - For DB sources:
      uses app.db_search.search_generic with sources and test_names to limit data at query time.
    """
    if source_kind == "DB (SQL)":
        if not (db_url and db_user and db_pass):
            raise ValueError("DB URL / user / password is not fully specified.")
        df = load_data_from_db(
            db_url=db_url,
            db_user=db_user,
            db_pass=db_pass,
            date_from_ymd=date_from_ymd,
            date_to_ymd=date_to_ymd,
            limit=db_limit,
            sources=load_sources,
            test_names=load_tests,
        )
        return df

    # Default: filesystem-based logs
    if not path:
        raise ValueError("Path must be provided for filesystem source.")
    df = _fs_load_data(
        path,
        date_from_ymd=date_from_ymd,
        date_to_ymd=date_to_ymd,
        debug_log_path=debug_log_path,
        lang=lang,
    )
    if df.empty:
        return df
    if load_sources and len(load_sources) > 0 and "Source" in df.columns:
        df = df.loc[df["Source"].astype(str).isin([str(s) for s in load_sources])]
    if load_tests and len(load_tests) > 0 and "TestName" in df.columns:
        test_set = set(str(t) for t in load_tests)
        df = df.loc[df["TestName"].astype(str).isin(test_set)]
    return df


_META_CFG = load_metadata_config()
_META_LOG_TARGETS: Tuple[str, ...] = tuple(
    {f["log_file"] for f in _META_CFG.get("fields", []) if f.get("log_file")}
)
SFTP_ARCHIVE_WORKERS = 2


def _open_sftp_client(
    host: str,
    port: int,
    user: str,
    pkey_file: Optional[str] = None,
    pkey_bytes: Optional[bytes] = None,
    passphrase: Optional[str] = None,
    password: Optional[str] = None,
    connect_timeout: int = 10,
):
    if not HAS_PARAMIKO:
        raise ImportError("paramiko required for SFTP")
    import paramiko
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    pkey = None
    if pkey_file and os.path.isfile(pkey_file):
        try:
            pkey = paramiko.RSAKey.from_private_key_file(pkey_file, password=passphrase or None)
        except paramiko.ssh_exception.SSHException:
            pkey = paramiko.Ed25519Key.from_private_key_file(pkey_file, password=passphrase or None)
    elif pkey_bytes:
        from io import StringIO
        key_obj = StringIO(pkey_bytes.decode("utf-8", errors="replace").replace("\r\n", "\n"))
        try:
            pkey = paramiko.RSAKey.from_private_key(key_obj, password=passphrase or None)
        except paramiko.ssh_exception.SSHException:
            key_obj.seek(0)
            pkey = paramiko.Ed25519Key.from_private_key(key_obj, password=passphrase or None)
    if pkey:
        client.connect(host, port=port, username=user, pkey=pkey, timeout=connect_timeout)
    elif password:
        client.connect(host, port=port, username=user, password=password, timeout=connect_timeout)
    else:
        raise ValueError("SSH key or password required")
    return client, client.open_sftp()


def _process_remote_zip_index(sftp, remote_zip_path: str, local_root: str, targets: Tuple[str, ...], logger: logging.Logger) -> int:
    zip_name = os.path.basename(remote_zip_path)
    extracted = 0
    tmp_zip_dir = os.path.join(tempfile.gettempdir(), "trezor_log_analyzer_zipcache")
    os.makedirs(tmp_zip_dir, exist_ok=True)
    tmp_zip_path = os.path.join(tmp_zip_dir, zip_name)
    try:
        with sftp.open(remote_zip_path, "rb") as src, open(tmp_zip_path, "wb") as dst:
            shutil.copyfileobj(src, dst)
        lower_name = zip_name.lower()
        if lower_name.endswith(".zip"):
            with zipfile.ZipFile(tmp_zip_path, "r") as zf:
                members = zf.infolist()
                matched = [m for m in members if any(t in m.filename for t in targets)]
                for m in matched:
                    try:
                        rel_name = m.filename.lstrip("/\\")
                        out_path = os.path.join(local_root, rel_name)
                        os.makedirs(os.path.dirname(out_path), exist_ok=True)
                        with zf.open(m, "r") as src, open(out_path, "wb") as dst:
                            shutil.copyfileobj(src, dst)
                        extracted += 1
                    except Exception:
                        logger.error("Error extracting '%s' from ZIP '%s'", m.filename, zip_name, exc_info=True)
        else:
            mode = "r:*"
            with tarfile.open(tmp_zip_path, mode) as tf:
                members = [m for m in tf.getmembers() if m.isreg()]
                matched = [m for m in members if any(t in m.name for t in targets)]
                for m in matched:
                    try:
                        rel_name = m.name.lstrip("/\\")
                        out_path = os.path.join(local_root, rel_name)
                        os.makedirs(os.path.dirname(out_path), exist_ok=True)
                        with tf.extractfile(m) as src, open(out_path, "wb") as dst:
                            if src is not None:
                                shutil.copyfileobj(src, dst)
                        extracted += 1
                    except Exception:
                        logger.error("Error extracting '%s' from TAR '%s'", m.name, zip_name, exc_info=True)
    finally:
        try:
            if os.path.exists(tmp_zip_path):
                os.remove(tmp_zip_path)
        except Exception:
            pass
    return extracted


def sync_ftp(host: str, user: str, pwd: str, remote_dir: str, local_dir: str) -> Tuple[int, Optional[str]]:
    logger = _get_ftp_logger()
    try:
        ftp = FTP(host, timeout=10)
        ftp.login(user, pwd)
        ftp.cwd(remote_dir)
        files = ftp.nlst()
        os.makedirs(local_dir, exist_ok=True)
        count = 0
        for file in files:
            if file == "detail.log" or (file.endswith(".csv") and re.match(r"^\d+_\d+\.csv$", file)):
                local_path = os.path.join(local_dir, file)
                if not os.path.exists(local_path):
                    with open(local_path, "wb") as lf:
                        ftp.retrbinary("RETR " + file, lf.write)
                    count += 1
        ftp.quit()
        return count, None
    except Exception as e:
        logger.error("FTP sync failed: %s", e, exc_info=True)
        return 0, str(e)


def sync_sftp(
    host: str,
    user: str,
    local_dir: str,
    remote_dir: str = "/",
    port: int = 22,
    pkey_file: Optional[str] = None,
    pkey_bytes: Optional[bytes] = None,
    passphrase: Optional[str] = None,
    password: Optional[str] = None,
    connect_timeout: int = 10,
    date_from_ymd: Optional[int] = None,
    date_to_ymd: Optional[int] = None,
) -> Tuple[int, Optional[str]]:
    if not HAS_PARAMIKO:
        return 0, "Module paramiko not installed. Run: pip install paramiko"
    import paramiko
    from app.utils import extract_ymd_from_name as _extract_ymd_from_name
    logger = _get_ftp_logger()
    zip_targets: Tuple[str, ...] = ("detail.log", ".csv") + _META_LOG_TARGETS
    last_err = None
    for attempt in range(1, 4):
        client, sftp = None, None
        try:
            client, sftp = _open_sftp_client(
                host, port, user,
                pkey_file=pkey_file, pkey_bytes=pkey_bytes, passphrase=passphrase, password=password,
                connect_timeout=connect_timeout,
            )
            count = 0
            loose_tasks = []
            zip_tasks = []

            def _walk(remote: str, local_root: str):
                nonlocal count
                try:
                    for entry in sftp.listdir_attr(remote):
                        rpath = remote + "/" + entry.filename if not remote.endswith("/") else remote + entry.filename
                        lpath = os.path.join(local_root, entry.filename)
                        if entry.st_mode is not None and (entry.st_mode & 0o170000) == 0o040000:
                            os.makedirs(lpath, exist_ok=True)
                            _walk(rpath, lpath)
                        else:
                            if rpath.lower().endswith((".zip", ".tgz", ".tar.gz")):
                                ymd = _extract_ymd_from_name(entry.filename)
                                if (date_from_ymd is None or ymd is None or ymd >= date_from_ymd) and (date_to_ymd is None or ymd is None or ymd <= date_to_ymd):
                                    zip_tasks.append((rpath, local_root))
                            else:
                                if "detail.log" in rpath or (rpath.endswith(".csv") and re.match(r".*\d+_\d+\.csv$", rpath)):
                                    loose_tasks.append((rpath, lpath))
                                    os.makedirs(os.path.dirname(lpath), exist_ok=True)
                except (OSError, socket.timeout) as e:
                    logger.error("SFTP walk error in '%s': %s", remote, e, exc_info=True)

            _walk(remote_dir.rstrip("/") or ".", local_dir)
            for rpath, lpath in loose_tasks:
                try:
                    st = sftp.stat(rpath)
                    if not os.path.exists(lpath) or os.path.getsize(lpath) != st.st_size:
                        sftp.get(rpath, lpath)
                        count += 1
                except Exception as e:
                    logger.error("SFTP get '%s': %s", rpath, e, exc_info=True)

            def _process_one_zip(task):
                rpath, local_root = task
                sub_client, sub_sftp = None, None
                try:
                    sub_client, sub_sftp = _open_sftp_client(
                        host, port, user,
                        pkey_file=pkey_file, pkey_bytes=pkey_bytes, passphrase=passphrase, password=password,
                        connect_timeout=connect_timeout,
                    )
                    _process_remote_zip_index(sub_sftp, rpath, local_root, zip_targets, logger)
                except Exception as e:
                    logger.error("Parallel ZIP '%s': %s", rpath, e, exc_info=True)
                finally:
                    if sub_sftp:
                        try:
                            sub_sftp.close()
                        except Exception:
                            pass
                    if sub_client:
                        try:
                            sub_client.close()
                        except Exception:
                            pass

            workers = min(SFTP_ARCHIVE_WORKERS, len(zip_tasks)) if zip_tasks else 0
            if workers > 0:
                list(_ThreadPoolExecutor(max_workers=workers).map(_process_one_zip, zip_tasks))
            return count, None
        except paramiko.AuthenticationException as e:
            last_err = str(e)
            logger.error("SFTP auth failed: %s", last_err, exc_info=True)
            return 0, last_err
        except Exception as e:
            last_err = str(e)
            logger.error("SFTP sync attempt failed: %s", last_err, exc_info=True)
            if attempt >= 3:
                break
        finally:
            if sftp:
                try:
                    sftp.close()
                except Exception:
                    pass
            if client:
                try:
                    client.close()
                except Exception:
                    pass
    return 0, last_err or "SFTP sync failed"


def test_ftp_connection(host: str, user: str, pwd: str, remote_dir: str) -> Optional[str]:
    try:
        ftp = FTP(host, timeout=10)
        ftp.login(user, pwd)
        if remote_dir:
            ftp.cwd(remote_dir)
        ftp.quit()
        return None
    except Exception as e:
        return str(e)


def test_sftp_connection(
    host: str,
    user: str,
    remote_dir: str = "/",
    port: int = 22,
    pkey_file: Optional[str] = None,
    pkey_bytes: Optional[bytes] = None,
    passphrase: Optional[str] = None,
    password: Optional[str] = None,
    connect_timeout: int = 10,
) -> Optional[str]:
    if not HAS_PARAMIKO:
        return "Module paramiko not installed. Run: pip install paramiko"
    client, sftp = None, None
    try:
        client, sftp = _open_sftp_client(
            host, port, user,
            pkey_file=pkey_file, pkey_bytes=pkey_bytes, passphrase=passphrase, password=password,
            connect_timeout=connect_timeout,
        )
        if remote_dir:
            sftp.chdir(remote_dir)
        return None
    except Exception as e:
        return str(e)
    finally:
        if sftp:
            try:
                sftp.close()
            except Exception:
                pass
        if client:
            try:
                client.close()
            except Exception:
                pass


def limits_config_path() -> str:
    return os.path.join(_BASE_DIR, "limits_config.json")


def load_limits_from_file() -> dict:
    path = limits_config_path()
    if not os.path.isfile(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return {k: (float(v[0]), float(v[1])) for k, v in data.items() if isinstance(v, (list, tuple)) and len(v) >= 2}
    except Exception:
        return {}


def save_limits_to_file(limits: dict) -> None:
    path = limits_config_path()
    data = {k: [float(lo), float(hi)] for k, (lo, hi) in limits.items()}
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def remove_limits_from_file(tests: list) -> None:
    saved = load_limits_from_file()
    for t in tests:
        saved.pop(t, None)
    data = {k: [float(lo), float(hi)] for k, (lo, hi) in saved.items()}
    with open(limits_config_path(), "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def calculate_ai_limits(values: pd.Series, method: str) -> Tuple[float, float]:
    if values.empty or len(values) < 3:
        return (0.0, 100.0)
    values_clean = values.dropna()
    if len(values_clean) < 3:
        return (float(values_clean.min()), float(values_clean.max()))
    try:
        if "IQR" in method:
            q1, q3 = np.percentile(values_clean, 25), np.percentile(values_clean, 75)
            iqr = q3 - q1
            return (float(q1 - 1.5 * iqr), float(q3 + 1.5 * iqr))
        if "Sigma" in method:
            mean, std = np.mean(values_clean), np.std(values_clean)
            return (float(mean - 3 * std), float(mean + 3 * std))
        if "Iso" in method and HAS_SKLEARN and IsolationForest is not None:
            model = IsolationForest(contamination=0.05, random_state=42)
            model.fit(values_clean.values.reshape(-1, 1))
            predictions = model.predict(values_clean.values.reshape(-1, 1))
            inliers = values_clean[predictions == 1]
            if not inliers.empty:
                return (float(inliers.min()), float(inliers.max()))
        return (float(values_clean.min()), float(values_clean.max()))
    except Exception:
        return (float(values_clean.min()), float(values_clean.max()))


def get_unit(df: pd.DataFrame, test_name: str) -> str:
    sub = df[df["TestName"] == test_name]
    if sub.empty:
        return ""
    if "Unit" not in sub.columns:
        return ""
    unit_counts = sub["Unit"].value_counts()
    for unit in unit_counts.index:
        if unit and str(unit).lower() not in ("nan", "times", ""):
            return str(unit)
    return ""


def apply_theme(fig):
    """Aplikuje Trezor theme na Plotly graf."""
    fig.update_layout(
        plot_bgcolor="rgba(31, 31, 31, 0.4)",
        paper_bgcolor="rgba(10, 10, 10, 0.6)",
        font=dict(family="Inter, sans-serif", size=13, color="#FFFFFF"),
        title_font=dict(size=20, color="#FFFFFF", family="Inter", weight=700),
        xaxis=dict(
            gridcolor="rgba(255, 255, 255, 0.06)",
            zerolinecolor="rgba(255, 255, 255, 0.1)",
            showline=True,
            linecolor="rgba(255, 255, 255, 0.15)",
            color="#FFFFFF",
        ),
        yaxis=dict(
            gridcolor="rgba(255, 255, 255, 0.06)",
            zerolinecolor="rgba(255, 255, 255, 0.1)",
            showline=True,
            linecolor="rgba(255, 255, 255, 0.15)",
            color="#FFFFFF",
        ),
        margin=dict(t=70, b=60, l=70, r=40),
        hovermode="x unified",
        hoverlabel=dict(
            bgcolor="rgba(10, 10, 10, 0.95)",
            font_size=13,
            font_family="Inter",
            font_color="#FFFFFF",
            bordercolor="rgba(96, 225, 152, 0.3)",
        ),
        legend=dict(
            bgcolor="rgba(10, 10, 10, 0.8)",
            bordercolor="rgba(255, 255, 255, 0.1)",
            borderwidth=1,
            font=dict(color="#FFFFFF"),
        ),
    )
    return fig


def create_kpi_card(title: str, value: str, icon: str, class_name: str) -> str:
    return f"""
    <div class="kpi-card {class_name}">
        <div class="kpi-icon">{icon}</div>
        <div class="kpi-title">{title}</div>
        <div class="kpi-value">{value}</div>
    </div>
    """


def evaluate_status(row, limits: dict) -> str:
    origin = row["Origin"] if "Origin" in row.index else None
    if origin is not None and not pd.isna(origin) and str(origin) == "t3w1_provisioning":
        return evaluate_provisioning_status(row["Value"])
    test = row["TestName"]
    value = row["Value"]
    if pd.isna(value):
        return "N/A"
    test_key = _norm_key(test)
    # 1) tester+test model from external CSV/XLSX export
    try:
        tester_limits = _load_tester_test_limits()
        by_tester = tester_limits.get("by_tester", {})
        by_test = tester_limits.get("by_test", {})
        for tester_key in _row_tester_keys(row):
            per_tester = by_tester.get(tester_key, {})
            if test_key in per_tester:
                lower, upper = per_tester[test_key]
                return "OK" if lower <= value <= upper else "NOK"
        if test_key in by_test:
            lower, upper = by_test[test_key]
            return "OK" if lower <= value <= upper else "NOK"
    except Exception:
        pass
    # 2) fallback: existing limits dictionary (run-specific or tests_config-derived)
    if test in limits:
        lower, upper = limits[test]
        return "OK" if lower <= value <= upper else "NOK"
    return "N/A"


def get_full_manual(_lang: str) -> str:
    """Return the short built-in manual text (English). Locale argument is legacy / ignored."""
    return "# 📘 Trezor LOG Analyzer — Manual\n\nConnect data source (local folder or FTP), load data, select tests and stations, set limits. Tabs: Dashboard, Detailed analysis, Correlation, Distribution, Trend, Export, Documentation."
