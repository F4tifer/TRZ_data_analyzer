from __future__ import annotations

import os
from pathlib import Path

import pandas as pd


def _artifact_dir() -> Path:
    # Keep artifacts inside the repo workspace by default.
    base = os.getenv("ANALYZER_ARTIFACT_DIR")
    if base:
        return Path(base).expanduser().resolve()
    return Path(__file__).resolve().parent.parent / ".artifacts"


def artifact_path(run_id: str) -> Path:
    return _artifact_dir() / f"{run_id}.pkl"


def save_run_df(run_id: str, df: pd.DataFrame) -> Path:
    path = artifact_path(run_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_pickle(path)
    return path


def load_run_df(run_id: str) -> pd.DataFrame:
    path = artifact_path(run_id)
    return pd.read_pickle(path)


def delete_run_artifact(run_id: str) -> None:
    p = artifact_path(run_id)
    if p.is_file():
        p.unlink()

