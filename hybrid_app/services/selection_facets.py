"""
Facets for cascaded selection in Detailed analysis: tester type -> tests -> stations.
"""
from __future__ import annotations

import pandas as pd

# Same mapping as in sn_metadata (DB sources)
SOURCE_TO_KIND_LABEL: dict[str, str] = {
    "T3W1": "PCBA",
    "fatpsub": "SUB-ASSY",
    "fatprf": "RF BOX",
    "fatpfinal": "FINAL",
}

# Preferred order in UI (others alphabetically after these)
_KIND_ORDER: tuple[str, ...] = (
    "PCBA",
    "RF",
    "RF BOX",
    "FT",
    "FINAL",
    "SUB-ASSY",
    "SUS-ASSY",
)


def _kind_sort_key(k: str) -> tuple[int, int | str]:
    k = str(k).strip()
    try:
        return (0, _KIND_ORDER.index(k))
    except ValueError:
        return (1, k.lower())


def add_tester_kind_series(df: pd.DataFrame) -> pd.Series:
    """
    Tester kind: prefer Operation (CSV), otherwise mapped Source (DB).
    """
    n = len(df)
    out = pd.Series(["Other"] * n, index=df.index, dtype=object)
    if "Operation" in df.columns:
        op = df["Operation"].fillna("").astype(str).str.strip()
        m = op != ""
        out.loc[m] = op.loc[m]
    if "Source" in df.columns:
        def map_source(s: object) -> str:
            if pd.isna(s):
                return "Other"
            key = str(s).strip()
            if not key:
                return "Other"
            return SOURCE_TO_KIND_LABEL.get(key, key)

        src_mapped = df["Source"].map(map_source)
        fill = out.eq("Other")
        out.loc[fill] = src_mapped.loc[fill]
    return out


def build_selection_facets(df: pd.DataFrame) -> dict:
    """
    Returns structure for cascaded UI:
    - kinds: sorted unique tester kinds
    - testsByKind: kind -> [tests]
    - stationsByKindTest: kind -> { test: [stations] }
    """
    if df.empty or "TestName" not in df.columns:
        return {"kinds": [], "testsByKind": {}, "stationsByKindTest": {}, "allStations": []}

    df = df.copy()
    df["_kind"] = add_tester_kind_series(df)

    all_stations: list[str] = []
    if "Station" in df.columns:
        all_stations = sorted(df["Station"].dropna().astype(str).unique().tolist())

    kinds = sorted(df["_kind"].dropna().astype(str).unique().tolist(), key=_kind_sort_key)
    tests_by_kind: dict[str, list[str]] = {}
    stations_by_kind_test: dict[str, dict[str, list[str]]] = {}

    for k in kinds:
        sub = df[df["_kind"].astype(str) == k]
        tests = sorted(sub["TestName"].dropna().astype(str).unique().tolist())
        tests_by_kind[k] = tests
        stations_by_kind_test[k] = {}
        for t in tests:
            sub2 = sub[sub["TestName"].astype(str) == t]
            if "Station" in sub2.columns:
                st_list = sorted(sub2["Station"].dropna().astype(str).unique().tolist())
            else:
                st_list = []
            stations_by_kind_test[k][t] = st_list

    return {
        "kinds": kinds,
        "testsByKind": tests_by_kind,
        "stationsByKindTest": stations_by_kind_test,
        "allStations": all_stations,
    }
