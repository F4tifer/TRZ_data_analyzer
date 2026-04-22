# Trezor LOG Analyzer 

Web application for manufacturing/test log analytics and MySQL/MariaDB data analysis.

## Runtime

- **Single runtime mode:** FastAPI + Jinja2 + HTMX (`hybrid_app`)
- **Entrypoint:** `hybrid_app/main.py`
- **Run command:** `uvicorn hybrid_app.main:app --reload`

## Features

- Local log folder analysis (CSV + `detail.log`)
- MySQL/MariaDB analysis path through `app/db_search.py`
- Run history persisted in SQLite (`analysis_runs`)
- KPI cards, Pareto/pie/extra charts, SN drilldown, export
- Multi-run merged analysis (cross-run SN search + merged exports)

## Repository Structure

```text
.
├── hybrid_app/               # FastAPI app (routes, templates, services)
├── app/                      # shared domain logic (charts, limits, db transforms)
├── config_loader.py          # config loading helpers
├── data_loader.py            # CSV/log loading pipeline
├── requirements.txt
└── pyproject.toml
```

## Quick Start

### 1) Create environment

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
```

Alternative install:

```bash
python -m pip install -r requirements.txt
python -m pip install pytest ruff
```

### 2) Run app

```bash
uvicorn hybrid_app.main:app --reload
```

Open:

- `http://127.0.0.1:8000/` - run form
- `http://127.0.0.1:8000/dashboard` - run history dashboard
- `http://127.0.0.1:8000/api/health` - health endpoint

## Lint

```bash
ruff check hybrid_app app
```

## Configuration

### JSON files

- `tests_config.json` - tests/options/default limits
- `metadata_config.json` - metadata extraction rules
- `limits_config.json` - persisted user limits

### Runtime storage

- `analyzer_meta.db` - SQLite database with runs
- `hybrid_app/.artifacts/` - cached run DataFrame artifacts (`.pkl`)

### Notes

- DB credentials are provided via UI/API and should never be committed.
- For large datasets use date filters and `mysql_row_limit`.

## Developer Notes

- Main app wiring: `hybrid_app/main.py`
- Page routes: `hybrid_app/routes/pages.py`
- API routes: `hybrid_app/routes/api.py`
- Analysis services: `hybrid_app/services/`
- Shared transformations: `app/core_services.py`, `app/db_search.py`

## License / Ownership

Internal Trezor quality tooling project.
