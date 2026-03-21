# data-cbs-interview

A production-grade DataOps pipeline that ingests daily financial price data, transforms it into enriched analytics-ready Parquet files, and loads it into a MotherDuck cloud data warehouse. Built as part of a Senior DataOps Engineer interview task.

---

## Architecture Overview

```
Yahoo Finance (yfinance)
        │
        ▼
  [Ingest]  →  S3 raw bucket        (raw/prices/<SYMBOL>/<DATE>.csv)
        │
        ▼
  [Transform]  →  S3 staging bucket  (processed/prices/<SYMBOL>/<DATE>.parquet)
        │
        ▼
  [Load]  →  MotherDuck              (<ENV>.main.prices)
```

All three stages run as a single Docker container triggered via GitHub Actions. Each stage is **idempotent** — re-runs within the same day are safe and produce no duplicate data.

---

## Tech Stack

| Layer | Tool |
|---|---|
| Language | Python 3.11 |
| Package manager | [uv](https://github.com/astral-sh/uv) |
| Data source | yfinance (Yahoo Finance, no API key required) |
| Raw storage | AWS S3 |
| Staging storage | AWS S3 (Parquet) |
| Secrets | AWS SSM Parameter Store |
| Data warehouse | [MotherDuck](https://motherduck.com) (DuckDB in the cloud) |
| Containerisation | Docker (two-stage build) |
| CI/CD | GitHub Actions |
| Container registry | AWS ECR (separate repos per environment) |
| Region | eu-west-2 |

---

## Repository Structure

```
├── config/
│   ├── ingest.yaml        # yfinance settings, S3 prefix, SSM param names
│   ├── transform.yaml     # Column renames, derived fields, S3 prefixes
│   └── load.yaml          # MotherDuck schema/table, S3 prefix, SSM param names
├── docker/
│   ├── Dockerfile         # Two-stage build (uv builder + slim runtime)
│   └── entrypoint.sh      # Accepts: ingest | transform | load | all
├── src/
│   ├── ingest/
│   │   └── main.py        # yfinance → S3 raw CSV
│   ├── transform/
│   │   └── main.py        # S3 raw CSV → enriched Parquet → S3 staging
│   ├── load/
│   │   └── main.py        # S3 Parquet → MotherDuck
│   └── utils/
│       ├── config_loader.py   # Loads YAML files from config/
│       └── get_parameter.py   # SSM Parameter Store client
├── .github/workflows/
│   ├── ci.yml             # Build + push Docker image on push to dev or main
│   └── pipeline.yml       # Manual trigger: run ingest → transform → load
└── pyproject.toml
```

---

## Pipeline Stages

### 1. Ingest

**Script:** `src/ingest/main.py`

Downloads daily OHLCV data for each configured symbol from Yahoo Finance and uploads it as a raw CSV to S3.

- S3 key: `raw/prices/<SYMBOL>/<YYYY-MM-DD>.csv`
- Idempotent: skips upload if the key already exists for today
- Symbols and bucket name are read from SSM Parameter Store at runtime

### 2. Transform

**Script:** `src/transform/main.py`

Reads all raw CSVs from S3, cleans and enriches the data, and writes Parquet files to the staging bucket.

Transformations applied:
- Normalise column names to lowercase (`Open` → `open`, etc.)
- Add `symbol` column
- Compute `daily_return` (percentage change of close price)
- Compute `moving_avg_10` (10-day rolling average of close price)
- Drop rows with null `date` or `close`

- S3 key: `processed/prices/<SYMBOL>/<YYYY-MM-DD>.parquet`
- Idempotent: skips files already present in the staging bucket

### 3. Load

**Script:** `src/load/main.py`

Reads all Parquet files from the staging bucket and loads them into MotherDuck using DuckDB's native `read_parquet` + S3 integration.

- Target table: `<ENV>.main.prices`
- Schema: `symbol, date, open, high, low, close, volume, daily_return, moving_avg_10`
- Idempotent: `INSERT OR IGNORE` on a `(symbol, date)` primary key
- Database name (`dev` or `prod`) is derived from the `ENV` environment variable

---

## Environments

Two environments are fully isolated across every layer:

| Layer | dev | prod |
|---|---|---|
| SSM namespace | `/data-cbs-interview/dev/` | `/data-cbs-interview/prod/` |
| S3 raw bucket | `data-infra-dev-raw` | `data-infra-prod-raw` |
| S3 staging bucket | `data-infra-dev-staging` | `data-infra-prod-staging` |
| MotherDuck database | `dev` | `prod` |
| ECR repository | `data-infra-dev-data-pipeline` | `data-infra-prod-data-pipeline` |

The `ENV` environment variable (passed into the container at runtime) controls which environment each run targets.

---

## AWS SSM Parameter Store

All secrets and environment-specific values are stored in SSM Parameter Store under `/data-cbs-interview/<ENV>/`. No secrets are stored in code or config files.

| Parameter | Type | Description |
|---|---|---|
| `s3_bucket_raw` | String | Name of the raw S3 bucket |
| `s3_bucket_staging` | String | Name of the staging S3 bucket |
| `symbols` | String | Comma-separated ticker symbols (e.g. `IBM,AAPL,MSFT`) |
| `motherduck_token` | SecureString | MotherDuck service token |
| `aws_region` | String | AWS region override |

---

## GitHub Actions Workflows

### CI (`ci.yml`) — automatic on push

Triggers on every push to `dev` or `main`, and on pull requests to `main`.

| Event | Action |
|---|---|
| Push to `dev` | Build + push image to `ECR_REPO_DEV` |
| Push to `main` | Build + push image to `ECR_REPO_PROD` |
| PR to `main` | Build only (no push) — validates the Dockerfile |

### Pipeline (`pipeline.yml`) — manual trigger only

Go to **Actions → Pipeline → Run workflow**, select an environment (`dev` or `prod`), and run.

Pulls the latest image for the chosen environment from ECR, then runs ingest → transform → load in sequence, passing `ENV=dev` or `ENV=prod` into the container.

---

## Branching Strategy

```
dev   ──► push ──► CI builds dev image ──► manual pipeline run (dev)
 │
 └─► PR to main ──► CI validates build
                         │
                         ▼
main  ──► merge ──► CI builds prod image ──► manual pipeline run (prod)
```

- Work is done on `dev`; the pipeline can be tested end-to-end against the dev environment
- A pull request to `main` triggers a build-only CI check
- Merging to `main` automatically builds and pushes the production image
- The production pipeline is always triggered manually

---

## Running Locally

**Prerequisites:** Python 3.11+, [uv](https://github.com/astral-sh/uv), AWS credentials configured

```bash
# Install dependencies
uv sync

# Run a single stage
uv run python -m src.ingest.main
uv run python -m src.transform.main
uv run python -m src.load.main

# Run all stages
ENV=dev uv run python -m src.ingest.main
ENV=dev uv run python -m src.transform.main
ENV=dev uv run python -m src.load.main
```

**Running with Docker locally:**

```bash
docker build -f docker/Dockerfile -t data-pipeline .

docker run --rm \
  -e AWS_ACCESS_KEY_ID=<key> \
  -e AWS_SECRET_ACCESS_KEY=<secret> \
  -e AWS_DEFAULT_REGION=eu-west-2 \
  -e ENV=dev \
  data-pipeline all
```

---

## MotherDuck Table Schema

```sql
CREATE TABLE IF NOT EXISTS <env>.main.prices (
    symbol        VARCHAR  NOT NULL,
    date          DATE     NOT NULL,
    open          DOUBLE,
    high          DOUBLE,
    low           DOUBLE,
    close         DOUBLE,
    volume        BIGINT,
    daily_return  DOUBLE,
    moving_avg_10 DOUBLE,
    PRIMARY KEY (symbol, date)
)
```

---

## Configuration Reference

### `config/ingest.yaml`

| Key | Default | Description |
|---|---|---|
| `yfinance.period` | `3mo` | Lookback window for each download |
| `yfinance.interval` | `1d` | Bar size |
| `yfinance.auto_adjust` | `true` | Adjust OHLC for splits/dividends |
| `s3.raw_prefix` | `raw/prices` | S3 key prefix for raw files |

### `config/transform.yaml`

| Key | Description |
|---|---|
| `columns.rename` | Map of yfinance column names → normalised names |
| `derived_fields.daily_return` | Compute percentage change of close price |
| `derived_fields.moving_avg_10` | Compute 10-day rolling mean of close price |

### `config/load.yaml`

| Key | Default | Description |
|---|---|---|
| `motherduck.schema` | `main` | Target schema in MotherDuck |
| `motherduck.table` | `prices` | Target table in MotherDuck |
| `s3.processed_prefix` | `processed/prices` | S3 key prefix for Parquet files |
