"""
Load script: reads processed Parquet files from S3 staging and loads them
into MotherDuck (dev.main.prices).

Idempotent: uses INSERT OR IGNORE on a (symbol, date) primary key, so
re-running never produces duplicate rows.

Usage:
    uv run src/load/main.py

All tunable settings are driven from config/load.yaml.
Secrets are pulled from AWS SSM Parameter Store.
"""

import logging
import os

import boto3
import duckdb

from src.utils.config_loader import load_config
from src.utils.get_parameter import get_parameter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS {table} (
    symbol        VARCHAR      NOT NULL,
    date          DATE         NOT NULL,
    open          DOUBLE,
    high          DOUBLE,
    low           DOUBLE,
    close         DOUBLE,
    volume        BIGINT,
    daily_return  DOUBLE,
    moving_avg_10 DOUBLE,
    PRIMARY KEY (symbol, date)
)
"""

INSERT_SQL = """
INSERT OR IGNORE INTO {table}
SELECT
    symbol,
    date::DATE AS date,
    open,
    high,
    low,
    close,
    volume::BIGINT AS volume,
    daily_return,
    moving_avg_10
FROM read_parquet('s3://{bucket}/{key}')
"""


def list_parquet_files(s3_client, bucket: str, prefix: str) -> list[str]:
    """Return all Parquet keys under the processed prefix."""
    keys = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                keys.append(obj["Key"])
    return keys


def main() -> None:
    cfg = load_config("load.yaml")

    ssm_params = cfg["ssm"]["parameters"]
    ssm_region = cfg["ssm"]["region"]

    staging_bucket   = get_parameter(ssm_params["s3_bucket_staging"], region=ssm_region)
    motherduck_token = get_parameter(ssm_params["motherduck_token"], region=ssm_region)
    region           = get_parameter(ssm_params["aws_region"], region=ssm_region, required=False) or cfg["defaults"]["aws_region"]

    # Database name matches the environment (dev/prod) so it's driven by ENV
    md_db     = os.environ.get("ENV", "dev")
    md_schema = cfg["motherduck"]["schema"]
    md_table  = cfg["motherduck"]["table"]
    full_table = f"{md_db}.{md_schema}.{md_table}"
    processed_prefix = cfg["s3"]["processed_prefix"]

    # Connect to MotherDuck and verify
    logger.info("Connecting to MotherDuck database: %s ...", md_db)
    con = duckdb.connect(f"md:{md_db}?motherduck_token={motherduck_token}")
    con.execute("SELECT 1").fetchone()
    logger.info("Connected to MotherDuck database: %s", md_db)

    # Configure DuckDB S3 access using current AWS credentials
    session = boto3.Session(region_name=region)
    creds = session.get_credentials().get_frozen_credentials()
    con.execute(f"SET s3_region='{region}'")
    con.execute(f"SET s3_access_key_id='{creds.access_key}'")
    con.execute(f"SET s3_secret_access_key='{creds.secret_key}'")
    if creds.token:
        con.execute(f"SET s3_session_token='{creds.token}'")

    # Ensure table exists
    con.execute(CREATE_TABLE_SQL.format(table=full_table))
    logger.info("Table ready: %s", full_table)

    # List and load Parquet files
    s3 = boto3.client("s3", region_name=region)
    parquet_keys = list_parquet_files(s3, staging_bucket, processed_prefix)
    logger.info("Found %d Parquet file(s) to load", len(parquet_keys))

    for key in parquet_keys:
        sql = INSERT_SQL.format(table=full_table, bucket=staging_bucket, key=key)
        con.execute(sql)
        logger.info("Loaded s3://%s/%s", staging_bucket, key)

    # Row count after all inserts
    total_rows = con.execute(f"SELECT COUNT(*) FROM {full_table}").fetchone()[0]
    con.close()
    logger.info("Load complete. Total rows in %s: %d", full_table, total_rows)


if __name__ == "__main__":
    try:
        main()
    except BaseException:
        logger.exception("Load pipeline failed")
        raise SystemExit(1)
