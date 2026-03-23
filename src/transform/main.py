"""
Transform script: reads raw OHLCV CSVs from S3 (raw bucket), cleans and enriches
the data, then writes Parquet files to the staging bucket.

Idempotent: if the Parquet output for a given symbol/date already exists in the
staging bucket it is skipped, so re-runs never reprocess the same file.

Usage:
    uv run src/transform/main.py

All tunable settings are driven from config/transform.yaml.
Bucket names are pulled from AWS SSM Parameter Store.
"""

import io
import logging
import os

import boto3
import pandas as pd
from botocore.exceptions import ClientError

from src.monitoring.logger import setup_logging
from src.monitoring.metrics import StageMetrics
from src.utils.config_loader import load_config
from src.utils.get_parameter import get_parameter

logger = logging.getLogger(__name__)


def s3_key_exists(s3_client, bucket: str, key: str) -> bool:
    """Return True if the key already exists in S3."""
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        raise


def list_raw_files(s3_client, bucket: str, prefix: str) -> list[str]:
    """Return all CSV keys under the raw prefix."""
    keys = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".csv"):
                keys.append(obj["Key"])
    return keys


def read_raw_csv(s3_client, bucket: str, key: str) -> pd.DataFrame:
    """Fetch a raw CSV from S3 and return as a DataFrame."""
    response = s3_client.get_object(Bucket=bucket, Key=key)
    content = response["Body"].read().decode("utf-8")
    # yfinance writes a 2-row header (Price, Ticker) — skip the second header row
    df = pd.read_csv(io.StringIO(content), header=0, skiprows=[1])
    return df


def transform(df: pd.DataFrame, symbol: str, rename_map: dict, derived: dict) -> pd.DataFrame:
    """Clean and enrich a raw OHLCV DataFrame."""
    # Normalise column names
    df.columns = [c.strip() for c in df.columns]
    df = df.rename(columns={"Price": "date", **rename_map})

    # Drop any rows where date or close is missing
    df = df.dropna(subset=["date", "close"])
    df["date"] = pd.to_datetime(df["date"])
    df.sort_values("date", inplace=True)
    df.reset_index(drop=True, inplace=True)

    # Add symbol column
    df.insert(0, "symbol", symbol)

    # Derived fields
    if derived.get("daily_return"):
        df["daily_return"] = df["close"].pct_change().round(6)

    if derived.get("moving_avg_10"):
        df["moving_avg_10"] = df["close"].rolling(10).mean().round(4)

    return df


def write_parquet(s3_client, df: pd.DataFrame, bucket: str, key: str) -> None:
    """Serialise DataFrame to Parquet and upload to S3."""
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=buffer.read(),
        ContentType="application/octet-stream",
    )
    logger.info("Written: s3://%s/%s (%d rows)", bucket, key, len(df))


def main() -> None:
    env = os.environ.get("ENV", "dev")
    cfg = load_config("transform.yaml")

    ssm_params = cfg["ssm"]["parameters"]
    ssm_region = cfg["ssm"]["region"]

    setup_logging("transform", env)
    metrics = StageMetrics("transform", region=ssm_region, env=env)
    metrics.start()

    try:
        raw_bucket     = get_parameter(ssm_params["s3_bucket_raw"], region=ssm_region)
        staging_bucket = get_parameter(ssm_params["s3_bucket_staging"], region=ssm_region)
        region         = get_parameter(ssm_params["aws_region"], region=ssm_region, required=False) or cfg["defaults"]["aws_region"]

        raw_prefix       = cfg["s3"]["raw_prefix"]
        processed_prefix = cfg["s3"]["processed_prefix"]
        rename_map       = cfg["columns"]["rename"]
        derived          = cfg["derived_fields"]

        s3 = boto3.client("s3", region_name=region)

        raw_keys = list_raw_files(s3, raw_bucket, raw_prefix)
        logger.info("Found %d raw file(s) to process", len(raw_keys))

        files_written = 0
        rows_written = 0
        for raw_key in raw_keys:
            # raw_key shape: raw/prices/<SYMBOL>/<DATE>.csv
            parts = raw_key.split("/")
            symbol   = parts[-2]
            date_str = parts[-1].replace(".csv", "")

            out_key = f"{processed_prefix}/{symbol}/{date_str}.parquet"

            if s3_key_exists(s3, staging_bucket, out_key):
                logger.info("Already processed, skipping: s3://%s/%s", staging_bucket, out_key)
                continue

            logger.info("Processing: s3://%s/%s", raw_bucket, raw_key)
            df_raw = read_raw_csv(s3, raw_bucket, raw_key)
            df_transformed = transform(df_raw, symbol, rename_map, derived)
            write_parquet(s3, df_transformed, staging_bucket, out_key)
            files_written += 1
            rows_written += len(df_transformed)

        logger.info("Transform complete.")
        metrics.finish(files_processed=files_written, rows_processed=rows_written)
    except Exception:
        metrics.finish(success=False)
        raise


if __name__ == "__main__":
    main()
