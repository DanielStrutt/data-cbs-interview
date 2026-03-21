"""
Ingestion script: fetches raw daily OHLCV data from Yahoo Finance via yfinance
and uploads it as-is to S3 under raw/prices/<SYMBOL>/<YYYY-MM-DD>.csv.

Idempotent: if the S3 key for today already exists it is skipped, so re-runs
within the same day never produce duplicate data.

Usage:
    uv run src/ingest/main.py

All tunable settings (yfinance period/interval, S3 prefix, SSM parameter names)
are driven from config/ingest.yaml. Secrets are pulled from AWS SSM Parameter Store.
"""

import io
import logging
from datetime import datetime, timezone

import boto3
import yfinance as yf
from botocore.exceptions import ClientError

from src.utils.config_loader import load_config
from src.utils.get_parameter import get_parameter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
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


def fetch_raw_csv(symbol: str, period: str, interval: str, auto_adjust: bool) -> str:
    """Download raw daily OHLCV for a symbol and return it as a CSV string, unmodified."""
    df = yf.download(symbol, period=period, interval=interval, auto_adjust=auto_adjust, progress=False)

    if df.empty:
        raise ValueError(f"No data returned for symbol={symbol}")

    buffer = io.StringIO()
    df.to_csv(buffer)
    return buffer.getvalue()


def upload_symbol(s3_client, bucket: str, prefix: str, symbol: str, run_date: str, yf_cfg: dict) -> None:
    """Upload raw CSV for a symbol to S3, skipping if the file already exists."""
    key = f"{prefix}/{symbol}/{run_date}.csv"

    if s3_key_exists(s3_client, bucket, key):
        logger.info("Already exists, skipping: s3://%s/%s", bucket, key)
        return

    csv_data = fetch_raw_csv(
        symbol,
        period=yf_cfg["period"],
        interval=yf_cfg["interval"],
        auto_adjust=yf_cfg["auto_adjust"],
    )

    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=csv_data.encode("utf-8"),
        ContentType="text/csv",
    )
    logger.info("Uploaded: s3://%s/%s", bucket, key)


def main() -> None:
    cfg = load_config("ingest.yaml")

    ssm_params = cfg["ssm"]["parameters"]
    ssm_region = cfg["ssm"]["region"]

    bucket  = get_parameter(ssm_params["s3_bucket_raw"], region=ssm_region)
    symbols_raw = get_parameter(ssm_params["symbols"], region=ssm_region, required=False) or cfg["defaults"]["symbols"]
    region  = get_parameter(ssm_params["aws_region"], region=ssm_region, required=False) or cfg["defaults"]["aws_region"]
    prefix  = cfg["s3"]["raw_prefix"]
    yf_cfg  = cfg["yfinance"]

    symbols  = [s.strip() for s in symbols_raw.split(",") if s.strip()]
    run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    logger.info("Starting ingestion — bucket=%s symbols=%s run_date=%s", bucket, symbols, run_date)

    s3 = boto3.client("s3", region_name=region)

    for symbol in symbols:
        upload_symbol(s3, bucket, prefix, symbol, run_date, yf_cfg)

    logger.info("Ingestion complete.")


if __name__ == "__main__":
    main()
