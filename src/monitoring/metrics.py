"""
CloudWatch custom metrics for the DataOps pipeline.

Publishes four metrics per stage run to the 'DataPipeline' namespace,
dimensioned by Environment and Stage:

    StageDuration   — wall-clock seconds for the stage
    FilesProcessed  — number of files read or written
    RowsProcessed   — number of data rows handled
    StageSuccess    — 1.0 (success) or 0.0 (failure)

These can be graphed, alarmed on, or queried in CloudWatch Metrics to track
pipeline health and data volume trends over time.

CloudWatch publish failures are caught and logged as warnings — they never
take down the pipeline itself.

Usage:
    from src.monitoring.metrics import StageMetrics

    metrics = StageMetrics("ingest", region="eu-west-2")
    metrics.start()
    # ... do work ...
    metrics.finish(files_processed=3, rows_processed=270)

    # On failure:
    except Exception:
        metrics.finish(success=False)
        raise
"""

import logging
import os
import time
from datetime import datetime, timezone

import boto3

logger = logging.getLogger(__name__)

_NAMESPACE = "DataPipeline"


class StageMetrics:
    """
    Publishes per-stage pipeline run metrics to CloudWatch.

    Namespace : DataPipeline
    Dimensions: Environment, Stage

    Metrics emitted on finish():
        StageDuration   — wall-clock seconds
        FilesProcessed  — number of files read/written
        RowsProcessed   — number of data rows handled
        StageSuccess    — 1 (success) or 0 (failure)
    """

    def __init__(self, stage: str, region: str, env: str | None = None) -> None:
        self.stage = stage
        self.region = region
        self.env = env or os.environ.get("ENV", "dev")
        self._start: float | None = None
        self._cw = None

    # ------------------------------------------------------------------ #
    # Public API                                                           #
    # ------------------------------------------------------------------ #

    def start(self) -> None:
        """Record the stage start time."""
        self._start = time.monotonic()
        logger.info("Stage %s starting (env=%s)", self.stage, self.env)

    def finish(
        self,
        *,
        files_processed: int = 0,
        rows_processed: int = 0,
        success: bool = True,
    ) -> None:
        """Record the stage end time and publish metrics to CloudWatch."""
        duration = time.monotonic() - self._start if self._start is not None else 0.0
        status = "succeeded" if success else "failed"
        logger.info(
            "Stage %s %s in %.2fs — files=%d rows=%d",
            self.stage,
            status,
            duration,
            files_processed,
            rows_processed,
        )
        self._publish(duration, files_processed, rows_processed, success)

    # ------------------------------------------------------------------ #
    # Internal                                                             #
    # ------------------------------------------------------------------ #

    @property
    def _client(self):
        if self._cw is None:
            self._cw = boto3.client("cloudwatch", region_name=self.region)
        return self._cw

    def _publish(
        self,
        duration: float,
        files_processed: int,
        rows_processed: int,
        success: bool,
    ) -> None:
        dimensions = [
            {"Name": "Environment", "Value": self.env},
            {"Name": "Stage", "Value": self.stage},
        ]
        now = datetime.now(timezone.utc)
        metric_data = [
            {
                "MetricName": "StageDuration",
                "Value": duration,
                "Unit": "Seconds",
                "Dimensions": dimensions,
                "Timestamp": now,
            },
            {
                "MetricName": "FilesProcessed",
                "Value": float(files_processed),
                "Unit": "Count",
                "Dimensions": dimensions,
                "Timestamp": now,
            },
            {
                "MetricName": "RowsProcessed",
                "Value": float(rows_processed),
                "Unit": "Count",
                "Dimensions": dimensions,
                "Timestamp": now,
            },
            {
                "MetricName": "StageSuccess",
                "Value": 1.0 if success else 0.0,
                "Unit": "Count",
                "Dimensions": dimensions,
                "Timestamp": now,
            },
        ]

        try:
            self._client.put_metric_data(Namespace=_NAMESPACE, MetricData=metric_data)
            logger.info(
                "CloudWatch metrics published — namespace=%s stage=%s env=%s",
                _NAMESPACE,
                self.stage,
                self.env,
            )
        except Exception:
            # Never let a CloudWatch failure bring down the pipeline
            logger.warning("Failed to publish CloudWatch metrics", exc_info=True)
