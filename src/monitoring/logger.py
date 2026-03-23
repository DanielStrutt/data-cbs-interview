"""
Structured JSON logging for the DataOps pipeline.

Replaces the plain-text basicConfig format with single-line JSON records.
Each record includes stage, env, level, and message, making them fully
queryable in CloudWatch Logs Insights:

    fields @timestamp, stage, level, message
    | filter stage = "ingest" and level = "ERROR"
    | sort @timestamp desc

Usage:
    from src.monitoring.logger import setup_logging
    setup_logging("ingest")        # reads ENV from environment variable
    setup_logging("transform", "prod")  # explicit env
"""

import json
import logging
import os


class _JsonFormatter(logging.Formatter):
    """Formats each log record as a single JSON line."""

    def __init__(self, stage: str, env: str) -> None:
        super().__init__()
        self._stage = stage
        self._env = env

    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "timestamp": self.formatTime(record, "%Y-%m-%dT%H:%M:%SZ"),
            "level": record.levelname,
            "stage": self._stage,
            "env": self._env,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(entry)


def setup_logging(stage: str, env: str | None = None) -> None:
    """
    Configure the root logger to emit structured JSON lines to stdout.

    Removes any handlers added by earlier logging.basicConfig() calls so
    this is safe to call at the top of each stage's main().

    Args:
        stage: Pipeline stage name ('ingest', 'transform', 'load').
        env:   Environment name. Defaults to the ENV environment variable ('dev').
    """
    if env is None:
        env = os.environ.get("ENV", "dev")

    root = logging.getLogger()
    root.setLevel(logging.INFO)

    # Clear any handlers added by basicConfig or earlier setup calls
    for handler in root.handlers[:]:
        root.removeHandler(handler)

    handler = logging.StreamHandler()
    handler.setFormatter(_JsonFormatter(stage=stage, env=env))
    root.addHandler(handler)
