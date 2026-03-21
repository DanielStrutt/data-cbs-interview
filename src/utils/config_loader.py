"""
Utility for loading YAML config files from the config/ directory.

Usage:
    from src.utils.config_loader import load_config

    cfg = load_config("ingest.yaml")
    period = cfg["yfinance"]["period"]
"""

from pathlib import Path

import yaml

CONFIG_DIR = Path(__file__).parents[2] / "config"


def load_config(filename: str) -> dict:
    """Load and return a YAML config file from the config/ directory."""
    path = CONFIG_DIR / filename
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    with path.open("r") as f:
        return yaml.safe_load(f)
