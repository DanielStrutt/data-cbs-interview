#!/bin/bash
set -euo pipefail

STAGE="${1:-all}"

run_stage() {
  echo "==> Running stage: $1"
  python -m src.$1.main
}

case "$STAGE" in
  ingest)
    run_stage ingest
    ;;
  transform)
    run_stage transform
    ;;
  load)
    run_stage load
    ;;
  all)
    run_stage ingest
    run_stage transform
    run_stage load
    ;;
  *)
    echo "ERROR: Unknown stage '$STAGE'. Valid values: ingest | transform | load | all"
    exit 1
    ;;
esac
