#!/usr/bin/env bash
set -euo pipefail

OUTPUT_DIR="${OUTPUT_DIR:-research_output/local_stream_collector}"

target/release/cli validate-material-hunter-artifacts \
  --output-dir "${OUTPUT_DIR}" \
  --json
