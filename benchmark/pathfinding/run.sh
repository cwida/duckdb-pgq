#!/bin/bash

set -eu
set -o pipefail

cd "$( cd "$( dirname "${BASH_SOURCE[0]:-${(%):-%x}}" )" >/dev/null 2>&1 && pwd )"


./get-small-data.sh

rm benchmarks/*
./generate_benchmark_scripts.sh
cd ../../../
BUILD_BENCHMARK=1 make GEN=ninja

COMMIT_HASH=$(git rev-parse --short HEAD)

# Define output file with commit hash and timestamp
OUTPUT_FILE="output_${COMMIT_HASH}.timing"

# Run benchmark and save output
build/release/benchmark/benchmark_runner --disable-timeout "benchmark/pathfinding/benchmarks/.*" > "$OUTPUT_FILE" 2>&1