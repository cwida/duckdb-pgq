#!/bin/bash

set -eu
set -o pipefail

cd "$( cd "$( dirname "${BASH_SOURCE[0]:-${(%):-%x}}" )" >/dev/null 2>&1 && pwd )"


./get-small-data.sh
./generate_benchmark_scripts.sh
cd ../../../
BUILD_BENCHMARK=1 make GEN=ninja
build/release/benchmark/benchmark_runner --disable-timeout "benchmark/pathfinding/benchmarks/.*" > output.timing 2>&1