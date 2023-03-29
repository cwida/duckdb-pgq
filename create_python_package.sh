#!/bin/bash

set -eu
set -o pipefail

cd "$( cd "$( dirname "${BASH_SOURCE[0]:-${(%):-%x}}" )" >/dev/null 2>&1 && pwd )"

if command -v deactivate; then
    deactivate
fi
rm -rf .venv

make clean

virtualenv .venv
source .venv/bin/activate

python3 -m pip install pandas numpy jproperties jupyter duckdb-engine

# duckdb-engine installs main duckdb which we need to install to have our own duckdb version
python3 -m pip uninstall duckdb

EXTENSION_STATIC_BUILD=1 BUILD_SQLPGQ=1 BUILD_PYTHON=1 make GEN=ninja
