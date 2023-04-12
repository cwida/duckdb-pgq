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

python3 -m pip install pandas numpy jproperties jupyter duckdb-engine jupysql

# duckdb-engine installs main duckdb which we need to install to have our own duckdb version
python3 -m pip uninstall duckdb

pip install git+https://github.com/ploomber/jupysql

EXTENSION_STATIC_BUILD=1 BUILD_JEMALLOC=1 BUILD_SQLPGQ=1 BUILD_PYTHON=1 GEN=ninja make
