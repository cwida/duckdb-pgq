#!/bin/bash

set -eu
set -o pipefail

cd "$( cd "$( dirname "${BASH_SOURCE[0]:-${(%):-%x}}" )" >/dev/null 2>&1 && pwd )"

cd data

# Check if DuckDB is installed
if ! command -v duckdb &> /dev/null; then
    echo "DuckDB not found. Installing..."
    curl https://install.duckdb.org | sh
else
    echo "DuckDB is already installed."
fi

for SF in 1 3 10 30 100 300; do
    curl --silent --fail --http1.1 https://pub-383410a98aef4cb686f0c7601eddd25f.r2.dev/duckpgq-experiments/snb-bi/snb-bi-sf${SF}.tar.zst | tar -xv --use-compress-program=unzstd
    mv snb-bi-sf${SF}.v snb-bi.v
    mv snb-bi-sf${SF}.e snb-bi.e
    duckdb snb-bi-sf${SF}.duckdb -f ../queries/load.sql
    rm snb-bi.e snb-bi.v
done

