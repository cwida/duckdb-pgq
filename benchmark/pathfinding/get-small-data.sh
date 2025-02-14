#!/bin/bash

set -eu
set -o pipefail

cd "$( cd "$( dirname "${BASH_SOURCE[0]:-${(%):-%x}}" )" >/dev/null 2>&1 && pwd )"

cd data

for SF in 1 3 10 30 100 300; do
    curl --silent --fail --http1.1 https://pub-383410a98aef4cb686f0c7601eddd25f.r2.dev/duckpgq-experiments/snb-bi/snb-bi-sf${SF}.tar.zst | tar -xv --use-compress-program=unzstd
done
