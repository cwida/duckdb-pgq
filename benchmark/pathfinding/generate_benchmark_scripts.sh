#!/bin/bash

# Define versions, sources, and scale factors
versions=("operator")
sources=(512 1024 2048 16384 32768 65536)
scale_factors=(30 100 300)
threads=(1)

# Loop over versions, sources, and scale factors
for version in "${versions[@]}"; do
  for source in "${sources[@]}"; do
    for scale in "${scale_factors[@]}"; do
      for thread in "${threads[@]}"; do

        # Define the file name
        filename="benchmarks/${version}_sf${scale}_src${source}_t${thread}.benchmark"

        # Write content to the file
        cat <<EOL > "$filename"
# name: benchmark/pathfinding/${version}.benchmark
# description: Run ${version} Query For Pathfinding Benchmark
# group: [pathfinding]

template benchmark/pathfinding/pathfinding.benchmark.in
QUERY_NAME=${version}
NUMBER_OF_SOURCES=${source}
SCALE_FACTOR=${scale}
THREADS=${thread}
EOL

      echo "Generated: $filename"
      done
    done
  done
done