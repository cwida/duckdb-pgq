#!/bin/bash

# Define versions, sources, and scale factors
versions=("operator-bottom-up")
sources=(16384)
scale_factors=(30)
threads=(2)
bottom_up_thresholds=(0.8 0.9 1.0)

# Loop over versions, sources, and scale factors
for version in "${versions[@]}"; do
  for source in "${sources[@]}"; do
    for scale in "${scale_factors[@]}"; do
      for thread in "${threads[@]}"; do
        for bottom_up_threshold in "${bottom_up_thresholds[@]}"; do
          # Define the file name
          filename="benchmarks/${version}_sf${scale}_src${source}_t${thread}_b${bottom_up_threshold}.benchmark"

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
BOTTOM_UP_THRESHOLD=${bottom_up_threshold}
EOL
        echo "Generated: $filename"
        done
      done
    done
  done
done
