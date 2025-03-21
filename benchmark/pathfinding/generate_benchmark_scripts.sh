#!/bin/bash

# Define versions, sources, and scale factors
versions=("operator")
sources=(1)
scale_factors=(3 30 300)
threads=(8)
partition_sizes=(65536 131072 262144 524288 1048576 2097152 3145728 4194304 5242880 6291456 7340032 8388608)
# Loop over versions, sources, and scale factors
for version in "${versions[@]}"; do
  for source in "${sources[@]}"; do
    for scale in "${scale_factors[@]}"; do
      for thread in "${threads[@]}"; do
        for partition_size in "${partition_sizes[@]}"; do
          # Define the file name
          filename="benchmarks/${version}_sf${scale}_src${source}_t${thread}_p${partition_size}.benchmark"

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
PARTITION_SIZE=${partition_size}
EOL

        echo "Generated: $filename"
        done
      done
    done
  done
done
