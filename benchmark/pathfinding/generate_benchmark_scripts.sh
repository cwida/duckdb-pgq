#!/bin/bash

# Define versions, sources, and scale factors
versions=("operator")
sources=(16384)
scale_factors=(1 3 10 30 100 300)
threads=(1 2 4 8)
light_multipliers=(1 2 4 8)
heavy_fractions=(0.5 0.6 0.7 0.8)

# Loop over versions, sources, and scale factors
for version in "${versions[@]}"; do
  for source in "${sources[@]}"; do
    for scale in "${scale_factors[@]}"; do
      for thread in "${threads[@]}"; do
        for light_multiplier in "${light_multipliers[@]}"; do
          for heavy_fraction in "${heavy_fractions[@]}"; do
            # Define the file name
            filename="benchmarks/${version}_sf${scale}_src${source}_t${thread}_l${light_multiplier}_h${heavy_fraction}.benchmark"

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
LIGHT_MULTIPLIER=${light_multiplier}
HEAVY_FRACTION=${heavy_fraction}
EOL

          echo "Generated: $filename"
          done
        done
      done
    done
  done
done
