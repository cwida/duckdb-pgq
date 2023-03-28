# Examples VLDB demonstration video

This folder contains the example queries used in the VLDB 2023 demonstration video for DuckPGQ. 

The demo makes use of the LDBC SNB scale factor 1 dataset. 
To download this dataset:
1. Make sure this is your current directory. 
2. To download and decompress the data sets on-the-fly, make sure you have `curl` and `zstd` installed, then run:
```bash
export DATASET_URL=https://pub-383410a98aef4cb686f0c7601eddd25f.r2.dev/interactive/snb-out-sf1-projected-fk.tar.zst
curl --silent --fail ${DATASET_URL} | tar -xv --use-compress-program=unzstd
```


