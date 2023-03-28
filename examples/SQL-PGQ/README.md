# Examples VLDB demonstration video

This folder contains the example queries used in the VLDB 2023 demonstration video for DuckPGQ. 

The extension currently requires to be made from source. 
Ensure `python3` and `virtualenv` are installed. 
Navigate to the main directory and execute `./create_python_package.sh`
Activate the virtual environment

The demo makes use of the LDBC SNB scale factor 1 dataset. 
To download this dataset:
1. Make sure this is your current directory (`examples/SQL-PGQ`). 
2. To download and decompress the data sets on-the-fly, make sure you have `curl` and `zstd` installed, then run:
    ```bash:
    export DATASET_URL=https://pub-383410a98aef4cb686f0c7601eddd25f.r2.dev/bi-pre-audit/bi-sf1-composite-merged-fk.tar.zst
    curl --silent --fail ${DATASET_URL} | tar -xv --use-compress-program=unzstd
    ```
3. Execute `python3 generate_database.py`
4. Execute 'jupyter notebook'
5. Open the notebook `Pattern-matching-example.ipynb`
