## SNB SF0.1 data
Make sure you are in the root folder of duckpgq
```
curl -L https://repository.surfsara.nl/datasets/cwi/snb/files/social_network-csv_composite/social_network-csv_composite-sf0.1.tar.zst | tar --use-compress-program=unzstd -x --directory data/sqlpgq-testing/snb/
```

## Graphalytics undirected example data
```
curl --silent --fail https://surfdrive.surf.nl/files/index.php/s/enKFbXmUBP2rxgB/download | tar -xv --use-compress-program=unzstd --directory data/sqlpgq-testing/graphalytics/
```