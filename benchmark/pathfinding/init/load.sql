attach '/Users/dljtw/git/duckpgq/test-pathfinding.duckdb';
use 'test-pathfinding';
set experimental_path_finding_operator=true;
create or replace table snb_pairs as (
   select src, dst
   from (select a.rowid as src from person a),
        (select b.rowid as dst from person b)
   using sample reservoir(16384 rows) repeatable (300)
);
