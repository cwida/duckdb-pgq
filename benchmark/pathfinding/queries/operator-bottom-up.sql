with reverse_csr_cte as (
    SELECT cast(min(create_csr_edge(
            0,
            (SELECT count(a.id) FROM person a),
            CAST (
                    (SELECT sum(create_csr_vertex(
                            0,
                            (SELECT count(a.id) FROM person a),
                            sub.dense_id,
                            sub.cnt))
                     FROM (
                              SELECT a.rowid as dense_id, count(k.person2id) as cnt
                              FROM person a
                                       LEFT JOIN person_knows_person k ON k.person2id = a.id
                              GROUP BY a.rowid) sub
                    )
                AS integer),
            (select count() FROM person_knows_person k JOIN person a on a.id = k.person2id JOIN person c on c.id = k.person1id),
            a.rowid,
            c.rowid,
            k.rowid)) as integer) as reverse_csr_id
    FROM person_knows_person k
             JOIN person a on a.id = k.person2id
             JOIN person c on c.id = k.person1id),
     csr_cte as (
         SELECT cast(min(create_csr_edge(
                 1,
                 (SELECT count(a.id) FROM person a),
                 CAST (
                         (SELECT sum(create_csr_vertex(
                                 1,
                                 (SELECT count(a.id) FROM person a),
                                 sub.dense_id,
                                 sub.cnt))
                          FROM (
                                   SELECT a.rowid as dense_id, count(k.person1id) as cnt
                                   FROM person a
                                            LEFT JOIN person_knows_person k ON k.person1id = a.id
                                   GROUP BY a.rowid) sub
                         )
                     AS integer),
                 (select count() FROM person_knows_person k JOIN person a on a.id = k.person1id JOIN person c on c.id = k.person2id),
                 a.rowid,
                 c.rowid,
                 k.rowid)) as integer) as csr_id
         FROM person_knows_person k
                  JOIN person a on a.id = k.person1id
                  JOIN person c on c.id = k.person2id)
SELECT src as source, dst as destination, iterativelengthoperator(src, dst, csr_id, reverse_csr_id) as path FROM snb_pairs, csr_cte, reverse_csr_cte;