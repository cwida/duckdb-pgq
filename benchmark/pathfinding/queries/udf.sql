WITH cte1 AS (
    SELECT  CREATE_CSR_EDGE(
                    0,
                    (SELECT count(a.id) FROM person a),
                    CAST (
                            (SELECT sum(CREATE_CSR_VERTEX(
                                    0,
                                    (SELECT count(a.id) FROM person a),
                                    sub.dense_id,
                                    sub.cnt)
                                    )
                             FROM (
                                      SELECT a.rowid as dense_id, count(k.person1id) as cnt
                                      FROM person a
                                               LEFT JOIN person_knows_person k ON k.person1id = a.id
                                      GROUP BY a.rowid) sub
                            )
                        AS BIGINT),
                    (select count(*) from person_knows_person k JOIN person a on a.id = k.person1id JOIN person c on c.id = k.person2id),
                    a.rowid,
                    c.rowid,
                    k.rowid) as temp
    FROM person_knows_person k
             JOIN person a on a.id = k.person1id
             JOIN person c on c.id = k.person2id
) SELECT src as source, dst as destination, iterativelength(0, (select count(*) from person), snb_pairs.src, snb_pairs.dst) as path
FROM snb_pairs, (select count(cte1.temp) * 0 as temp from cte1) __x
WHERE __x.temp * 0 = 0;