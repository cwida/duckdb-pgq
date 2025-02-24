create or replace table person as select * from read_csv_auto('snb-bi.v',
                                                   header=FALSE,
                                                   sep=' ',
                                                   columns={'id':'bigint'});
create or replace table person_knows_person as select * from read_csv_auto('snb-bi.e',
                                                                header=FALSE,
                                                                sep=' ',
                                                                columns={'person1id':'bigint',
                                                                    'person2id':'bigint',
                                                                    'weight':'bigint'
                                                                });
