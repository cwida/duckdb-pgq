import duckdb
from os import remove
from os import listdir
from os.path import isfile, join

SF = 1

database_file = "snb.duckdb"

dynamic_types = ["Comment", "Comment_hasTag_Tag", "Forum", "Forum_hasMember_Person", "Forum_hasTag_Tag",
                 "Person", "Person_hasInterest_Tag", "Person_knows_Person", "Person_likes_Comment",
                 "Person_likes_Post", "Person_studyAt_University", "Person_workAt_Company", "Post", "Post_hasTag_Tag"]
static_types = ["Organisation", "Place", "Tag", "TagClass"]
path = "bi-sf1-composite-merged-fk/graphs/csv/bi/composite-merged-fk/initial_snapshot/"

con = duckdb.connect(database_file)

for query in open("schema-composite-merged-fk.sql").read().split(";"):
    con.execute(query)

for dynamic_type in dynamic_types:
    file_path = path + f"dynamic/{dynamic_type}/"
    files = [f for f in listdir(file_path) if isfile(join(file_path, f)) and ".csv" in f]
    for file in files:
        full_file_path = file_path + file
        con.execute(f"""
                    COPY {dynamic_type} FROM '{full_file_path}' (DELIMITER '|', HEADER)
                    """)

for static_type in static_types:
    file_path = path + f"static/{static_type}/"
    files = [f for f in listdir(file_path) if isfile(join(file_path, f)) and ".csv" in f]
    for file in files:
        full_file_path = file_path + file
        con.execute(f"""
                    COPY {static_type} FROM '{full_file_path}' (DELIMITER '|', HEADER)
                    """)

con.execute("""
        CREATE OR REPLACE TABLE Company AS 
            (SELECT * FROM organisation where type = 'Company');
            """)

con.execute("""
        CREATE OR REPLACE TABLE University AS 
            (SELECT * FROM organisation where type = 'University');
            """)



