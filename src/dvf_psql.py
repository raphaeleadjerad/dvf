# -*- coding: utf-8 -*-
# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.11.1
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# # Creation of a Postgresql DVF database

# +
# modules
import os
import yaml
import psycopg2
import pandas as pd

os.getcwd()
# -

# for part I
import csvkit

# Put your token in the yaml file, be careful to name it secrets.yml
with open("../secrets.yml", 'r') as f:
    secrets = yaml.safe_load(f)


# ## Functions

def open_connection(dbname: str = "dvf", secrets: str = "../secrets.yml"):
    """
    Function to create a connection with the postgresql db using the secrets yaml file
    """
    print(f"opening connection")

    with open(secrets, "r") as s:
        sql_s = yaml.safe_load(s)
    
    USERNAME = sql_s["connection"]["username"]
    HOST = sql_s["connection"]["host"]
    conn = psycopg2.connect(
        host=HOST,
        port="5432",
        user=USERNAME,
        dbname=dbname,
        password= sql_s['connection']["token"],
    )

    return conn


def check_db(dbname: str = "dvf", secrets: str = "../secrets.yml", with_connection: bool = False, 
             cur: psycopg2.extensions.cursor = None):
    """
    Function to check the tables existing in a specific db
    """
    if cur is not None:
        with_connection = True

    if not with_connection:
        conn = open_connection(dbname, secrets)
        cur = conn.cursor()

    cur.execute(
        """SELECT table_name FROM information_schema.tables
       WHERE table_schema = 'public'"""
    )
    elements = [element[0] for element in cur.fetchall()]

    if not with_connection:
        cur.close()
        conn.close()

    return elements


def drop_table(table_name: str, dbname: str = "dvf", path_to_secret_yml: str = "../secrets.yml") -> None:
    """
    Function to drop a specific table from a db
    """
    conn = open_connection(dbname=dbname, secrets=path_to_secret_yml)

    cur = conn.cursor()

    if table_name not in check_db(cur=cur):
        print(f"{} non présente dans la base PostgreSQL {}".format(table_name, dbname))
        return

    drop_request = f"DROP TABLE {table_name};"

    cur.execute(drop_request)

    cur.close()
    conn.commit()
    conn.close()

    print(f"{} supprimée de la base PostgreSQL {}".format(table_name, dbname))


def create_table(table_name: str, sql_query: str, dbname: str = "dvf", path_to_secret_yml: str = "../secrets.yml") -> None:
    """
    Function to create a table in a specific db
    """
    conn = open_connection(dbname=dbname, secrets=path_to_secret_yml)

    cur = conn.cursor()

    if table_name in check_db(cur=cur):
        print(f"{} déjà présente dans la base PostgreSQL {}".format(table_name, dbname))
        return

    create_request = sql_query
    
    cur.execute(create_request)

    cur.close()
    conn.commit()
    conn.close()

    print(f"{} créée dans la base PostgreSQL {}".format(table_name, dbname))


# 
def copy_from_file(conn, path2file, table):
    """
    function from https://naysan.ca/2020/06/21/pandas-to-postgresql-using-psycopg2-copy_from/ check different possibilities to copy from file
    here we use COPY as user with no superuser priviledges careful about what it implies regarding data being loaded
    Here we are going save the dataframe on disk as 
    a csv file, load the csv file  
    and use copy_from() to copy it to the table
    """
    # Save the dataframe to disk
    
    f = open(path2file, 'r')
    cursor = conn.cursor()
    try:
        cursor.copy_from(f, table, sep=";")
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("copy_from_file() done")
    cursor.close()


def execute_sql_query_table(table_name: str, sql_query: str, dbname: str = "dvf", path_to_secret_yml: str = "../secrets.yml") -> None:
    """
    Function to execute a specific sql query on a table
    """
    conn = open_connection(dbname=dbname, secrets=path_to_secret_yml)

    cur = conn.cursor()

    if table_name in check_db(cur=cur):
        print(f"{table_name} bien présente dans la base PostgreSQL {dbname}")

    sql = sql_query.format(table_name)
    cur.execute(
       sql
    )
    print(cur.fetchall())
    cur.close()
    conn.close()
   


path2data = "https://cadastre.data.gouv.fr/data/etalab-dvf/latest/csv/"
df = pd.read_csv(path2data + "2020/full.csv.gz")
df.to_csv("data/df_example.csv", sep=",", encoding="utf-8", index=False)

df = pd.read_csv("data/df_example.csv")
df.shape

# + active=""
# # In command line to get sql query to CREATE TABLE, then amend it if copy to db shows pbs, take n large enough
# > head -n "data/df_example.csv" | csvsql "data/df_example.csv" 
# -

# ## CREATE TABLE communes

drop_table("communes")

query_create_table = """CREATE TABLE communes (
        id_mutation VARCHAR NOT NULL, 
        date_mutation VARCHAR NOT NULL, 
        numero_disposition VARCHAR NOT NULL, 
        nature_mutation VARCHAR NOT NULL, 
        valeur_fonciere VARCHAR, 
        adresse_numero VARCHAR, 
        adresse_suffixe VARCHAR, 
        adresse_nom_voie VARCHAR, 
        adresse_code_voie VARCHAR, 
        code_postal VARCHAR, 
        code_commune VARCHAR NOT NULL, 
        nom_commune VARCHAR NOT NULL, 
        code_departement VARCHAR NOT NULL, 
        ancien_code_commune VARCHAR, 
        ancien_nom_commune VARCHAR, 
        id_parcelle VARCHAR NOT NULL, 
        ancien_id_parcelle VARCHAR, 
        numero_volume VARCHAR, 
        lot1_numero VARCHAR, 
        lot1_surface_carrez VARCHAR, 
        lot2_numero VARCHAR, 
        lot2_surface_carrez VARCHAR, 
        lot3_numero VARCHAR, 
        lot3_surface_carrez VARCHAR, 
        lot4_numero VARCHAR, 
        lot4_surface_carrez VARCHAR, 
        lot5_numero VARCHAR, 
        lot5_surface_carrez VARCHAR, 
        nombre_lots DECIMAL NOT NULL, 
        code_type_local VARCHAR, 
        type_local VARCHAR, 
        surface_reelle_bati VARCHAR, 
        nombre_pieces_principales VARCHAR, 
        code_nature_culture VARCHAR, 
        nature_culture VARCHAR, 
        code_nature_culture_speciale VARCHAR, 
        nature_culture_speciale VARCHAR, 
        surface_terrain VARCHAR, 
        longitude VARCHAR, 
        latitude VARCHAR
);"""


create_table(table_name = "communes", sql_query=query_create_table)

path2data = "https://cadastre.data.gouv.fr/data/etalab-dvf/latest/csv/"
df = pd.read_csv(path2data + "2020/full.csv.gz")

path2file = "data/dataframe.csv"
df.to_csv(path2file, index_label='id_mutation', index=False, header=False, sep=";")
print("writing done")

conn = open_connection("dvf")
copy_from_file(conn, "data/dataframe.csv", "communes")
conn.close()

sql_query = """
     SELECT 
       COUNT(*) 
    FROM 
    {};
    """
execute_sql_query_table('communes', sql_query)

# It is possible to stream the file instead of writing it to disk, but some variables show issues with comma separator
for year in ["2019", "2018", "2017", "2016", "2015", "2014"]:
    path2data = "https://cadastre.data.gouv.fr/data/etalab-dvf/latest/csv/"
    df = pd.read_csv(path2data + year + "/full.csv.gz")
    path2file = "../data/" + year + ".csv"
    df.to_csv(path2file, index_label='id_mutation', index=False, header=False, sep=";")
    print("writing done")
    conn = open_connection("dvf")
    copy_from_file(conn,  path2file, "communes")
    os.remove(path2file)
    conn.close()

conn.close()

sql_query = """
     SELECT 
       COUNT(*) 
    FROM 
    {};
    """
execute_sql_query_table('communes', sql_query) # 20 911 315 lines

sql_query =  "SELECT pg_size_pretty(pg_total_relation_size('{}'));"
execute_sql_query_table('communes', sql_query) # 4.3 Gigas, could hold in memory, but we will add other tables


