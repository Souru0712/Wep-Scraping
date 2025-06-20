# for accessing databases
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

#for querying the server
import pyodbc
import psycopg2

# for extracting data
import pandas as pd
import sqlalchemy as sa

# set variables
uid = "etl"
pwd = "demopass"
driver = "ODBC Driver 18 for SQL SERVER"
server = "LAM-PC"
database = "AdventureWorksDW2022;"
port = "1433"


def ms_engine():
    engine = create_engine(f'mssql+pyodbc://{uid}:{pwd}@{server}:{port}/{database}?driver={driver}')
    return engine

def load(df, tbl):
    try:
        rows_imported = 0

        # load to postgreSQL

        pg_engine = create_engine(f'postgresql://{uid}:{pwd}@{server}:5432/AdventureWorks?sslmode=prefer')
        conn = pg_engine.connect()
        print(f'importing rows {rows_imported} to {rows_imported + len(df)}... for table {tbl}')

        # save df to postgres
        # truncate and load
        df.to_sql(f'stg_{tbl}', conn.connection, if_exists='replace', index=False, chunksize=100000)
        rows_imported += len(df)
        print("Data Imported successful")
    except Exception as e:
        print("Loading failed: " + str(e))



# connection string with autocommit set to true and trust server certificate set to true for authorization

schemas_query = ''' SELECT s.name AS schema_name
    FROM sys.schemas s
    WHERE s.name IN ('HumanResources');
    '''
tables_query = (''' SELECT t.name AS table_name
#     FROM sys.tables t
#     WHERE schema_name(schema_id) = 'HumanResources'
#     ''')

engine = ms_engine()
conn = engine.connect()
# df = pd.read_sql_table('HumanResources.Shift', conn)
# print(df)
# with engine.connect() as connection:
#     result = connection.execute(schemas_query)
#     print(result)


# conn = pyodbc.connect('DRIVER=' + driver +';SERVER=' + server +';DATABASE=' + database +';UID=' + uid +';PWD=' + pwd + ';Trusted_Connection=yes;TrustServerCertificate=yes;&autocommit=True')

# cur = conn.cursor()

# connectionString = ('DRIVER=' +driver
#                     +';SERVER=' + server
#                     +';DATABASE=' + database
#                     +';UID=' + uid
#                     +';PWD=' + pwd
#                     +';Trusted_Connection=yes;TrustServerCertificate=yes;'
#                     )
# # connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connectionString})
# connection_url = (f'mssql+pyodbc://elt:demopass@localhost:1433/AdventureWorksDW2022?driver={driver}')
# ms_engine = create_engine(connection_url)
# conn = ms_engine.connect()
# schemas_query = ''' SELECT s.name AS schema_name
#     FROM sys.schemas s
#     WHERE s.name IN ('HumanResources');
#     '''
#
# # pd.read_sql(schemas_query,conn)
# # cur.execute(schemas_query)
# # schema = cur.fetchone().schema_name
#
# tables_query = (''' SELECT t.name AS table_name
#     FROM sys.tables t
#     WHERE schema_name(schema_id) = 'HumanResources'
#     ''')
# cur.execute(tables_query)
# tables = cur.fetchall()
#
# for tbl in tables:
#     table_name = tbl.table_name
#     extractString = (f'''SELECT * FROM {schema}.{table_name}''')
#     df = pd.read_sql(extractString, conn)


    # df = pd.read_sql(f'SELECT * FROM {schema_name}.{table_name}', conn)
# tables_query = (''' SELECT t.name AS table_name
#     FROM sys.tables t
#     WHERE schema_name(schema_id) = 'HumanResources'
#     ''')
# schemas = pd.read_sql_query(schemas_query, conn.connection).to_dict()['table_name']

# tables = pd.read_sql_query(tables_query, conn.connection).to_dict()['table_name']
# for tbl in tables:
#     schema_name = schemas[0]
#     table_name = tables[tbl]
#     df = pd.read_sql_query(f'SELECT * FROM {schema_name}.{table_name}', conn.connection)
#     load(df, table_name)
# # hr = 'HumanResources'
# # df = pd.read_sql_query(f'SELECT * FROM {hr}.Shift', conn.connection)
# # df
#
#
