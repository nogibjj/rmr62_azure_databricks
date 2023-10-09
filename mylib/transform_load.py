"""
This module contains functions to transform and load data into a 
local SQLite3 database. The SQLite3 database is created in the 
current working directory. If the database already exists, it will 
be overwritten.
"""
import csv
from databricks import sql
import os


def create_and_load_db(dataset:str="data/nba_22_23.csv", 
                       db_name:str="nba_players",
                       sql_conn=None):
    """"function to create a remote databricks sql database and load data into it. 
    The data is transformed from a CSV file."""

    with open(dataset, newline='') as csvfile:
        payload = list(csv.reader(csvfile, delimiter=','))

    column_names = [name.replace('%', 'Perc') if name else 'ID' for name in payload[0]]
    # replace the start of the string if its a number
    column_nmess = [f"{name[1:]}{name[0]}" if name[0].isdigit() else 
                    f"{name}" for name in column_names]

    column_names = [f"{name[1:]}{name[0]} string" if name[0].isdigit() else 
                    f"{name} string" for name in column_names]
    
    if not sql_conn:
        # connect to the remote databricks sql database
        conn = sql.connect(
                        server_hostname = "adb-2816916652498074.14.azuredatabricks.net",
                        http_path = "/sql/1.0/warehouses/2e1d07a8ec5d6691",
                        access_token = os.getenv('ACCESS_TOKEN_DB'))
        
        print(f"Database {db_name} created.")
    else:
        conn = sql_conn
    
    c = conn.cursor() # create a cursor
    # drop the table if it exists
    c.execute(f"DROP TABLE IF EXISTS {db_name}")
    print(f"Excuted: DROP TABLE IF EXISTS {db_name}") 
    c.execute(f"CREATE TABLE IF NOT EXISTS {db_name} ({', '.join(column_names)})")
    print(f"Excuted: CREATE TABLE {db_name} ({', '.join(column_names)})")
    # insert the data from payload
    payload[1:] = [f"{tuple(row)}" for row in payload[1:]]   
    c.execute(f"INSERT INTO {db_name} ({', '.join(column_nmess)}) VALUES {', '.join(payload[1:])}")
    
    print(f"Excuted: INSERT INTO {db_name} VALUES" \
                  f"({', '.join(['?']*len(column_names))})")
    
    conn.commit()
    conn.close()
    
    return conn


if __name__ == '__main__':
    create_and_load_db()
