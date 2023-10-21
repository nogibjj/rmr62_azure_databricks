"""
This module contains functions to transform and load data into a 
Azure Databricks SQL warehouse database table. If the table exists, it will be 
overwritten. 
"""
import csv
from databricks import sql
import os
from install_credentials import install_credentials


def create_and_load_db(dataset:str="data/nba_22_23.csv", 
                       db_name:str="nba_players",
                       sql_conn=None, d_type_dict:dict=None):
    """"Function to create a remote databricks sql database table and 
    load data into it.vThe data is transformed from a CSV file."""

    with open(dataset, newline='') as csvfile:
        payload = list(csv.reader(csvfile, delimiter=','))
        # filter out the nested lists that cointain ''
        payload = [row for row in payload if '' not in row ]
        # from the second row onwards, drop rows that contain 'Rk'
        payload[1:] = [row for row in payload[1:] if 'Rk' not in row]

    column_names = [name.replace('%', 'Perc') if name else 'ID' for name in payload[0]]
    # replace the start of the string if its a number
    column_names_raw = [f"{name[1:]}{name[0]}" if name[0].isdigit() else 
                    f"{name}" for name in column_names]

    if not d_type_dict:
        d_type_dict = {"Tm": "STRING", "Player": "STRING", "Pos": "STRING", 
                       'Rk': "STRING", "Age": "INT", "G": "FLOAT", 
                       "GS": "FLOAT", "MP": "FLOAT"}

    column_names = [f"{n} {d_type_dict.get(n, 'FLOAT')}" for n in column_names_raw]
    
    if not sql_conn:
        # connect to the remote databricks sql database
        install_credentials()
        conn = sql.connect(
                        server_hostname = os.path.getenv('server_hostname'),
                        http_path = os.getenv('http_path'),
                        access_token = os.getenv('access_token'))
        
        print(f"Database {db_name} created.")
    else:
        conn = sql_conn
    
    c = conn.cursor() # create a cursor
    # drop the table if it exists
    c.execute(f"DROP TABLE IF EXISTS {db_name}")
    print(f"Excuted: DROP TABLE IF EXISTS {db_name}")
    c.execute("SET ansi_mode = false;")
    c.execute(f"CREATE TABLE IF NOT EXISTS {db_name} ({', '.join(column_names)})")
    print(f"Excuted: CREATE TABLE {db_name} ({', '.join(column_names)})")
    # insert the data from payload
    str_p1 = f"INSERT INTO {db_name} ({', '.join(column_names_raw)}) VALUES"
    insert_to_tbl_stmt = f"{str_p1} ({', '.join(['%s']*len(column_names))})"
    c.executemany(insert_to_tbl_stmt, payload[1:]) #load data into azure sql db
    # c.execute()
    
    conn.commit()
    conn.close()
    print(f"Data inserted into {db_name}.")
    
    return conn


if __name__ == '__main__':
    create_and_load_db()
