"""This module contains functions to delete a table from a remote 
databricks SQL warehouse database."""
from databricks import sql
import os
try:
    from mylib.install_credentials import install_credentials
except ModuleNotFoundError:
    from install_credentials import install_credentials


def drop_data(table_name:str="nba_players", 
              sql_conn=None, 
              condition="Tm = 'LAL'"):
    """Function to drop data based on a input condition and table name"""
    if not sql_conn:
        install_credentials()
        print("Credentials installed")
        conn = sql.connect(
                    server_hostname = os.getenv('server_hostname'),
                    http_path = os.getenv('http_path'),
                    access_token = os.getenv('access_token'))
    else:
        conn = sql_conn
    

    cursor = conn.cursor()
    cursor.execute(f"DELETE FROM {table_name} WHERE {condition};")
    conn.commit()
    conn.close()

    print(f"EXECUTING: DELETE FROM {table_name} WHERE {condition};")
    print(f"Records deleted from {table_name}")
    
if __name__ == '__main__':
    drop_data() 
    print("Executed: drop_data()")
