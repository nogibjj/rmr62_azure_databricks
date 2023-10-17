"""This module contains functions to delete a table from remoe 
databricks sql database"""""
from databricks import sql
import os


def drop_data(table_name:str="nba_players", 
              sql_conn=None, 
              condition="count_priducts = '11"):
    """function to drop data based on condition and table"""
    if not sql_conn:
        conn = sql.connect(
            server_hostname = "adb-2816916652498074.14.azuredatabricks.net",
            http_path = "/sql/1.0/warehouses/2e1d07a8ec5d6691",
            access_token = os.getenv('ACCESS_TOKEN_DB'))
    else:
        conn = sql_conn
    
    cursor = conn.cursor()
    cursor.execute(f"DELETE FROM {table_name} WHERE {condition};")
    conn.commit()
    conn.close()

    print(f"Table {table_name} dropped from Databricks sql database.")
