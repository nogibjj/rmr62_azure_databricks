"""Update the database"""
from databricks import sql
import os
try:
    from install_credentials import install_credentials
except ModuleNotFoundError:
    from mylib.install_credentials import install_credentials


def update_db(conn=None,
              query_str:str='')->None:
    """Update the database"""
    if not conn:
        install_credentials()
        print("Credentials installed")
        conn = sql.connect(
                        server_hostname = os.getenv('server_hostname'),
                        http_path = os.getenv('http_path'),
                        access_token = os.getenv('access_token'))

    cursor = conn.cursor()
    print("Connected to database")
    
    if query_str == '':
        query_str = """UPDATE nba_players 
                        SET PTS = PTS + 1
                        WHERE Tm = 'LAL';"""
        cursor.execute(query_str)
    else:
        cursor.execute(query_str)

    conn.commit()

    print(f"Executed: {query_str}")
    print("Records updated")


if __name__ == '__main__':
    update_db()
