"""Update the database"""
from install_credentials import install_credentials
from databricks import sql
import os


def update_db(conn=None, 
              database:str="nba_players",
              query_str:str='')->None:
    """Update the database"""
    if not conn:
        install_credentials()
        conn = sql.connect(
                        server_hostname = os.path.getenv('server_hostname'),
                        http_path = os.getenv('http_path'),
                        access_token = os.getenv('access_token'))

    cursor = conn.cursor()
    
    if query_str == '':
        cursor.execute("""UPDATE nba_players 
                        SET PTS = PTS + 1
                        WHERE Tm = 'LAL';""")
    else:
        cursor.execute(query_str)
    
    conn.commit()

    print("Records updated")


if __name__ == '__main__':
    update_db()
