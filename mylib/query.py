"""Query the database"""

from databricks import sql
from prettytable import PrettyTable
import os


def print_pretty_table(cursor):
    """Print a pretty table from a cursor object"""
    rows = cursor.fetchall()
    x = PrettyTable()
    x.field_names = [i[0] for i in cursor.description]
    for row in rows:
        x.add_row(row)

    print(x)

def query(query_str:str='', db_name:str='nba_players', 
          sql_conn=None) -> str:
    """Query the database"""

    if not sql_conn:
        # connect to the remote databricks sql database
        print(os.getenv('ACCESS_TOKEN_DB'))
        conn = sql.connect(
                        server_hostname = "adb-2816916652498074.14.azuredatabricks.net",
                        http_path = "/sql/1.0/warehouses/2e1d07a8ec5d6691",
                        access_token = "dapib2d54250560ee475e0eeb40ef86f9c26-3")
        
        print(f"Database {db_name} created.")
    else:
        conn = sql_conn
    
    cursor = conn.cursor() # create a cursor
    
    if query_str == '':
        query = """WITH team_stats AS (
                    SELECT Tm, 
                            AVG(PTS) AS avg_pts,
                            AVG(FGPerc) AS avg_fg_perc,
                            AVG(DRB) AS avg_drb,
                            AVG(ORB) AS avg_orb,
                            AVG(TOV) AS avg_tov, 
                            AVG(Rk) AS avg_rk
                    FROM nba_players
                    GROUP BY Tm
                    ),
                    min_max_values AS (
                        SELECT 
                        Tm,
                        MIN(PTS)  AS min_pts,
                        ROUND(MAX(PTS), 2) AS max_pts,
                        MIN(FGPerc) AS min_fg_perc,
                        MAX(FGPerc) AS max_fg_perc,
                        MIN(DRB) AS min_drb,
                        MAX(DRB) AS max_drb,
                        MIN(ORB) AS min_orb,
                        MAX(ORB) AS max_orb,
                        MIN(TOV) AS min_tov,
                        MAX(TOV) AS max_tov,
                        MIN(RK) AS min_rk,
                        MAX(RK) AS max_rk
                        FROM 
                        nba_players
                        group by Tm
                    ),
                    ranked_teams AS (
                        SELECT 
                        Tm,
                        avg_pts,
                        avg_fg_perc,
                        avg_drb,
                        avg_orb,
                        avg_tov,
                        avg_rk,
                        (avg_pts - (SELECT MIN(min_pts) FROM min_max_values)) / 
                            ((SELECT MAX(max_pts) FROM min_max_values) - (SELECT MIN(min_pts) FROM min_max_values)) AS norm_pts,
                        (avg_fg_perc - (SELECT MIN(min_fg_perc) FROM min_max_values)) / 
                            ((SELECT MAX(max_fg_perc) FROM min_max_values) - (SELECT MIN(min_fg_perc) FROM min_max_values)) AS norm_fg_perc,
                        (avg_drb - (SELECT MIN(min_drb) FROM min_max_values)) / 
                            ((SELECT MAX(max_drb) FROM min_max_values) - (SELECT MIN(min_drb) FROM min_max_values)) AS norm_drb,
                        (avg_orb - (SELECT MIN(min_orb) FROM min_max_values)) / 
                            ((SELECT MAX(max_orb) FROM min_max_values) - (SELECT MIN(min_orb) FROM min_max_values)) AS norm_orb,
                        (avg_tov - (SELECT MIN(min_tov) FROM min_max_values)) / 
                            ((SELECT MAX(max_tov) FROM min_max_values) - (SELECT MIN(min_tov) FROM min_max_values)) AS norm_tov,
                        (avg_rk - (SELECT MIN(min_rk) FROM min_max_values)) / 
                            ((SELECT MAX(max_rk) FROM min_max_values) - (SELECT MIN(min_rk) FROM min_max_values)) AS norm_rk,
                            
                        RANK() OVER (ORDER BY avg_pts DESC) AS team_rank
                        FROM 
                        team_stats
                    ), 
                    ranked_teams_2 AS (
                    SELECT
                        Tm, 
                        avg_pts,
                        avg_fg_perc,
                        avg_drb,
                        avg_orb,
                        avg_tov, 
                        norm_pts,
                        avg_rk,
                        norm_fg_perc, 
                        norm_drb, 
                        norm_orb,
                        norm_tov,
                        norm_rk, 
                        RANK() OVER (ORDER BY norm_pts - norm_tov + norm_drb DESC) AS team_rank
                        FROM 
                        ranked_teams
                    )
                    SELECT ranked_teams_2.Tm, 
                        ROUND(avg_pts, 2),
                        min_max_values.max_pts max_points,
                        ROUND(avg_fg_perc, 2),
                        ROUND(avg_drb, 2) avg_Dreb,
                        ROUND(avg_orb, 2) avg_Oreb,
                        ROUND(avg_tov, 2),
                        ROUND(avg_rk, 2) avg_rannking,
                        ROUND(min_max_values.min_rk, 2) min_ranking, 
                        ROUND(norm_pts - norm_tov - norm_rk, 2) as custom_metric
                    FROM ranked_teams_2
                    LEFT JOIN min_max_values
                    on min_max_values.Tm = ranked_teams_2.Tm
                    WHERE team_rank <= 10;"""
        cursor.execute(query)
        
        print("Rows where count_products = '11':")
        print_pretty_table(cursor)
    else:
        cursor.execute(query_str)
        print(f"QUERY: {query_str}")
        print("Query results:")
        print_pretty_table(cursor)

    conn.close()
    
    return "Success"

if __name__ == "__main__":
    query()
