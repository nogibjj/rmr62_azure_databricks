"""Query the database"""
import os
from databricks import sql
from prettytable import PrettyTable
try:
    from install_credentials import install_credentials
except ModuleNotFoundError:
    from mylib.install_credentials import install_credentials


def print_pretty_table(cursor):
    """Print a pretty table from a cursor object"""
    rows = cursor.fetchall()
    x = PrettyTable()
    x.field_names = [i[0] for i in cursor.description]
    for row in rows:
        x.add_row(row)

    print(x)

def query(query_str:str='', 
          sql_conn=None) -> str:
    """Query the database"""
    if not sql_conn:
        install_credentials()
        print("Credentials installed")
        sql_conn = sql.connect(
            server_hostname = os.getenv('server_hostname'),
            http_path = os.getenv('http_path'),
            access_token = os.getenv('access_token'))
    
    cursor = sql_conn.cursor()
    print("Connected to database")
    
    if query_str == '':
        default_query_str = """WITH team_stats AS (
                                SELECT
                                    Tm,
                                    AVG(PTS) AS avg_pts,
                                    AVG(FGPerc) AS avg_fg_perc,
                                    AVG(DRB) AS avg_drb,
                                    AVG(ORB) AS avg_orb,
                                    AVG(TOV) AS avg_tov,
                                    AVG(Rk) AS avg_rk
                                FROM
                                    nba_players
                                GROUP BY
                                    Tm
                                ),
                                min_max_values AS (
                                SELECT
                                    Tm,
                                    MIN(PTS) AS min_pts,
                                    MAX(PTS) AS max_pts,
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
                                group by
                                    Tm
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
                                    (
                                    avg_pts - (
                                        SELECT
                                        MIN(min_pts)
                                        FROM
                                        min_max_values
                                    )
                                    ) / (
                                    (
                                        SELECT
                                        MAX(max_pts)
                                        FROM
                                        min_max_values
                                    ) - (
                                        SELECT
                                        MIN(min_pts)
                                        FROM
                                        min_max_values
                                    )
                                    ) AS norm_pts,
                                    (
                                    avg_fg_perc - (
                                        SELECT
                                        MIN(min_fg_perc)
                                        FROM
                                        min_max_values
                                    )
                                    ) / (
                                    (
                                        SELECT
                                        MAX(max_fg_perc)
                                        FROM
                                        min_max_values
                                    ) - (
                                        SELECT
                                        MIN(min_fg_perc)
                                        FROM
                                        min_max_values
                                    )
                                    ) AS norm_fg_perc,
                                    (
                                    avg_drb - (
                                        SELECT
                                        MIN(min_drb)
                                        FROM
                                        min_max_values
                                    )
                                    ) / (
                                    (
                                        SELECT
                                        MAX(max_drb)
                                        FROM
                                        min_max_values
                                    ) - (
                                        SELECT
                                        MIN(min_drb)
                                        FROM
                                        min_max_values
                                    )
                                    ) AS norm_drb,
                                    (
                                    avg_orb - (
                                        SELECT
                                        MIN(min_orb)
                                        FROM
                                        min_max_values
                                    )
                                    ) / (
                                    (
                                        SELECT
                                        MAX(max_orb)
                                        FROM
                                        min_max_values
                                    ) - (
                                        SELECT
                                        MIN(min_orb)
                                        FROM
                                        min_max_values
                                    )
                                    ) AS norm_orb,
                                    (
                                    avg_tov - (
                                        SELECT
                                        MIN(min_tov)
                                        FROM
                                        min_max_values
                                    )
                                    ) / (
                                    (
                                        SELECT
                                        MAX(max_tov)
                                        FROM
                                        min_max_values
                                    ) - (
                                        SELECT
                                        MIN(min_tov)
                                        FROM
                                        min_max_values
                                    )
                                    ) AS norm_tov,
                                    (
                                    avg_rk - (
                                        SELECT
                                        MIN(min_rk)
                                        FROM
                                        min_max_values
                                    )
                                    ) / (
                                    (
                                        SELECT
                                        MAX(max_rk)
                                        FROM
                                        min_max_values
                                    ) - (
                                        SELECT
                                        MIN(min_rk)
                                        FROM
                                        min_max_values
                                    )
                                    ) AS norm_rk,
                                    RANK() OVER (
                                    ORDER BY
                                        avg_pts DESC
                                    ) AS team_rank
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
                                    RANK() OVER (
                                    ORDER BY
                                        norm_pts - norm_tov + norm_drb DESC
                                    ) AS team_rank
                                FROM
                                    ranked_teams
                                )
                                SELECT
                                team_rank,
                                ranked_teams_2.Tm,
                                ROUND(avg_pts, 2),
                                ROUND(min_max_values.max_pts, 2) max_points,
                                ROUND(avg_fg_perc, 2),
                                ROUND(avg_drb, 2) avg_Dreb,
                                ROUND(avg_orb, 2) avg_Oreb,
                                ROUND(avg_tov, 2),
                                ROUND(avg_rk, 2) avg_rannking,
                                ROUND(min_max_values.min_rk, 2) min_ranking,
                                ROUND(norm_pts - norm_tov + norm_drb, 2) 
                                as custom_metric
                                FROM
                                ranked_teams_2
                                LEFT JOIN 
                                min_max_values on min_max_values.Tm = ranked_teams_2.Tm
                                WHERE
                                team_rank <= 10;"""
        print("Executing default query to display top ten teams by custom metric")
        cursor.execute(default_query_str)

        print("Query results:")
        print_pretty_table(cursor)
    else:
        cursor.execute(query_str)
        print(f"QUERY: {query_str}")
        print("Query results:")
        print_pretty_table(cursor)

    sql_conn.close()
    
    return "Success"

if __name__ == '__main__':
    query()
