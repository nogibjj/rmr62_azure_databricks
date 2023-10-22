"""
Test goes here

"""
import pytest
from mylib.delete_db import drop_data
from mylib.updateDb import update_db
from mylib.query import query
from mylib.extract import extract
from databricks import sql
from mylib.install_credentials import install_credentials
import os


@pytest.fixture
def setup_database():
    install_credentials()
    conn = sql.connect(
        server_hostname=os.getenv("server_hostname"),
        http_path=os.getenv("http_path"),
        access_token=os.getenv("access_token"),
    )
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS test_table")
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS test_table
                      (id INT,
                       name STRING,
                       count_products INT)"""
    )
    cursor.execute(
        """INSERT INTO test_table (name, count_products)
                      VALUES ('apple', 10),
                             ('banana', 11),
                             ('orange', 12)"""
    )
    conn.commit()
    yield conn

    conn.close()


def test_drop_data(setup_database):
    drop_data(
        table_name="test_table",
        condition="count_products = 11",
        sql_conn=setup_database,
    )
    conn = sql.connect(
        server_hostname=os.getenv("server_hostname"),
        http_path=os.getenv("http_path"),
        access_token=os.getenv("access_token"),
    )
    cursor = conn.cursor()
    cursor.execute("""SELECT * FROM test_table""")
    rows = cursor.fetchall()
    assert len(rows) == 2
    assert rows[0][1] == "apple"
    assert rows[0][2] == 10
    assert rows[1][1] == "orange"
    assert rows[1][2] == 12

    conn.close()


# def test_create_and_load_db():
#     create_and_load_db(dataset="data/nba_22_23.csv", table_name="test_tab")
#     install_credentials()
#     conn = sql.connect(
#         server_hostname=os.getenv("server_hostname"),
#         http_path=os.getenv("http_path"),
#         access_token=os.getenv("access_token"),
#     )
#     cursor = conn.cursor()
#     cursor.execute("""SELECT * FROM test_tab""")
#     rows = cursor.fetchall()
#     assert len(rows) > 1

#     conn.close()


def test_update_db(setup_database):
    update_db(
        conn=setup_database,
        query_str="UPDATE test_table SET name = 'Rakeen' WHERE count_products = 11",
    )
    conn = sql.connect(
        server_hostname=os.getenv("server_hostname"),
        http_path=os.getenv("http_path"),
        access_token=os.getenv("access_token"),
    )
    cursor = conn.cursor()
    cursor.execute("""SELECT * FROM test_table""")
    rows = cursor.fetchall()
    assert rows[1][1] == "Rakeen"
    assert rows[1][2] == 11

    setup_database.close()


def test_query(setup_database):
    assert (
        query(
            sql_conn=setup_database,
            query_str="SELECT * FROM test_table WHERE count_products = 11",
        )
        == "Success"
    )
    setup_database.close()


def test_extract():
    assert (
        extract(
            url="https://www.basketball-reference.com/leagues/NBA_2023_per_game.html#per_game_stats",
            file_path="data/extract.csv",
        )
        == "data/extract.csv"
    )
    os.remove("data/extract.csv")
