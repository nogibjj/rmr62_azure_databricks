## Azure DataBricks MySQL ETL (Extract, Transform, Load), CI (Continuous Intrigation), CLI
### By Rakeen Rouf
[![CI](https://github.com/nogibjj/rmr_62_sqlite-lab/actions/workflows/cicd.yml/badge.svg)](https://github.com/nogibjj/rmr_62_sqlite-lab/actions/workflows/cicd.yml)

This project serves as a comprehensive showcase of a custom Command Line Interface (CLI) tool tailored for seamless data management using a SQL warehouse on Azure Databricks. It seamlessly handles the Extract, Transform, and Load (ETL) processes, efficiently transferring data into a Databricks SQL Warehouse. In addition, this repository exemplifies crucial CRUD (Create, Read, Update, Delete) operations, offering a holistic understanding of database management.

Explore the power of this custom CLI, empowering users to effortlessly handle data, from acquisition to advanced database operations.

Note: Feel free to customize, enhance, and extend the capabilities of this versatile tool to suit your unique needs.

### Project Architecture
![Alt text](https://user-images.githubusercontent.com/36940292/277129478-736a7903-e074-4c11-9a77-f5c49df5b1d8.png)
### Function Descriptions (Located in ~/mylib):

`extract(url:str, file_path:str) -> str` (in extract.py):

Extracts data from the passed url to a csv file. The data must be in an HTML table format

`create_and_load_db(dataset:str, db_name:str="nba_players", sql_conn:databricks.sql=None, d_type_dict:dict) -> databricks.sql` (in transform_load.py):

Function to create a Table on a Databricks SQL Warehouse and load data into it.The data is transformed from a CSV file with appropriate format changes made.

dataset -> Link to CSV FIle to load

db_name -> Desired name for table

d_type_dict -> dictionary for desired datatypes for each column (defualt is FLOAT)

`update_db(conn:databricks.sql=None, database:str="nba_players", query_str:str='') -> None` (in updateDb.py):

Updates entries in the specified data base, based on the passed query. If query is left empty, this perform a defualt update on the `nba_players` table.

`query(query_str:str='', db_name:str='nba_players', sql_conn:databricks.sql=None=None) -> str` (in query.py):

Function to query the database based on the passed query string. If query string is left blank then a couple of default queries are performed on the `nba_players` table.

`drop_data(db_name:str="nba_players", table_name:str="nba_players", sql_conn:databricks.sql=None=None, condition="count_priducts = '11") -> None` (in delete_db.py):

Function to drop data based on the passed condition and table

### Example Usage:
#### `Extract` 2022-2023 NBA player stats from the Basketball Reference website
![Alt text](https://user-images.githubusercontent.com/36940292/272149032-86d67039-9f4b-4de7-86db-f63983319ba2.png)

#### `Creating` a new local Sqlite database for the 2022 2023 NBA player stats
![Alt text](https://user-images.githubusercontent.com/36940292/272152401-fd2c7862-d6d6-43f0-99b4-be90218c2ed5.png)

#### `Updating` the database to change the Steven Adams age to 27 (Before and after tabled are shown, with the update command in the middle)
![Alt text](https://user-images.githubusercontent.com/36940292/272156679-37cfa2b6-9cf8-4c51-aa04-3b5078e9e4a4.png)

#### `Reading` data from the database
![Alt text](https://user-images.githubusercontent.com/36940292/272158416-e62dc846-3a9f-4439-a9f6-6f72c21a2e40.png)
![Alt text](https://user-images.githubusercontent.com/36940292/272158764-1ce859c4-5ca5-4629-a078-fb6113bb87ab.png)

#### `Deleting` all instances of where team is POR
![Alt text](https://user-images.githubusercontent.com/36940292/272159480-3abb7220-d198-4997-9551-fcbbe9795228.png)


### Efficiency and Limitations of SQLite and SQL:
SQLite and SQL greatly enhance data analysis efficiency. The lightweight nature of SQLite makes it a fast and accessible choice for smaller projects or local applications. Its simplicity and self-contained architecture streamline setup and deployment. However, for larger datasets (~>280 TB) or scenarios requiring concurrent access from multiple users, more robust database systems may be more suitable. Additionally, while SQLite supports most standard SQL operations, it may have limitations in handling very large datasets or complex operations that some enterprise-level databases can manage more effectively.
