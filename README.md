## Azure DataBricks MySQL ETL (Extract, Transform, Load), CI (Continuous Intrigation), CLI
### By Rakeen Rouf
[![CI](https://github.com/nogibjj/rmr_62_sqlite-lab/actions/workflows/cicd.yml/badge.svg)](https://github.com/nogibjj/rmr_62_sqlite-lab/actions/workflows/cicd.yml)

This project serves as a comprehensive showcase of a custom Command Line Interface (CLI) tool tailored for seamless data management using a SQL warehouse on Azure Databricks. It seamlessly handles the Extract, Transform, and Load (ETL) processes, efficiently transferring data into a Databricks SQL Warehouse. In addition, this repository exemplifies crucial CRUD (Create, Read, Update, Delete) operations, offering a holistic understanding of database management.

Explore the power of this custom CLI, empowering users to effortlessly handle data, from acquisition to advanced database operations.

Note: Feel free to customize, enhance, and extend the capabilities of this versatile tool to suit your unique needs.

### Project Architecture
![Alt text](https://user-images.githubusercontent.com/36940292/277129478-736a7903-e074-4c11-9a77-f5c49df5b1d8.png)

** Remember to have valid credentials in the setup_credentials.json file in the root directoy

### Function Descriptions (Located in ~/mylib):

`extract(url:str, file_path:str) -> path str to extracted csv` (in extract.py):

Extracts data from the passed url to a csv file. The data must be in an HTML table format

url -> Path to online table data content

file_path -> OUtput file path

`create_and_load_db(dataset:str, db_name:str="nba_players", sql_conn:databricks.sql=None, d_type_dict:dict) -> databricks.sql` (in transform_load.py):

Function to create a Table on a Databricks SQL Warehouse and load data into it.The data is transformed from a CSV file with appropriate format changes made.

dataset -> Link to CSV FIle to load

db_name -> Desired name for table

d_type_dict -> dictionary for desired datatypes for each column (defualt is FLOAT)

`update_db(conn:databricks.sql=None, query_str:str='') -> None` (in updateDb.py):

Updates table entries based on the passed query. If query is left empty, this perform a defualt update on the `nba_players` table.

`query(query_str:str='', sql_conn:databricks.sql=None=None) -> str` (in query.py):

Function to query the database based on the passed query string. If query string is left blank then a default complex query is performed on the `nba_players` table.

`drop_data(table_name:str="nba_players", sql_conn:databricks.sql=None=None, condition="Tm = 'LAL'") -> None` (in delete_db.py):

Function to drop data based on the passed condition and table. The table must be defined in the input Databricks SQL Warehouse.

### Example Usage:
#### `Extract` 2022-2023 NBA player stats from the Basketball Reference website
![Alt text](https://user-images.githubusercontent.com/36940292/277146542-23690414-2d95-4cc1-83e7-87b5a4bef62b.png)

#### `Creating` a new remote Databrick SQL Warehouse table for the 2022 2023 NBA player stats
![Alt text](https://user-images.githubusercontent.com/36940292/277147153-4ae9d95f-cfe2-4a95-8867-e613a788621e.png)

#### `Updating` the rempte table to change Steven Adams's age to 27 (Before and after tabled are shown, with the update command in the middle)
![Alt text](https://user-images.githubusercontent.com/36940292/277147523-a731e34b-9427-440d-9220-79547f4a9099.png)

#### `Reading` data from the SQL Warehouse nba_players table
##### This is a default example of a complex SQL Query on a Databrick SQL warehouse.
![Alt text](https://user-images.githubusercontent.com/36940292/277147561-58af2a48-2840-4893-a156-70f3fd114652.png)

This query is calculating certain statistics for NBA teams and then using those stats to rank them according to a custom metric. Here is a breakdown of the query:

Common Table Expressions (CTEs)

- `team_stats`: Calculates the average points, field goal percentage, defensive rebounds, offensive rebounds, turnovers, and ranking for each team in the NBA based on information from the `nba_players` table.
- `min_max_values`: Calculates the minimum and maximum values for each statistic for each team based on the `nba_players` table.
- `ranked_teams`: Calculates the normalized values for each statistic for each team based on the minimum and maximum values found in `min_max_values`. It also ranks each team based on its average points.
- `ranked_teams_2`: Calculates a custom metric for each team by subtracting the normalized turnovers and ranking values from the normalized points value. It then ranks each team based on this custom metric.

Main query

Selects the top 10 teams based on the custom ranking calculated in `ranked_teams_2` and outputs various statistics for those teams, including their average points, maximum points, average field goal percentage, average defensive rebounds, average offensive rebounds, average turnovers, and average ranking. It also calculates the minimum ranking for each team and the custom metric specified in `ranked_teams_2`.

Overall

Aggregations are used in several places, such as in the team_stats CTE where AVG() is used to calculate the average statistics for each team, and in the min_max_values CTE where MIN() and MAX() are used to find the minimum and maximum values for each statistic for each team.

Joins are also used in this query. In the main query, a LEFT JOIN is used to combine the results from the ranked_teams_2 CTE with the min_max_values CTE so that the maximum points and minimum ranking for each team can be displayed alongside the other statistics. Joins are also used implicitly in the CTEs where subqueries are used to access the min_max_values data for each team.

Overall, this query demonstrates how to perform complicated calculations and comparisons on aggregated data from multiple sources through the use of various SQL functions and techniques.

##### The following are examples of the CLI implementation
![Alt text](https://user-images.githubusercontent.com/36940292/272158416-e62dc846-3a9f-4439-a9f6-6f72c21a2e40.png)
![Alt text](https://user-images.githubusercontent.com/36940292/272158764-1ce859c4-5ca5-4629-a078-fb6113bb87ab.png)

#### `Deleting` all instances of where team is POR
![Alt text](https://user-images.githubusercontent.com/36940292/272159480-3abb7220-d198-4997-9551-fcbbe9795228.png)


### Efficiency and Limitations of Azure Databricks SQL Analytics

Azure Databricks SQL Analytics is a powerful tool for data analysis that can greatly enhance efficiency. It is a cloud-based solution that allows for easy scaling and concurrent access from multiple users. Its architecture is designed to handle large datasets, making it a suitable choice for enterprise-level applications.

Compared to SQLite, Azure Databricks SQL Analytics offers more robust features and better performance for complex operations and large datasets. It supports most standard SQL operations and provides additional functionality for data analysis, such as machine learning and data visualization tools.

However, as with any technology, there are limitations to consider. While Azure Databricks SQL Analytics can handle large datasets, it may not be the best choice for extremely large datasets or scenarios with very high concurrency requirements. Additionally, it is a cloud-based solution, which may not be suitable for applications with strict data privacy or security requirements.

Overall, Azure Databricks SQL Analytics is a powerful tool for data analysis that offers many advantages over traditional database systems like SQLite. However, it is important to carefully consider the specific requirements of your application before choosing a database solution. 
