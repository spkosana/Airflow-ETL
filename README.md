# Project Name: ETL pipeline using Apache Airflow

#### Project Overview: 
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. They are planning to move their process on to the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

#### Project Aim:
Sparkify like a to hire a data engineer to create ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. The role of a data engineer is mainly to determine what data model to use and create a data model specific database schema and also appropriate ETL pipeline to bring the data from json files in s3 to amazon redshift database tables for sparkify to do their required analysis.

#### Project Description
After thoroughly reading through the requirement and understanding the data and needs of sparkify, for analysis team to understand the user activity on the app they need necessary statistics of the activities and the results that are retrieved should be fast and accurate. The primary reason dimensional modeling is its ability to allow data to be stored in a way that is optimized for information retrieval once it has been stored in a database.Dimensional models are specifically designed for optimized for the delivery of reports and analytics.It also provides a more simplified structure so that it is more intuitive for business users to write queries. Tables are de-normalized and are few tables which will have few joins to get the results with high performance.

#### Tables (Facts)
Table Name: Songplay(fact)
Column Names: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent


#### Dimension Tables: 
The dimension tables contain descriptive information about the numerical values in the fact table.

#### Tables ( Dimensions )

Table Name:Songs (Song Dimension)
Column Names: song_id, title, artist_id, year, duration

Table Name: Artists (Artist Dimension)
Column Names: artist_id, name, location, lattitude, longitude

Table Name:Users (User Dimension)
Column Names: user_id, first_name, last_name, gender, level

Table Name: Time (Time Dimension)
Column Names: start_time, hour, day, week, month, year, weekday
    
#### Database creation and loading approach:

Implement all the database DDL and DML statements in the sql_queries.py that create all the dimension tables and fact tables with all necessary columns. Build an ETL pipeline to extract data from the Json files from s3 and  push the data into necessary staging tables and then use the staging tables to insert appropriate data into dimensions and fact tables.

#### Database Design scripts :

##### Dags Directory:

udac_dag.py: This script is main script and is available in dags directory and has all the imports of necessary operators which are invoked in the script. All the operators are linked in the way to load the data into staging tables, dimension tables and fact tables. Finally added data quality operator to validate the load as expected. 

##### Helpers Directory:

sql_statements.py: This script is available in helpers directory in plugins directory and responsible for all the sql statements that are given to the input of the operators.

##### Operators Directory:

stage_redshift.py: Stage operator is defined in this scirpt which takes necessary parameters that get the data from json files in s3 into stage tables in redshift.

load_dimension.py: Load Dimension operator is defined in this script which takes necessary parameters that takes the sql and loads the dimensions tables

load_fact.py: Load Fact operator is defined in this script which takes necessary parameters that takes the sql and loads fact tables

data_quality.py: Data quality operator is defined in this script which takes necessary sql parameters and validate the data load to ensure all the load has been taken place as expetced. 





