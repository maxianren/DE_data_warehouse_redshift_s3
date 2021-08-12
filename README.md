Summary of the project
Sparkify is a music streaming startup. They want to move their processes and data , which resides in S3 including a directory of JSON logs on user activity and a directory with JSON metadata on the songs in their app , onto the cloud.
The objective is to build an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team.
 
Description of the working folder
supporting file
 - dwh.cfg: credential information about objective cloud database, user and source location
 - sql_queries.py: SQL syntax to create staging and query table, to transfer data from source to Redshift, and insert them into query table for further analytics
 
Main file:
Implementation of SQL command
 - create_tables.py: connect to cloud server and create tables for staging and query
 - etl.py: connect to cloud server, load data into Redshift and insert data into query table
 
Test file
 - test.ipynb: code to test if the implementation correspond to expectation and debug
 
Steps of implementation:
 - Activate terminal, go to the working directory
 - Type "python create_tables.py",  wait until you see "staging tables and query tables has been successfully created", which means  previously created table are dropped and new tables are created and ready for the data pipeline
 - Type "etl.py", wait until "data sources have been successfully loaded into staging tables" and "data in the staging tables has been successfully inserted into query tables".
 - You can check the result either in the test file or using query service in redshift
 

# DE_data_warehouse_redshift_s3
