# Data Modeling with Apache Cassandra

## Overview


This project will collecting on songs and user activity on their new music streaming app to analyze the data.
By get data from data event then insert into Apache Cassandra database.


## Structure

The project contains the following elements:
* `event_data/` contains the event on song and user activity in JSON format
* `cql_queries.py` defines the CQL queries that underpin the creation of the database schema and ETL pipeline
* `create_tables.py` the script to drops and creates for the Sparkify keyspace and tables
* `etl.py` defines the ETL pipeline, which pulls and transforms the song and user activity in JSON files and inserts them into the database 
<br/><br/>
* `Project_2_Template.ipynb` the Jupyter Notebook for interactively develop and run python code to be used in etl.py and create_tables.py
* `main.ipynb` this notebook is for run the project. It will create Sparkify database, table, and load data to the database
* `test.ipynb` displays the first 5 rows of each table and test 3 example queries to let you check your database.

## Schema
List tables will serve for 3 example queries 
* `song_session`
* `artist_session `
* `song_user `


## Instructions

To run the project in Udacity's Project Workspace, open `main.ipynb` and  `Run All Cells`
To run the project in Udacity's Project Workspace, open `test.ipynb` and  `Run All Cells`

