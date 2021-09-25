# Data Modeling with Postgres

## Overview

This project provides the schema and ETL to create and populate an analytics database for the music streaming app Sparkify.

It used the PostgreSQL relational database to store data that stream from the data in JSON.

## Structure

The project contains the following elements:
* `data/` contains song and log files on user activity in JSON format
* `sql_queries.py` defines the SQL queries that underpin the creation of the database schema and ETL pipeline
* `create_tables.py` the script to drops and creates for the Sparkify database and tables
* `etl.py` defines the ETL pipeline, which pulls and transforms the song and log JSON files and inserts them into the database 
<br/><br/>
* `etl.ipynb` reads and processes a single file from `song_data` and `log_data` and loads the data into your tables.
* `main.ipynb` this notebook is for run the project. It will create Sparkify database, table, and load data to the database
* `test.ipynb` displays the first 5 rows of each table to let you check your database.

## Schema

The database contains 
fact table:
* `songplays`

dimension tables:
* `users`
* `songs`
* `artists`
* `time`

## Instructions

To run the project in Udacity's Project Workspace, open `main.ipynb` and  `Run All Cells`


## Query Example


```
# Connect to database
%load_ext sql
%sql postgresql://student:student@127.0.0.1/sparkifydb

# Number of song plays before Nov 20, 2018
%sql SELECT COUNT(songplay_id) FROM songplays WHERE start_time < '2018-11-20';

```
