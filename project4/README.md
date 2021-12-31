# PROJECT-3: Data Warehouse

## Overview


This project will build an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.


## Structure

The project contains the following elements:

* `dl.cfg/` defines the variable for AWS_KEY, AWS_SECRET, OUTPUT_DATA

* `project-setup.ipynb` the notebook for interactively develop and run python code to be used in etl.py and create_tables.py

* `etl.py` defines the ETL pipeline, which load data from S3 and build the tables and write back to S3

* `main.ipynb` this notebook is for run the project.


## Schema
List tables
* `staging_events`
* `staging_songs `
* `songplays `
* `users `
* `songs `
* `artists `
* `time `

## How to run 

### Prerequisite

1.Check input data in the S3
* s3://udacity-dend/log_data
* s3://udacity-dend/song_data

2.Create the user for datalake, get the KEY, SECRET and set to `dl.cfg` file

3.Create the a S3 bucket and set path to OUTPUT_DATA


### Ready to run

To run the project in Udacity's Project Workspace, open `main.ipynb` and  `Run All Cells`

