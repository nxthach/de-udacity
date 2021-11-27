# PROJECT-3: Data Warehouse

## Overview


This project provides the schema and ETL to create and populate an analytics database for the music streaming app Sparkify.

It used the AWS Redshift to store data that stream from the data was store in S3 with JSON fomart.


## Structure

The project contains the following elements:

* `dwh.cfg/` defines the variable for IAM, DWH, IAM_ROLE, S3

* `sql_queries.py` defines the queries that use for the drops and create table

* `create_tables.py` the script to drops and creates for the tables

* `etl.py` defines the ETL pipeline, which load data from S3 and insert to the tables 

* `project-setup.ipynb` the notebook for interactively develop and run python code to be used in etl.py and create_tables.py

* `main.ipynb` this notebook is for run the project. It will connect database and create the tables, load data to table.

* `test.ipynb` use for test data after load data successed.



## Schema
List tables will serve for 3 example queries 
* `staging_events`
* `staging_songs `
* `songplays `
* `users `
* `songs `
* `artists `
* `time `

## How to run 

### Prerequisite

1.Create the user, get the KEY, SECRET and set to `dwh.cfg` file

2.Create the IAM_ROLE and attach the policy `AmazonS3ReadOnlyAccess` for that role, get ARN set to `dwh.cfg` file

3.Set value for DWH, S3 variables in the `dwh.cfg` file

4.Create the AWS Redshift with:
* type: dc2.large
* number of node: 4
* location: us-west-2 (as AWS S3 bucket)

and allow access to the database of Redshift by add rule for `default` security group

5.Check raw data in the S3
* s3://udacity-dend/log_data
* s3://udacity-dend/song_data


### Ready to run

To run the project in Udacity's Project Workspace, open `main.ipynb` and  `Run All Cells`

To test the project in Udacity's Project Workspace, open `test.ipynb` and  `Run All Cells`

