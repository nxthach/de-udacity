# PROJECT-3: Data Warehouse

## Overview


This project will build an Airflow pipeline that will do:
- Load log event and song data from S3 to Redshift
- After done load data, will load data to fact and dimension tabls
- Finally will check quality data were load

## Structure

```
dags
    udac_example_dag.py     The main dag file

plugins
    helpers
        sql_queries.py:     SQL helper for load data and check quality data

    operators
        data_quality.py     Build DataQualityOperator
        load_dimension.py   Build LoadDimensionOperator
        load_fact.py        Build LoadFactOperator
        stage_redshift.py   Build StageS3ToRedshiftOperator

create_tables.sql           DDL to initialize tables on Redshift
```
* StageS3ToRedshiftOperator: will load data from S3 to Redshift to staging table (`staging_events`, `staging_songs`)
* LoadFactOperator: will load data from staging tables to fact table (`songplays`)
* LoadDimensionOperator: will load data from staging tables to dimension table (`users`, `songs`, `artists`, `time`)
* DataQualityOperator: will check quality on tables were load by check null data and checking number of records



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

2.Create Redshift cluster

3.Create AWS credentials

4.Create connections (aws_credentials, redshift) on Airflow by go to Airflow UI. 
Click menu Admin > Connections > Create

5.Initalize tables to Redshift by `create_tables.sql`



### Ready to run

1.Run Airflow by command `/opt/airflow/start.sh`

2.The DAG will run by trigger daily from 2018/11/1 

