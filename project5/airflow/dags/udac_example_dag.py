from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2022, 1, 15),
    'depends_on_past': False,
    'catchup': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False
}

dag = DAG('udac_example_dag6',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@daily'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


# stage_events_to_redshift 

# stage_events_to_redshift = StageToRedshiftOperator(
#     task_id='Stage_events',
#     redshift_conn_id="redshift",
#     aws_credentials_id="aws_credentials",
#     table="staging_events",
#     s3_bucket="udacity-dend",
#     #s3_key="log_data/{{execution_date.year}}/{{execution_date.month}}/{{execution_date.strftime('%Y-%m-%d')}}-events.json",
#     s3_key="log_data/2018/11/2018-11-01-events.json",
#     json_option="s3://udacity-dend/log_json_path.json",
#     dag=dag
# )



# stage_songs_to_redshift 

# stage_songs_to_redshift = StageToRedshiftOperator(
#     task_id='Stage_songs',
#     redshift_conn_id="redshift",
#     aws_credentials_id="aws_credentials",
#     table="staging_songs",
#     s3_bucket="udacity-dend",
#     s3_key="song_data/",
#     json_option="auto",
#     dag=dag
# )


# load_songplays_table = LoadFactOperator(
#     task_id='Load_songplays_fact_table',
#     dag=dag
# )


# load_songplays_table = LoadFactOperator(
#     task_id='Load_songplays_fact_table',
#     redshift_conn_id="redshift",
#     table_target="songplays",
#     table_select="songplay_table_select",
#     truncate=False,
#     dag=dag
# )


# load_user_dimension_table = LoadDimensionOperator(
#     task_id='Load_user_dim_table',
#     dag=dag
# )

# load_users_dimension_table = LoadDimensionOperator(
#     task_id='Load_user_dim_table',
#     redshift_conn_id="redshift",
#     table_target="users",
#     table_select="user_table_select",
#     truncate=False,
#     dag=dag
# )


# load_song_dimension_table = LoadDimensionOperator(
#     task_id='Load_song_dim_table',
#     dag=dag
# )

# load_song_dimension_table = LoadDimensionOperator(
#     task_id='Load_song_dim_table',
#     redshift_conn_id="redshift",
#     table_target="songs",
#     table_select="song_table_select",
#     truncate=False,
#     dag=dag
# )

# load_artist_dimension_table = LoadDimensionOperator(
#     task_id='Load_artist_dim_table',
#     dag=dag
# )

# load_artist_dimension_table = LoadDimensionOperator(
#     task_id='Load_artist_dim_table',
#     redshift_conn_id="redshift",
#     table_target="artists",
#     table_select="artist_table_select",
#     truncate=False,
#     dag=dag
# )

# load_time_dimension_table = LoadDimensionOperator(
#     task_id='Load_time_dim_table',
#     dag=dag
# )

# load_time_dimension_table = LoadDimensionOperator(
#     task_id='Load_time_dim_table',
#     redshift_conn_id="redshift",
#     table_target="time",
#     table_select="time_table_select",
#     truncate=False,
#     dag=dag
# )

# run_quality_checks = DataQualityOperator(
#     task_id='Run_data_quality_checks',
#     dag=dag
# )

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id="redshift",
    table_targets = ["songplays", "users", "songs", "artists", "time"],
    dag=dag
)


# run_quality_checks = DummyOperator(task_id='Run_data_quality_checks',  dag=dag)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


# start_operator >> stage_events_to_redshift
# start_operator >> stage_songs_to_redshift

# stage_events_to_redshift >> load_songplays_table
# stage_songs_to_redshift >> load_songplays_table


# load_songplays_table >> load_users_dimension_table
# load_songplays_table >> load_song_dimension_table
# load_songplays_table >> load_artist_dimension_table
# load_songplays_table >> load_time_dimension_table


# load_users_dimension_table >> run_quality_checks
# load_song_dimension_table >> run_quality_checks
# load_artist_dimension_table >> run_quality_checks
# load_time_dimension_table >> run_quality_checks

# run_quality_checks >> end_operator


start_operator >> run_quality_checks
run_quality_checks >> end_operator

