from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)

from helpers import SqlQueries

default_args = {
    'owner': 'udacity',    
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

dag = DAG('udac_project_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',    
    dag=dag,
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    table = 'staging_events',
    s3_bucket = 'udacity-dend',
    s3_key = 'log_data',
    json_format = "json 's3://udacity-dend/log_json_path.json'"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials', 
    table = 'staging_songs',
    s3_bucket = 'udacity-dend',
    s3_key = 'song_data',
    json_format = "format as json 'auto'"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table = 'songplay_table',
    table_col = "songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent",
    sql_load_query = SqlQueries.songplay_table_insert,
    redshift_conn_id = 'redshift'
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table = 'user_table',
    table_col = "user_id, first_name, last_name, gender, level",
    sql_load_query = SqlQueries.user_table_insert,
    operation = "truncate", #(truncate/append)
    redshift_conn_id = 'redshift'
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table = 'song_table',
    table_col = "song_id, title, artist_id, year, duration",
    sql_load_query = SqlQueries.song_table_insert,
    operation = "truncate", #(truncate/append)
    redshift_conn_id = 'redshift'
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table = 'artist_table',
    table_col = "artist_id, artist_name, location, latitude, longitude",
    sql_load_query = SqlQueries.artist_table_insert,
    operation = "truncate", #(truncate/append)
    redshift_conn_id = 'redshift'
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table = 'time_table',
    table_col = "start_time, hour, day, week, month, year, weekday",
    sql_load_query = SqlQueries.time_table_insert,
    operation = "truncate", #(truncate/append)
    redshift_conn_id = 'redshift'
)

check_sql="""
    SELECT COUNT(*)
    FROM (
    SELECT * FROM {}
    WHERE {} IS NULL
    )
"""
check_sql1 = check_sql.format('artist_table', 'artist_id')
check_sql2 = check_sql.format('time_table', 'start_time')
exp_val = 0

run_quality_checks = DataQualityOperator(
    task_id='data_quality_check',
    dag=dag,
    dq_checks=[{'check_sql': check_sql1, 'expected_result': exp_val}, {'check_sql': check_sql1, 'expected_result': exp_val}],
    redshift_conn_id='redshift'
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator