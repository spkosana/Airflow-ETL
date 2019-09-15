from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)

from helpers import CreateSql

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime.now(),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'email_on_retry': False,
    'catchup_by_default': False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

''' Loading data from s3 to Redshift using custom designed StageToRedshift operator which takes the events data from s3 and loads into staging_events table '''
stage_events_to_redshift = StageToRedshiftOperator(
     redshift_conn_id="redshift",
     aws_credentials_id="aws_credentials",
     task_id='Stage_events',
     table="staging_events",
     create_sql=CreateSql.staging_events_table_create,
     copy_sql=CreateSql.copy_sql_json,
     s3_bucket="udacity-dend",
     s3_key="log_data",
     json_path="log_json_path.json",
    dag=dag
)

''' Loading data from s3 to Redshift using custom designed StageToRedshift operator which takes the songs data from s3 and loads into staging_songs table '''
stage_songs_to_redshift = StageToRedshiftOperator(
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    task_id='Stage_songs',
    table="staging_songs",
    create_sql=CreateSql.staging_songs_table_create,
    copy_sql=CreateSql.copy_sql_json,
    s3_bucket="udacity-dend",
    s3_key="song_data",
    json_path="auto",
    dag=dag
)

''' Loading songplays data into songplay fact table using custom designed LoadFactOperator which inserts data into fact table as per requirements '''
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    redshift_conn_id="redshift",
    table="songplays",
    create_sql=CreateSql.songplays_table_create,
    insert_sql=CreateSql.songplays_table_insert,
    dag=dag
)

''' Loading user data into users dimension table using custom designed LoadDimensionOperator which inserts data into user table as per requirements '''
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    redshift_conn_id="redshift",
    table="users",
    create_sql=CreateSql.user_table_create,
    insert_sql=CreateSql.user_table_insert,
    dag=dag
)

''' Loading song data into songs dimension table using custom designed LoadDimensionOperator which inserts data into songs table as per requirements '''
load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    redshift_conn_id="redshift",
    table="songs",
    create_sql=CreateSql.songs_table_create,
    insert_sql=CreateSql.songs_table_insert,
    dag=dag
)

''' Loading artists data into artists dimension table using custom designed LoadDimensionOperator which inserts data into artists table as per requirements '''
load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    redshift_conn_id="redshift",
    table="artists",
    create_sql=CreateSql.artists_table_create,
    insert_sql=CreateSql.artists_table_insert,
    dag=dag
)

''' Loading time data into time dimension table using custom designed LoadDimensionOperator which inserts data into time table as per requirements '''
load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    redshift_conn_id="redshift",
    table="time",
    create_sql=CreateSql.time_table_create,
    insert_sql=CreateSql.time_table_insert,
    dag=dag
)

''' Data quality operator which checks for the data quality to ensure the data is populated a expected '''
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id="redshift",
    source_sql=CreateSql.source_data_quality_query,
    target_sql=CreateSql.target_data_quality_query,
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

''' Assignment of start operator to staging tables and fact tables '''
start_operator >> [stage_events_to_redshift , stage_songs_to_redshift] >> load_songplays_table 

''' Continuing the flow to load data in dimensions tables and then then running the data quality check operator '''
load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks >> end_operator


