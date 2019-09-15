from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

class DataQualityOperator(BaseOperator):
    
    '''
    Aim : Custom designed operator which does the data quality check by comparing the counts of the data before loading and after loading into redshift. 
    
    Input : 
        redshift_conn_id: redshift connection id - This need to be added in the connections of airflow UI 
        source_sql : source sql  -- This is to get the source count before loading. This is coming from sql_statements.sql file 
        target_sql : target sql  -- This is to get the target count after loading. This is coming from sql_statements.sql file 
               
    Output : Returns none 
    '''

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 source_sql= "",
                 target_sql="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.source_sql = source_sql
        self.target_sql = target_sql

    def execute(self, context):
        self.log.info('DataQualityOperator not implemented yet')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Getting source records count from sql statement")
        src_records = redshift.get_records(self.source_sql)
        src_count = src_records[0][1]
        
        self.log.info("Getting target records count from sql statement")
        tgt_records = redshift.get_records(self.target_sql)
        tgt_count = tgt_records[0][1]

        if src_count != tgt_count:
            raise ValueError(f"Data quality check failed.Source count has {src_count} records and Target count has {tgt_count} records")
        logging.info(f"Data quality check passed with {tgt_count} records")

   