from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    
    '''
    Aim : Custom designed operator which loads data to fact table in Redshift cluster. 
    
    Input : 
        redshift_conn_id: redshift connection id - This need to be added in the connections of airflow UI 
        table : table that we are currently working on loading. 
        create_sql : create sql  -- This is for creating table in redshift based on the value. This is coming from sql_statements.sql file 
        insert_sql : Insert sql  -- This is where the data is inserted into table. This is coming from sql_statements.sql file 
               
    Output : Returns none 
    '''

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 create_sql="",
                 insert_sql="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.create_sql = create_sql
        self.insert_sql = insert_sql

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Creating table from sql statement")
        redshift.run(self.create_sql)
        
        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))
        
        self.log.info("inserting data into fact table")
        redshift.run(self.insert_sql)
        
        
