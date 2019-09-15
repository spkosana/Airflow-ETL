from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    
    '''
    Aim : Custom designed operator which loads data from source to staging tables in Redshift cluster. 
    
    Input : 
        redshift_conn_id: redshift connection id - This need to be added in the connections of airflow UI 
        aws_credentials_id : aws connection key and secret -- This need to be added in the connection sof airflow UI
        table : table that we are currently working on loading. 
        copy_sql : copy sql -- This is for inserting data into reshift tables
        s3_bucket : s3 bucket -- This is where the source data is present. 
        s3_key : s3 key -- This is the subfolder path up to actual files
        json_path : json path -- this takes auto or json file as parameter to insert into the reshift tables.   
        
    Output : Returns none 
    '''
    
    ui_color = '#358140'    
    

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 create_sql="",
                 copy_sql="",
                 s3_bucket="",
                 s3_key="",
                 json_path="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path
        self.create_sql = create_sql
        self.copy_sql = copy_sql
        self.aws_credentials_id = aws_credentials_id

    def execute(self, context):
        self.log.info('StageToRedshiftOperator execution start')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Creating table from sql statement")
        redshift.run(self.create_sql)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}/".format(self.s3_bucket, rendered_key)
        if self.json_path=="auto":
            json_format="auto"
        else:
            json_format = f"s3://{self.s3_bucket}/{self.json_path}"
        formatted_sql = self.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            json_format
        )
        redshift.run(formatted_sql)





