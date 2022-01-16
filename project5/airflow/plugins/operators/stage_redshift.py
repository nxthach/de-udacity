from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    
    template_fields = ("s3_key",)

    # sql to copy data to redshift
    copy_sql_template = """
        COPY {}
        FROM '{}'
        JSON '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        COMPUPDATE off
        REGION 'us-west-2'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 json_option="auto",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_option = json_option
        

    def execute(self, context):
        self.log.info("Get aws hook")
        aws_hook = AwsHook(self.aws_credentials_id)
        
        self.log.info("Getting aws credentials")
        credentials = aws_hook.get_credentials()
        
        self.log.info("Get redshift connection")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Clear data before insert
        self.log.info(f"Start clearing data from table {self.table}")
        redshift.run("DELETE FROM {}".format(self.table))
        self.log.info(f"End clearing data from table {self.table}")
            
        # Copy from s3 to redshift
        self.log.info(f"Start copying data from S3 to table {self.table}")
        
        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
        
        copy_sql = StageToRedshiftOperator.copy_sql_template.format(
            self.table,
            s3_path,
            self.json_option,
            credentials.access_key,
            credentials.secret_key
        )
        redshift.run(copy_sql)
        self.log.info(f"End copying data from S3 to table {self.table}")




