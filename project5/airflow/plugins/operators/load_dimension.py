from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from helpers import SqlQueries

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    
    insert_template = """
        INSERT INTO {}
        {}
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table_target="",
                 table_select="",
                 truncateData=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_target = table_target
        self.table_select = table_select
        self.truncateData = truncateData

    def execute(self, context):
        
        self.log.info("Get redshift connection")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # Truncate the target table if truncateData=True
        self.log.info(f"truncateData: {self.truncateData}")
        if self.truncateData:            
            self.log.info(f"Start truncating table: {self.table_target}")
            
            delete_query = "DELETE FROM {}".format(self.table_target)
            redshift.run(delete_query)
            
            self.log.info(f"End truncating table: {self.table_target}")

        # Run query to load data to table
        sql_table_select = getattr(SqlQueries, self.table_select)
        sql_insert = self.insert_template.format(self.table_target, sql_table_select)
        self.log.info(f"sql_insert: {sql_insert}".format(sql_insert))
        
        self.log.info(f"Start inserting table: {self.table_target}")
        
        redshift.run(sql_insert)
        
        self.log.info(f"End inserting table: {self.table_target}")
