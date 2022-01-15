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
                 truncate=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_target = table_target
        self.table_select = table_select
        self.truncate = truncate

    def execute(self, context):
        # Acquire redshift connection
        self.log.info("Acquiring redshift connection")
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # Truncate the target table if Truncate=True
#         self.log.info("Truncate mode: {}".format(self.truncate))
#         if self.truncate:
#             delete_query = "DELETE FROM {}".format(self.table)
#             self.log.info("Start truncating table: {}".format(self.table))
#             redshift.run(delete_query)
#             self.log.info("End truncating table: {}".format(self.table))

        # Run query to load the table
        self.log.info("Start inserting table: {}".format(self.table_target))
        
#         select_query = getattr(SqlQueries, "{}_table_query".format(self.table))

        
        sql_table_select = getattr(SqlQueries, self.table_select)
        sql_insert = self.insert_template.format(self.table_target, sql_table_select)
        
        self.log.info("sql_insert: {}".format(sql_insert))
        
        redshift.run(sql_insert)
        
        self.log.info("End inserting table: {}".format(self.table_target))
