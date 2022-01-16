from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from helpers import SqlQueries

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    
    
    insert_template = """
        INSERT INTO {}
        {}
    """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table_target="",
                 table_select="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_target = table_target
        self.table_select = table_select

    def execute(self, context):
        
        self.log.info("Get redshift connection")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        
        # Run query to load the table
        sql_table_select = getattr(SqlQueries, self.table_select)
        sql_insert = self.insert_template.format(self.table_target, sql_table_select)
        
        self.log.info(f"sql_insert: {sql_insert}")
        
        self.log.info(f"Start inserting table: {self.table_target}")
        
        redshift.run(sql_insert)
        
        self.log.info(f"End inserting table: {self.table_target}")
