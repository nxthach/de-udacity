from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from helpers import SqlQueries

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table_targets="",
                 truncate=False,
                 
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_targets = table_targets


    def execute(self, context):
        self.log.info(f"List table need to check: {self.table_targets}")
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        check_nulls_queries = []
        check_count_queries = []
        check_tables = []
        
        if "songplays" in self.table_targets:
            check_tables.append("songplays")
            check_nulls_queries.append(SqlQueries.songplays_check_nulls)
            
            
        if "users" in self.table_targets:
#             check_nulls_queries.append(SqlQueries.users_check_nulls)
            self.log.info("check users for songplays")
            
        if "songs" in self.table_targets:
            self.log.info("check songs for songplays")
        
        if "artists" in self.table_targets:
            self.log.info("check artists for songplays")
            
        if "time" in self.table_targets:
            self.log.info("check time for songplays")
            
            
        #for query in check_nulls_queries:
        for idx, query in enumerate(check_nulls_queries):            
            self.log.info(f"Checking null on table: {check_tables[idx]}...")
            
            records = redshift.get_records(query)
            
            if len(records) < 1:
                raise ValueError("No results returned by redshift")
                
            counts = records[0][0]
            
            if counts > 0:                
                raise ValueError(f"Table {check_tables[idx]} exsited null data!")
            
            self.log.info(f"Data quality on table {check_tables[idx]} check passed!")

            
            
            