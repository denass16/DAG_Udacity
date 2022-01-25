from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 table = "",
                 table_col = "",
                 sql_load_query = "",
                 operation = "",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.table_col = table_col
        self.operation = operation
        self.sql_load_query = sql_load_query
    
    def execute(self, context):
        self.log.info("connecting to redshift cluster...")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.operation=='truncate':
            self.log.info("truncating and inserting data into {}...".format(self.table))
            redshift.run("""
                         TRUNCATE {self.table}
                         INSERT INTO {self.table} ({self.table_col}) 
                         {self.sql_load_query}
                         """.format(self.table, self.table_col, self.sql_load_query)
                        )
            self.log.info("inserting data into {} completed".format(self.table))
        if self.operation=='append':
            self.log.info("inserting data into {}...".format(self.table))
            redshift.run("""
                         INSERT INTO {self.table} ({self.table_col}) 
                         {self.sql_load_query}
                         """.format(self.table, self.table_col, self.sql_load_query)
                        )
            self.log.info("inserting data into {} completed".format(self.table))