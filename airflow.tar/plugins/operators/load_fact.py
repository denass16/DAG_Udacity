from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 table = "",
                 table_col = "",
                 sql_load_query = "",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.table_col = table_col
        self.sql_load_query = sql_load_query

    def execute(self, context):
        self.log.info("connecting to redshift cluster...")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("inserting data into {}...".format(self.table))
        redshift.run("""INSERT INTO {} ({}) 
                             {}""".format(self.table, self.table_col, self.sql_load_query)
                    )
        self.log.info("inserting data into {} completed".format(self.table))
