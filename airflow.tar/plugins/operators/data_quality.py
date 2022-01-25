from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 data_quality_checks = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.data_quality_checks = data_quality_checks
        
    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        for quality_check_pair in data_quality_checks:
            for key in quality_check_pair:
                sql = key
                expected_result = quality_check_pair[key]
                
                records = redshift_hook.get_records(sql)
                num_records = records[0][0]
                if num_records > expected_result:
                    raise ValueError("Data quality check failed")
                self.log.info("Data quality check passed with columns containing no NULL values{records[0][0]} records")
        
        
        