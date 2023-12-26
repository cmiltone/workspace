from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = 'redshift',
                 checks = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.checks = checks

    def execute(self, context):
        hook = PostgresHook(self.redshift_conn_id)
        failed_str = 'Data quality check failed. {} returned no results'
        failed_str2 = 'Data Quality Check failed for {}'

        for check in self.checks:
            records = hook.get_records(check['query'])
            if len(records) < 1 or len(records[0])< 1:
                raise ValueError(failed_str.format(check['query']))
            
            if (records[0][0] != check['expected_result']):
                raise ValueError(failed_str2.format(check['query']))

        self.log.info('Data Quality checks passed')
