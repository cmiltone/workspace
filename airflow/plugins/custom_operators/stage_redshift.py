from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.secrets.metastore import MetastoreBackend

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = 'redshift',
                 aws_credentials = 'aws_credentials',
                 sql = '',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials = aws_credentials

    def execute(self, context):
        conn = MetastoreBackend().get_connection(self.aws_credentials)
        hook = PostgresHook(self.redshift_conn_id)
        hook.run(self.sql.format(
            access = conn.login,
            secret = conn.password
        ))

        self.log.info('Data Staged To Redshift from S3')
