from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = 'redshift',
                 create_sql = '',
                 insert_sql = '',
                 truncate_sql = '',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.create_sql = create_sql
        self.insert_sql = insert_sql
        self.truncate_sql = truncate_sql

    def execute(self, context):
        hook = PostgresHook(self.redshift_conn_id)

        hook.run(self.create_sql)
        hook.run(self.truncate_sql)
        hook.run(self.insert_sql)

        self.log.info('Loaded Fact data')
