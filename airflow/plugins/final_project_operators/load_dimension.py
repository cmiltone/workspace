from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    allowed_modes = ('delete-load', 'append-only')

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = 'redshift',
                 insert_mode='delete-load', # allows switching between insert modes
                 truncate_sql = '',
                 insert_sql = '',
                 create_sql = '',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        if insert_mode not in self.allowed_modes:
            raise ValueError('{mode} is not a valid mode')
        self.redshift_conn_id = redshift_conn_id
        self.insert_mode = insert_mode
        self.create_sql = create_sql
        self.inser_sql = insert_sql
        self.truncate_sql = truncate_sql

    def execute(self, context):
        hook = PostgresHook(self.redshift_conn_id)
        hook.run(self.create_sql)
        if self.insert_mode == 'delete-load':
            hook.run(self.truncate_sql)
        hook.run(self.inser_sql)
        self.log.info('Loaded Dimension data')
