from airflow.providers.postgres.hooks.postgres import PostgresHook
# from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    sql_template = """
        INSERT INTO {} ({})
        {};
    """

    @apply_defaults
    def __init__(self,
                 # Define operators params (with defaults) here
                 redshift_conn_id="",
                 target_table="",
                 target_columns="",
                 query="",
                 insert_mode="append",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.target_table = target_table
        self.target_columns = target_columns
        self.query = query
        self.insert_mode = insert_mode

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.insert_mode == "truncate":
            redshift.run("DELETE FROM {}".format(self.target_table))


        query_name = ""
        if self.query == "user_table_insert":
            query_name = SqlQueries.user_table_insert

        elif self.query == "song_table_insert":
            query_name = SqlQueries.song_table_insert

        elif self.query == "artist_table_insert":
            query_name = SqlQueries.artist_table_insert

        elif self.query == "time_table_insert":
            query_name = SqlQueries.time_table_insert
        else:
            self.log.info("Not found")

        formatted_sql = LoadDimensionOperator.sql_template.format(
            self.target_table,
            self.target_columns,
            query_name.format(self.target_table, self.target_table)
        )


        self.log.info("RUnning: {}".format(formatted_sql))
        redshift.run(formatted_sql)
        self.log.info("Finished")