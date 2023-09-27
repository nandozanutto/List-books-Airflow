# from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define operators params (with defaults) here
                 redshift_conn_id="",
                 target_table="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.target_table = target_table

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        tables = []
        check_nulls_queries = []
        check_count_queries = []
        tables = self.target_table.replace(" ", "").split(",")
        if "songplays" in tables:
            check_nulls_queries.append(SqlQueries.songplays_check_nulls)
            check_count_queries.append(SqlQueries.songplays_check_count)
        if "users" in tables:
            check_nulls_queries.append(SqlQueries.users_check_nulls)
            check_count_queries.append(SqlQueries.users_check_count)
        if "songs" in tables:
            check_nulls_queries.append(SqlQueries.songs_check_nulls)
            check_count_queries.append(SqlQueries.songs_check_count)
        if "artists" in tables:
            check_nulls_queries.append(SqlQueries.artists_check_nulls)
            check_count_queries.append(SqlQueries.artists_check_count)
        if "time" in tables:
            check_nulls_queries.append(SqlQueries.time_check_nulls)
            check_count_queries.append(SqlQueries.time_check_count)

        # Executing quality checks
        for query in check_nulls_queries:
            records = redshift.get_records(query)
            self.log.info(f"RESULTS: {records}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"check failed. {query} returned results.")
            num_records = records[0][0]
            if num_records > 0:
                raise ValueError(f"check failed. {query} contained > 0 rows")
            self.log.info(f"{query} check passed with {records[0][0]} records")

        for query in check_count_queries:
            records = redshift.get_records(query)
            self.log.info(f"RESULTS: {records}")
            self.log.info(f"RESULTS: {query} had {records[0][0]} records.")

        self.log.info("finished")