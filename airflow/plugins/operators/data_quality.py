from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables=[],
                 expected_results={},
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
        self.expected_results = expected_results

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for table in self.tables:
            records = redshift.get_records(f"SELECT COUNT(*) FROM {table}")

            if len(records) < 1 or len(records[0]) < 1:
                self.log.error(f"{table} returned no results")
                raise ValueError(f"Data quality check failed. {table} returned no results")

            num_records = records[0][0]
            expected_result = self.expected_results.get(table, 0)

            if num_records != expected_result:
                self.log.error(f"Data quality check failed for {table}. Expected {expected_result} records, but found {num_records}")
                raise ValueError(f"Data quality check failed for {table}. Expected {expected_result} records, but found {num_records}")

            self.log.info(f"Data quality on table {table} check passed with {num_records} records")
