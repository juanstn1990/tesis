from google.cloud import bigquery
class Loader:
    def __init__(self):
        pass

    def load_bigquery_df(self, dataframe, table_id, disposition_method):
        """
        disposition_method
        """
        client = bigquery.Client()
        job_config = bigquery.LoadJobConfig(
            write_disposition=disposition_method,
        )
        job = client.load_table_from_dataframe(
            dataframe, table_id, job_config=job_config
        )
        job.result()
        table = client.get_table(table_id)
        print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")