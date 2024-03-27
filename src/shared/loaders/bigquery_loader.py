from google.cloud import bigquery
class Loader:
    def __init__(self):
        pass

    def load_bigquery_df(self, dataframe, table_id):
        client = bigquery.Client()
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("nombre_entidad", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("nivel_entidad", bigquery.enums.SqlTypeNames.STRING),
            ],
            write_disposition="WRITE_TRUNCATE",
        )
        job = client.load_table_from_dataframe(
            dataframe, table_id, job_config=job_config
        )
        job.result()
        table = client.get_table(table_id)
        print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")