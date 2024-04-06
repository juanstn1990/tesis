from google.cloud import bigquery

class Extractor:

    def __init__(self):
        self.client = bigquery.Client()

    def extract_bigquery(self, table_id):
        query = f"SELECT * FROM {table_id}"
        df = self.client.query(query).to_dataframe()
        return df
    
    def extract_bigquery_flag(self, table_id):
        query = f"SELECT * FROM {table_id} where flag = 0"
        df = self.client.query(query).to_dataframe()
        return df
    
    def update_bigquery(self, table_id, column, value, condition):
        query = f"UPDATE {table_id} SET {column} = {value} WHERE {condition}"
        query_job = self.client.query(query)
        query_job.result()
        print(f"Actualizadas {query_job.num_dml_affected_rows} filas")