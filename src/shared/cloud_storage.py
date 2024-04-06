from google.cloud import storage

class CloudStorage:
    def __init__(self):
        self.client = storage.Client()

    def list_files_in_gcs(self, bucket_name, folder_name):
        bucket = self.client.get_bucket(bucket_name)
        blobs = bucket.list_blobs(prefix=folder_name)
        return [blob.name for blob in blobs]

    def download_blob(self, bucket_name, source_blob_name, destination_file_name):
        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(source_blob_name)
        blob.download_to_filename(destination_file_name)
        print(f'Archivo {source_blob_name} descargado en {destination_file_name}.')