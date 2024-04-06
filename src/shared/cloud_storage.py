from google.cloud import storage

# Autenticación de tu aplicación
# Debes asegurarte de que tengas configuradas las credenciales adecuadas para acceder a GCS
# Consulta la documentación de Google Cloud para obtener más información sobre cómo configurar las credenciales.
client = storage.Client()

def list_files_in_gcs(bucket_name, folder_name):
    """List all files in GCS bucket"""
    bucket = client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=folder_name)
    return [blob.name for blob in blobs]

def download_blob(bucket_name, source_blob_name, destination_file_name):
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)
    print(f'Archivo {source_blob_name} descargado en {destination_file_name}.')

# Ejemplo de uso
d = list_files_in_gcs('pdfs_utadeo', 'pdfs')
print(d)
download_blob('pdfs_utadeo', 'pdfs/C_PROCESO_17-4-6010410_276001622_25657719.pdf', 'C_PROCESO_17-4-6010410_276001622_25657719.pdf')