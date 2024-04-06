import os
import pandas as pd

from azure.core.credentials import AzureKeyCredential
from azure.ai.formrecognizer import DocumentAnalysisClient
from azure.ai.documentintelligence.models import AnalyzeResult

from shared.prompts.prompts import df_transform
from shared.transformer.chat_gpt import get_completion
from shared.cloud_storage import CloudStorage
from shared.loaders.bigquery_loader import Loader
from shared.extractors.api.bigquery_extractor import Extractor


endpoint = "https://utadeo.cognitiveservices.azure.com/"
key = os.getenv("AZURE_KEY")

def analyze_layout(path_to_sample_documents):
    document_intelligence_client = DocumentAnalysisClient(
        endpoint=endpoint, credential=AzureKeyCredential(key)
    )

    with open(path_to_sample_documents, "rb") as f:
        poller = document_intelligence_client.begin_analyze_document(
            "prebuilt-invoice", document=f, locale="en-US"
        )

    result: AnalyzeResult = poller.result()

    if result.tables:
        for table_idx, table in enumerate(result.tables):
            print(f"Table # {table_idx} has {table.row_count} rows and {table.column_count} columns")
            if table.row_count > 3:
                table_data = {}
                for cell in table.cells:
                    row_index = cell.row_index
                    column_index = cell.column_index
                    content = cell.content
                    if row_index not in table_data:
                        table_data[row_index] = {}
                    table_data[row_index][column_index] = content
                df = pd.DataFrame.from_dict(table_data, orient="index").sort_index(axis=1)
                df = df.apply(lambda x: x.str.replace('\n', ''))
                results = get_completion(df_transform.format(dataframe=df))
                print(results)

def main():
    cloud = CloudStorage()
    lista = cloud.list_files_in_gcs("pdfs_utadeo", "pdfs/")
    lista.remove("pdfs/")
    df_storage = pd.DataFrame()
    df_storage["file_path"] = lista
    df_storage["flag"] = 0

    Extract = Extractor()
    df_bigquery = Extract.extract_bigquery("artful-sled-419501.secop.pdf_control")
    nuevos_pdfs = df_storage[~df_storage["file_path"].isin(df_bigquery["file_path"])]
    
    if not nuevos_pdfs.empty:
        load = Loader()
        load.load_bigquery_df(nuevos_pdfs, "artful-sled-419501.secop.pdf_control", "WRITE_APPEND")

    df_bigquery_new = Extract.extract_bigquery_flag("artful-sled-419501.secop.pdf_control")
    if not df_bigquery_new.empty:
        for row in df_bigquery_new.itertuples():
            print(row.file_path)
            destino = f"/home/juanstn/{row.file_path.split('/')[-1]}"
            cloud.download_blob("pdfs_utadeo", row.file_path, destino)
            analyze_layout(destino)
            Extract.update_bigquery("artful-sled-419501.secop.pdf_control", "flag", 1, f"file_path = '{row.file_path}'")
            os.remove(destino)
    else:
        print("No hay nuevos archivos para procesar")

if __name__ == "__main__":
    main()
