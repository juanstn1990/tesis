import os
import pandas as pd

from azure.core.credentials import AzureKeyCredential
from azure.ai.formrecognizer import DocumentAnalysisClient
from azure.ai.documentintelligence.models import AnalyzeResult

from shared.prompts.prompts import df_transform
from shared.transformer.chat_gpt import get_completion
from shared.cloud_storage import CloudStorage
from shared.loaders.bigquery_loader import Loader


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
    for element in lista:
        destino = f"/home/juanstn/{element.split('/')[-1]}"
        print(destino)
        cloud.download_blob("pdfs_utadeo", element, destino)
        analyze_layout(destino)
        os.remove(destino)
if __name__ == "__main__":
    main()
