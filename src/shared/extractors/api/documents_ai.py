from typing import Optional, Sequence
from google.api_core.client_options import ClientOptions
from google.cloud import documentai
from google.cloud import storage
from prettytable import PrettyTable
from google.cloud import documentai_v1 as documentai
import pandas as pd
from shared.transformer.chat_gpt import get_completion
import ast
from src.shared.loaders import bigquery_loader

project_id = 'utadeo-418221'
location = 'us'
processor_id = '96b58070c12117b2'
file_path = r'/tmp/C_PROCESO_21-12-12645260_276001622_98573431.pdfx'
mime_type = 'application/pdf'
processor_version = 'pretrained-form-parser-v2.1-2023-06-26'


def find_column_names(columnes, find_columns):
    for valor in find_columns:
        if valor in columnes:
            return True
    return False


def process_document_form_sample(
    project_id: str,
    location: str,
    processor_id: str,
    processor_version: str,
    file_path: str,
    mime_type: str,
) -> documentai.Document:
    document = process_document(
        project_id, location, processor_id, processor_version, file_path, mime_type
    )
    text = document.text
    print(f"There are {len(document.pages)} page(s) in this document.")
    all_df = pd.DataFrame()
    for page in document.pages:
        print(f"\n\n**** Page {page.page_number} ****")
        print(f"\nFound {len(page.tables)} table(s):")
        for table in page.tables:
            num_columns = len(table.header_rows[0].cells)
            columnes = print_table_rows(table.header_rows, text)
            find_columns = ['ITEM','ITEMS','DESCRIPCION','PRESENTACION','LABORATORIO','VALOR','NOMBRE','CONCENTRACION','UNITARIO','DESCRIPCIÓN','CONCENTRACIÓN']
            value_columns = find_column_names(columnes, find_columns)

            if num_columns > 2:
                if value_columns:
                    df = print_table_rows(table.body_rows, text)
                else:
                    df_headers = print_table_rows(table.header_rows, text)
                    df_rows = print_table_rows(table.body_rows, text)
                    df = df_headers + df_rows

                respuesta = get_completion(f"""
                organiza esto en un dataframe, crea la siguientes columnas
                item, Descripcion, Concentracion, laboratorio , valor
                y forma farmaceutica (tableta, capsula, jarave, solucion oral, solucion inyectable, crema, gel, polvo, suspension, gotas, ovulo, supositorio, inyectable, inhalador, parche, shampoo, locion, enema, pasta, aerosol, liquido,crema, polvo para solucion oral, polvo para suspension oral)
                , si no existe el dato para la columna concentracion intenta extraerlo de la descripcion,
                
                si no existe el dato para la columna presentacion intenta extraerlo de la descripcion,pero manten la descripcion con el nombre original 
                para el valor manten que sea un dato numerico y que no tenga \n1
                Hazlo tu no me entregues codigo {df}""" + """
                    ,requiero que la salida tenga esta estructura , si o si siempre la debe tener, haz todos los items

                    [
                        {
                            'item': '1',
                            'Descripcion': 'Acetaminofen',
                            'Concentracion': '500mg',
                            'Forma_farmaceutica': 'Tableta',
                            'Laboratorio': 'Genfar',
                            'Valor_Unitario': 125
                        },
                        {
                            'item': '2',
                            'Descripcion': 'Amoxicilina',
                            'Concentracion': '250mg',
                            'Forma_farmaceutica': 'Capsula',
                            'Laboratorio': 'Aust',
                            'Valor_Unitario': 90
                        }
                    }
                    ]
                    si el dataframe no correspode a medicamentos solo devuelve {}
                """, model="gpt-3.5-turbo")
                print(respuesta)
                data_list = ast.literal_eval(respuesta)
                df = pd.DataFrame(data_list)
                print(df)
                all_df = pd.concat([all_df, df], ignore_index=True)
    all_df = all_df.applymap(lambda x: x.upper() if isinstance(x, str) else x)
    all_df['file_path'] = file_path
    return all_df

def print_table_rows(
    table_rows: Sequence[documentai.Document.Page.Table.TableRow], text: str
) -> None:
    table_ok = []
    for table_row in table_rows:
        row_text = ""
        for cell in table_row.cells:
            cell_text = layout_to_text(cell.layout, text)
            row_text += f"{repr(cell_text.strip())} | "

        print(row_text)

        row_text = row_text.replace('\n','')
        row_text = row_text.replace("'",'')
        row_text = row_text.split('|')
        table_ok.append(row_text)
    return table_ok

def process_document(
    project_id: str,
    location: str,
    processor_id: str,
    processor_version: str,
    file_path: str,
    mime_type: str,
    process_options: Optional[documentai.ProcessOptions] = None,
) -> documentai.Document:
    client = documentai.DocumentProcessorServiceClient(
        client_options=ClientOptions(
            api_endpoint=f"{location}-documentai.googleapis.com"
        )
    )
    name = client.processor_version_path(
        project_id, location, processor_id, processor_version
    )

    with open(file_path, "rb") as image:
        image_content = image.read()

    request = documentai.ProcessRequest(
        name=name,
        raw_document=documentai.RawDocument(content=image_content, mime_type=mime_type),
        process_options=process_options,
    )

    result = client.process_document(request=request)

    return result.document


def layout_to_text(layout: documentai.Document.Page.Layout, text: str) -> str:
    """
    Document AI identifies text in different parts of the document by their
    offsets in the entirety of the document"s text. This function converts
    offsets to a string.
    """
    return "".join(
        text[int(segment.start_index) : int(segment.end_index)]
        for segment in layout.text_anchor.text_segments
    )

df = process_document_form_sample(project_id, location, processor_id, processor_version, file_path, mime_type)
bq = bigquery_loader.Loader()
bq.load_bigquery_df(df, "utadeo-418221.SECOP.detalle_contratos","WRITE_APPEND")