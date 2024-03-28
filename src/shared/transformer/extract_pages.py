import pytesseract
from PIL import Image
from pdf2image import convert_from_path
from PyPDF2 import PdfWriter, PdfReader
import os

DOWNLOAD_FOLDER = os.environ["DOWNLOAD_FOLDER"]

def ocr_pdf_con_tesseract(pdf_path, medicamentos):
    paginas = convert_from_path(pdf_path)
    pagina_number = 1
    pages_list = []
    for pagina in paginas:
        texto = pytesseract.image_to_string(pagina)
        texto = texto.upper()
        if any(medicamento in texto for medicamento in medicamentos):
            pages_list.append(pagina_number)
        pagina_number += 1
    return pages_list

def select_pages_pdf(filename, first_page, last_page):
    first_page = first_page - 1
    last_page = last_page - 1   
    infile = PdfReader(os.path.join(DOWNLOAD_FOLDER, filename))
    output = PdfWriter()
    for i in range(first_page, last_page + 1):
        p = infile.pages[int(i)]
        output.add_page(p)
    with open(os.path.join(DOWNLOAD_FOLDER, filename), "wb") as f:
        output.write(f)

def main():
    medicametos = ['ACETAMINOFEN', 'ACICLOVIR', 'ALBENDAZOL', 'AMOXICILINA', 'AMPICILINA', 'AZITROMICINA', 'BENZATINA', 'BENZOATO', 'BENZOIL', 'BENZOILO', 'TRAZODONA', 'ZIDOVUDINA']
    ruta_imagen = "/tmp/C_PROCESO_19-4-9750833_22587527_61531123.pdf"
    pages_list = ocr_pdf_con_tesseract(ruta_imagen, medicametos)
    print(pages_list)
    select_pages_pdf('C_PROCESO_19-4-9750833_22587527_61531123.pdf', min(pages_list), max(pages_list))

if __name__ == "__main__":
    main()