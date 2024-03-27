import pytesseract
from PIL import Image
from pdf2image import convert_from_path


def ocr_pdf_con_tesseract(pdf_path, medicamentos):
    paginas = convert_from_path(pdf_path)
    texto_extraido = ''
    pagina_number = 1
    for pagina in paginas:
        texto = pytesseract.image_to_string(pagina)
        texto = texto.upper()
        if any(medicamento in texto for medicamento in medicamentos):
            print(f"Encontrado en la página {pagina_number}")
        pagina_number += 1
    return texto_extraido


medicametos = ['ACETAMINOFEN', 'ACICLOVIR', 'ALBENDAZOL', 'AMOXICILINA', 'AMPICILINA', 'AZITROMICINA', 'BENZATINA', 'BENZOATO', 'BENZOIL', 'BENZOILO', 'TRAZADONA']

ruta_imagen = "/tmp/C_PROCESO_21-12-12645260_276001622_98573431.pdf"
texto_extraido = ocr_pdf_con_tesseract(ruta_imagen, medicametos)
print("Texto extraído:")
print(texto_extraido)
