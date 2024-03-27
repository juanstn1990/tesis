"""
A__ext_selenium_hotjar.py

change history:

date            author                          observations
2023-01-10      juan.paez@Finkargo.com          Creation
2023-07-13      juan.paez@Finkargo.com          Drop country columns
"""

from shared.selenium import SeleniumObj
from selenium.webdriver.common.by import By
import time
import pandas as pd
import os
from os.path import exists


DOWNLOAD_FOLDER = os.environ["DOWNLOAD_FOLDER"]


def check_file(contains_string):
    while True:
        files_in_folder = os.listdir(DOWNLOAD_FOLDER)
        for filename in files_in_folder:
            if contains_string in filename and not filename.endswith('.crdownload'):
                return filename


def delete_file(contains_string):
    while True:
        files_in_folder = os.listdir(DOWNLOAD_FOLDER)
        for filename in files_in_folder:
            if contains_string in filename and not filename.endswith('.crdownload'):
                os.remove(DOWNLOAD_FOLDER + '/' + filename)
                return


def extract_contracts(webdriver, url, constancia):
    webdriver.driver.get(url)
    webdriver.driver.find_element(By.LINK_TEXT, 'Contrato').click()
    name = check_file(constancia)
    return name

def main(url, constancia):
    webdriver = SeleniumObj()
    document = extract_contracts(webdriver, url, constancia)
    print(document)

if __name__ == '__main__':
    main('https://www.contratos.gov.co/consultas/detalleProceso.do?numConstancia=21-12-12645260', '21-12-12645260')