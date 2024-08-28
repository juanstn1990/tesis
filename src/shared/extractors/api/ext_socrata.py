import requests
import pandas as pd
import os

class SocrataData:
    def __init__(self):
        self.api_key = os.getenv("TOKEN_SOCRATA")
        self.limit = 50000
        self.offset = 0
        

    def extract_data(self, first_date,last_date):
        #self.url=f"https://www.datos.gov.co/resource/f789-7hwg.json?$where=fecha_de_cargue_en_el_secop between '{first_date}' and '{last_date}'"
        self.url=f"https://www.datos.gov.co/resource/p6dx-8zbt.json?$where=fecha_de_publicacion_del between '{first_date}' and '{last_date}'"
        #self.url=f"https://www.datos.gov.co/resource/qmzu-gj57.json?$where=fecha_creacion between '{first_date}' and '{last_date}'"
        all_data = pd.DataFrame()
        while True:
            params = {
                "$limit": self.limit,
                "$offset": self.offset,
                "$$app_token": self.api_key
            }
            response = requests.get(self.url, params=params)
            print(response.status_code)
            if response.status_code == 200:
                data = response.json()
                df = pd.DataFrame(data)
                all_data = pd.concat([all_data, df], ignore_index=True)
                if len(df) < self.limit:
                    break
                self.offset += self.limit
            else:
                print("Error:", response.status_code)
                break
        return all_data