import requests
import pandas as pd
import os

class SocrataData:
    def __init__(self):
        self.api_key = os.getenv("TOKEN_SOCRATA")
        self.url = "https://www.datos.gov.co/resource/rpmr-utcd.json?municipio_entidad=CHISCAS"
        self.limit = 1000
        self.offset = 0
        self.all_data = pd.DataFrame()

    def extract_data(self):
        while True:
            params = {
                "$limit": self.limit,
                "$offset": self.offset,
                "$$app_token": self.api_key
            }
            response = requests.get(self.url, params=params)

            if response.status_code == 200:
                data = response.json()
                df = pd.DataFrame(data)
                all_data = pd.concat([self.all_data, df], ignore_index=True)
                if len(df) < self.limit:
                    break
                self.offset += self.limit
            else:
                print("Error:", response.status_code)
                break

        return all_data
