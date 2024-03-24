import requests
import pandas as pd
import os

key = os.getenv("TOKEN_SOCRATA")
print("API Key:", key)

url = "https://www.datos.gov.co/resource/rpmr-utcd.json?municipio_entidad=CHIA"
limit = 1000
offset = 0
all_data = pd.DataFrame()
while True:
    params = {
        "$limit": limit,
        "$offset": offset,
        "$$app_token": key
    }
    response = requests.get(url, params=params)

    if response.status_code == 200:
        data = response.json()
        df = pd.DataFrame(data)
        all_data = pd.concat([all_data, df], ignore_index=True)
        if len(df) < limit:
            break
        offset += limit
    else:
        print("Error:", response.status_code)
        break
print(all_data) 