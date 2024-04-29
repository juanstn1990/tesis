from shared.extractors.api import ext_socrata
from src.shared.loaders import bigquery_loader
from datetime import datetime
from src.shared.utils import create_trimesters



def main():
    loader = bigquery_loader.Loader()
    #extraction
    for week in create_trimesters("2022-02-23", "2024-04-28"):
        initial_date = week["trimester_start"]
        last_date = week["trimester_end"]
        print(initial_date, last_date)
        soc = ext_socrata.SocrataData()
        df = soc.extract_data(initial_date, last_date)
        loader.load_bigquery_df(df, "artful-sled-419501.secop.secop", "WRITE_APPEND")
if __name__ == "__main__":
    main()