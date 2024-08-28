from shared.extractors.api import ext_socrata
from shared.loaders import bigquery_loader
from datetime import datetime
from shared.utils import create_trimesters



def main():
    loader = bigquery_loader.Loader()
    #extraction
    for week in create_trimesters("2020-11-30", "2024-08-19"):
        initial_date = week["trimester_start"]
        last_date = week["trimester_end"]
        print(initial_date, last_date)
        soc = ext_socrata.SocrataData()
        df = soc.extract_data(initial_date, last_date)
        loader.load_bigquery_df(df, "artful-sled-419501.secop.secop_2", "WRITE_APPEND")
if __name__ == "__main__":
    main()