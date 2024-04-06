from shared.extractors.api import ext_socrata
from src.shared.loaders import bigquery_loader



def main():
    #extraction
    soc = ext_socrata.SocrataData()
    df = soc.extract_data()
    
    #loading
    bq = bigquery_loader.Loader()
    bq.load_bigquery_df(df, "artful-sled-419501.secop.secop","WRITE_TRUNCATE")

if __name__ == "__main__":
    main()