from src.shared.extractors import ext_socrata
from src.shared.loaders import bigquery_loader



def main():
    #extraction
    soc = ext_socrata.SocrataData()
    df = soc.extract_data()
    
    #loading
    bq = bigquery_loader.Loader()
    bq.load_bigquery_df(df, "utadeo-418221.SECOP.secop")

if __name__ == "__main__":
    main()