from shared.extractors.api import ext_socrata
from src.shared.loaders import bigquery_loader
from datetime import datetime


def main():
    #extraction
    soc = ext_socrata.SocrataData()


    soc.extract_data()

if __name__ == "__main__":
    main()