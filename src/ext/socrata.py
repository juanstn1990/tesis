from src.shared.extractors import ext_socrata


def main():
    #extraction
    soc = ext_socrata.SocrataData()
    df = soc.extract_data()

if __name__ == "__main__":
    main()