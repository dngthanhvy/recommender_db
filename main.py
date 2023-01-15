from ETL import ETL

if __name__ == "__main__":
    # START
    print("PROCESS STARTED")

    # Extract data from source
    extracted_data = ETL.extract('./data/dataset_cleaned.parquet')

    # Transform data
    transformed_data = ETL.transform(extracted_data)

    # Load data
    ETL.load(transformed_data)

    # END
    print("PROCESS ENDED")
