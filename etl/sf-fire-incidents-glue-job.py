import pandas as pd
import boto3
import io
import logging
from sodapy import Socrata
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StringType
from awsglue.utils import getResolvedOptions
import sys

# Configure logging
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)

def get_job_params():
    """Retrieve job parameters from AWS Glue."""
    args = getResolvedOptions(sys.argv, ["S3_BUCKET", "PROCESSED_DIR", "API_END_POINT", "PAGE_SIZE", "FROM_DATE", "API_DATASET", "COALESCE"])
    return {
        "S3_BUCKET": args["S3_BUCKET"],
        "PROCESSED_DIR": args["PROCESSED_DIR"],
        "API_END_POINT": args["API_END_POINT"],
        "PAGE_SIZE": int(args["PAGE_SIZE"]),
        "FROM_DATE": args["FROM_DATE"],
        "API_DATASET": args["API_DATASET"],
        "COALESCE": int(args["COALESCE"])
    }

def initialize_spark():
    """Initialize Spark session."""
    logging.info("Initializing Spark session")
    return SparkSession.builder.appName("FireIncidentsETL").getOrCreate()

def extract_data(api_endpoint, api_dataset, page_size, from_date):
    """Fetch fire incident data from API using pagination and date filter."""
    client = Socrata(api_endpoint, None)
    offset = 0
    all_data = []

    # while offset == 0:  # Just page 1
    while True: # Full pagination
        where = f"incident_date >= '{from_date}'"
        data = client.get(api_dataset, limit=page_size, offset=offset, where=where)

        if not data:
            break
        
        all_data.extend(data)
        offset += page_size

        logging.info(f"Fetched {len(data)} records, total so far: {len(all_data)}")

    return pd.DataFrame.from_records(all_data)

def transform_data(df_pandas):
    """Transform data: Fix data types, replace NaN values, drop duplicates, and clean nulls."""
    
    # Convert all columns to string type and replace 'nan' with empty strings
    df_pandas = df_pandas.astype(str).replace("nan", "")
    
    logging.info(f"Converted API data to Pandas DataFrame with {len(df_pandas)} records")

    # Convert Pandas DataFrame to Spark DataFrame
    spark = initialize_spark()
    df = spark.createDataFrame(df_pandas)

    # Ensure all columns remain as strings in PySpark
    for column in df.columns:
        df = df.withColumn(column, col(column).cast(StringType()))

    logging.info("Resolved type inconsistencies dynamically")

    # Drop missing values & deduplicate
    df_transformed = df.dropna().distinct()

    logging.info(f"Processed {df_transformed.count()} unique records after cleaning")

    return df_transformed

def load_data(df_transformed, s3_bucket, processed_dir, coalesce_factor):
    """Save processed data to S3 in partitioned Parquet format."""
    processed_path = f"s3://{s3_bucket}/{processed_dir}/"
    
    df_transformed.coalesce(coalesce_factor).write.mode("overwrite").partitionBy("battalion").parquet(processed_path)

    logging.info("Fire incidents extracted, transformed, and stored in partitioned Parquet format")

def main():
    """Main function to orchestrate ETL process."""
    logging.info("AWS Glue ETL Job Started")

    # Retrieve job parameters
    params = get_job_params()

    # Extract Data
    df_pandas = extract_data(params["API_END_POINT"], params["API_DATASET"], params["PAGE_SIZE"], params["FROM_DATE"])

    # Transform Data
    df_transformed = transform_data(df_pandas)

    # Load Data
    load_data(df_transformed, params["S3_BUCKET"], params["PROCESSED_DIR"], params["COALESCE"])

    logging.info("AWS Glue ETL Job Completed Successfully")

# Run the ETL Job
if __name__ == "__main__":
    main()