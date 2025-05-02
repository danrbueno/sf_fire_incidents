CREATE EXTERNAL TABLE fire_incidents (
    id STRING,
    incident_date TIMESTAMP,
    district STRING,
    battalion STRING
)
PARTITIONED BY (battalion STRING)
STORED AS PARQUET
LOCATION 's3://<bucket_name>/processed/';