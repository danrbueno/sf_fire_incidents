- [Challenge](#challenge)
- [Description](#description)
- [Solution](#solution)
- [Assumptions](#assumptions)
- [ETL Process](#etl-process)
- [Setup AWS environment](#setup-aws-environment)
- [Final Report](#final-report)

# Challenge
Build a data pipeline to analize fire incidents data in San Francisco.

# Description
A customer needs to analyze a dataset of fire incidents in the city of San Francisco. 
To do so, it requests you to make the data available in a data warehouse and create a model to run dynamic queries efficiently

# Solution
For this challenge, the technologies used were:
- AWS Glue Jobs, for ETL;
- AWS S3, for storage;
- AWS Athena, for queries;
- AWS QuickSight for reports;

# Assumptions
- API: <code>https://data.sfgov.org/resource/wr8u-xric.json</code>
    - No authentication needed
- Date filter: '<code>incident_date >= --FROM_DATE</code>'
    - --FROM_DATE initially >= 2024-01, if you want to change it, just put another value in this job parameter.
    - Valid formats: 'yyyy-mm-dd', 'yyyy-mm' or 'yyyy'
- 'Time Period' is based on field 'incident_date'
- 'District' is based on field 'neighborhood_district'

# ETL Process
- Extraction:
    - Fectch the data from API and put into a staging pandas dataframe
- Transformations:
    - Fix data types, replace NaN values and clean nulls
- Load:
    - Load the transformed data in a AWS S3 bucket in parquet files

# Setup AWS environment
1) AWS S3
- Go to AWS Console -> S3
- Click "Create Bucket"
- Choose a unique name, for this project the name chosen was 'sf-fire-incidents-datalake' 
    - You have to choose another name, because it's unique

2) IAM Role (needed for Glue to save parquets in S3 and logs in CloudWatch):
This role grants Glue permissions to read/write to S3 and run ETL scripts.
- Go to AWS IAM -> Roles -> Create Role
- Choose AWS Glue as the trusted entity
- Attach permissions policy defined at <code>\config\sf-fire-incidents-policy.json</code>

3) Glue Jobs:
The job that process the ETL. Import the job script and JSON, by following the steps below:
- Go to AWS Glue -> ETL Jobs -> Create job -> Script editor
- Choose 'Spark' in 'Engine' field
- Choose 'Upload script'
- Choose file <code>\etl\sf-fire-incidents-glue-job.py</code>
- Go to tab 'Job details' -> Actions -> Upload
- Choose the file <code>\config\sf-fire-incidents-glue-job.json</code>
    - Remember to change the references of your bucket name, AWS account id and job name in this file.
        - Search for <code><account_id></code>, <code><bucket_name></code> and <code><glue_job_name></code> to make the changes.
- Go to tab 'Schedule' and create a scheduled execution with the frequency you need it.

4) Athena:
- Go to AWS Athena -> Query Editor and execute the DDL's below:
- Creating database 'sf_fire_incidents_db':
    - Copy the content of the file <code>database\create_database.sql</code> and execute;
- Creating table 'fire_incidents':
    - Copy the content of the file <code>database\create_table.sql</code> and execute;
- Updating partitions in table 'fire_incidents':
    - Copy the content of the file <code>database\update_partitions.sql</code> and execute;

# Final Report
[📑 Fire Incidents Report](https://github.com/danrbueno/sf_fire_incidents/blob/main/report/sf_fire_incidents_report.pdf)