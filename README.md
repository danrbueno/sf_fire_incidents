# Challenge
Build a data pipeline to analize fire incidents data in San Francisco.

# Description
A customer needs to analyze a dataset of fire incidents in the city of San Francisco. 
To do so, it requests you to make the data available in a data warehouse and create a model to run dynamic queries efficiently

# Solution
For this challenge, I use:
- AWS Glue Jobs, for ETL;
- AWS S3, for storage;
- AWS Athena, for queries;

# Setup AWS environment
1) AWS S3 Setup (Store raw and processed fire incident data)
- Go to AWS Console -> S3
- Click "Create Bucket"
- Name it 'sf-fire-incidents-datalake' 
    ## Please note that this name is unique, so if you want to create your own bucket, you have to choose another name.

2) Glue Service Role:
This role grants Glue permissions to read/write to S3 and run ETL scripts.
- Go to AWS IAM -> Roles -> Create Role
- Choose AWS Glue as the trusted entity
- Attach permissions policy defined at <code>/infra/sf-fire-incidents-policy.json</code>
    ## Remember to change the references of your bucket name and your AWS account id in this file

3) Glue Jobs:
The job that process the ETL. Import the job script and JSON, by following the steps below:
- Go to AWS Glue -> ETL Jobs -> Create job -> Script editor
- Choose 'Spark' in 'Engine' field
- Choose 'Upload script'
- Choose file <code>etl\sf-fire-incidents-glue-job.py</code>
- Go to Job details -> Actions -> Upload
- Choose the file <code>infra\sf-fire-incidents-glue-job.json</code>