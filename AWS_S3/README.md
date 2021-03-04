![Databand & AWS  S3 Logo](https://raw.githubusercontent.com/kalebinn/dbnd_doc_resources/main/s3%2Bdbnd.png)
# AWS S3 Monitoring Templates
- [Overview](#overview)
- [Scope of Observibility](#scope_of_observibility)
- [Requirements](#requirements)
- [Set up Instructions](#setup-instructions)
    - [Defining the AWS Airflow Connection](#airflow-connections)
    - [Defining the required Airflow Variables](#airflow-variables)
    - [Starting the DAG](#dag-start)



## [Overview](#overview) 
Our AWS S3 Monitoring Templates provide out of the box observibility into your S3 buckets and keys without any modification to existing data pipelines. They are written as standalone Airflow DAGs but can be easily extracted to be additional tasks in other DAGs, if desired. 

## [Scope of Observibility](#scope_of_observibility)
These are the metrics that will be collected out of the box. Additional metrics can be added with minimal changes.
 
- [S3 Bucket Monitor](./s3_bucket_monitor.py)<sup>*</sup>
    - Total bucket size (GB)
    - Number of objects in bucket 
    - Pandas Dataframe containing: 
        - Names of all keys in target bucket(s)
        - Size(MB) of all keys 
        - Last modified timestamp of all keys 
    - If multiple buckets were provided:
        - Largest bucket by object count 
        - Largest bucket by memory size 
- [S3 Key Monitor](./s3_key_monitor.py)
    - size (MB)
    - context type (MIME type)
    - last modified timestamp
    - metadata associated with the key
    - parts count 
    - storage class 

<sup>*</sup> The `S3 Bucket Monitor`, by default, takes a naive approach to the metrics collection process. This monitor is not recommended for very large buckets. 

## [Requirements](#requirements)
Your environment should be running Python 3.6+. 
- [S3 Bucket Monitor](./s3_bucket_monitor.py)
    - `dbnd`
    - `dbnd-airflow`
    - `dbnd-airflow-autotracking`
    - `apache-airflow`
    - `pandas` 
    - `boto3` 
- [S3 Key Monitor](./s3_key_monitor.py)
    - `dbnd`
    - `dbnd-airflow`
    - `dbnd-airflow-autotracking`
    - `apache-airflow`
    - `boto3`

## [Set up Instructions](#setup-instructions)
After installing the requirements, the monitoring templates can be set up in three short steps. 

### [1. Defining the AWS Airflow Connection](#airflow-connections)
First, define an Airflow connection to AWS. This can be [done through the CLI of your Airflow environment](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html), or through Airflow's web UI.

Note that the AWS connection you define must have access to the buckets/keys you are monitoring. 

### [2. Defining the required Airflow Variables](#airflow-variables)
After defining the connection, you must define several Airflow variables. These can be defined in your code using `airflow.models.Variable`, through your Airflow environment's CLI, or through Airflow's web UI.

The required variables are different for each monitor. 

Variables for [S3 Bucket Monitor](./s3_bucket_monitor.py)
- `s3_monitor_target_buckets`
    - URI(s) of bucket or buckets for monitor to track. You can enter multiple buckets to monitor by seperating each target with a comma 
    - The URI(s) should be of the format: 
        - Single bucket: `s3://<your bucket name>`
        - Multiple buckets: `s3://<your first bucket>,s3://<your second bucket>`
- `s3_bucket_monitor_schedule`
    - cron or airflow format schedule for monitor to run
- `AWS_s3_conn_id` 
    - AWS connection ID defined in the previous step 
- `s3_bucket_monitor_DAG_id`
    - `DAG ID` for the monitor 

Variables for [S3 Key Monitor](./s3_key_monitor.py)
- `s3_monitor_target_URIs` 
    - URI(s) of key or keys to for monitor to track. Multiple keys can be defined with comma separated values. The keys can be contained in different buckets.
    - The URI(s) should be of the format: 
        - Single key: `s3://<bucket name>/<path>/<to>/<key>`
        - Multiple keys: `s3://<bucket1>/<path>/<to>/<key1>,s3://<bucket2>/<path>/<to>/<key2>`
- `s3_key_monitor_schedule`
    - cron or airflow format schedule for monitor to run
- `AWS_s3_conn_id` 
    - AWS connection ID defined in the previous step 
- `s3_key_monitor_DAG_id`
    - `DAG ID` for the monitor 
    
### [3. Starting the DAG](#dag-start)
The final step is to move the monitor into the `dag` directory of your Airflow environment, and enable the monitoring DAG. 

---
The monitoring template most useful when paired with Databand's monitoring system. Metrics will be automatically collected and send to Databand's monitoring system where alerts, visualizations, data previews, historical trends, and more can be monitored. Visit [https://databand.ai/](https://databand.ai/) to learn more! If Databand is not available, metrics will be stored in Airflow logs. 



