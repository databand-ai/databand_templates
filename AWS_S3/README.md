![Databand & AWS  S3 Logo](https://raw.githubusercontent.com/kalebinn/dbnd_doc_resources/main/s3%2Bdbnd.png)
# AWS S3 Tracking Templates
- [Overview](#overview)
- [Scope of Observability](#scope_of_observability)
- [Requirements](#requirements)
- [Set up Instructions](#setup-instructions)
    - [Defining the AWS Airflow Connection](#airflow-connections)
    - [Defining the required Airflow Variables](#airflow-variables)
    - [Starting the DAG](#dag-start)



## [Overview](#overview) 
Our AWS S3 Tracking Templates provide out of the box observability into your S3 buckets and keys. The templates provide visibility into your data lake without any modification to existing data pipelines. They are written as standalone Airflow DAGs but can also be easily extracted to run as additional tasks in other DAGs, if desired, or as a jobs run by another scheduler. 

## [Scope of Observability](#scope_of_observability)
These are the metrics that will be collected out of the box. Users can additional metrics with minimal changes.
 
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
    - Keys:
        - size (MB)
        - content type (MIME type)
        - last modified timestamp
        - metadata associated with the key
        - parts count 
        - storage class 
    - Prefixes:
        - total size (MB)
        - last modified timestamp for all keys
        - size (MB) for all keys
        - largest key with prefix 
        - largest prefix by object count
        - largest prefix by total size

<sup>*</sup> The `S3 Bucket Monitor`, by default, takes a naive approach to the metrics collection process. This monitor is not recommended for very large buckets. 

## [Requirements](#requirements)
Your Airflow environment should be running Python 3.6+. 
- [S3 Bucket Monitor](./s3_bucket_monitor.py)
    - `dbnd`
    - `dbnd-airflow-autotracking`
    - `pandas`

- [S3 Key Monitor](./s3_key_monitor.py)
    - `dbnd`
    - `dbnd-airflow-autotracking`

## [Set up Instructions](#setup-instructions)
After installing the requirements, the monitoring templates can be set up in three short steps. 

### [1. Defining the AWS Airflow Connection](#airflow-connections)
First, define an Airflow connection to AWS. This can be [done through the CLI of your Airflow environment](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html), or through Airflow's web UI.

Note that the AWS connection you define must have access to the buckets/keys you are monitoring. 

### [2. Defining the required Airflow Variables](#airflow-variables)
After defining the connection, you must define several Airflow variables. These can be defined in your code using `airflow.models.Variable`, through your Airflow environment's CLI, or through Airflow's web UI.

The required variables are different for each monitor. 

#### Variables for [S3 Bucket Monitor](./s3_bucket_monitor.py)
| Variable Name | Description | Example |
|---------------|-------------|---------|
|`s3_monitor_target_buckets`|URI(s) of bucket or buckets designated for monitoring. You can enter multiple buckets to monitor by seperating each target with a comma.| `s3_monitor_target_buckets`: `s3://<your first bucket>,s3://<your second bucket>`|
|`s3_bucket_monitor_schedule` | cron or airflow format schedule for monitor to run | `s3_bucket_monitor_schedule` : `0 0 * * *`|
|`AWS_s3_conn_id` | AWS connection ID defined in the previous step | `AWS_s3_conn_id` : `aws_default`|
|`s3_bucket_monitor_DAG_id`| `DAG ID` for the monitor | `s3_bucket_monitor_DAG_id`: `sample_bucket_monitor`|

#### Variables for [S3 Key Monitor](./s3_key_monitor.py)
| Variable Name | Description | Example | 
|---------------|-------------|---------|
|`s3_key_monitor_schedule` | cron or airflow format schedule for monitor to run | `s3_key_monitor_schedule` : `@daily`|
|`AWS_s3_conn_id` | AWS connection ID defined in the previous step | `AWS_s3_conn_id` : `aws_default`|
|`s3_key_monitor_DAG_id`| `DAG ID` for the monitor | `s3_key_monitor_DAG_id`: `sample_key_monitor`|
|***optional variables***|-|-|
|`s3_monitor_target_keys`| URI(s) of key or keys designated for tracking. Multiple keys can be defined with comma separated values. The keys can be contained in different buckets. |`s3_monitor_target_keys`: `s3://<bucket1>/<path>/<to>/<key1>,s3://<bucket2>/<key2>` |
|`s3_monitor_target_prefixes`| URI(s) of prefix or prefixes designated for tracking. Multiple prefixes can be defined with comma separated values. The prefixes can be in different buckets. | `s3_monitor_target_prefixes`: `s3://<bucket1>/<prefix>,s3://<bucket2>/<path>/<to>/<prefix>` |

### [3. Starting the DAG](#dag-start)
The final step is to move the monitor into the `dag` directory of your Airflow environment, and enable the monitoring DAG. 

---
The monitoring template most useful when paired with Databand's monitoring system. Metrics will be automatically collected and send to Databand's monitoring system where alerts, visualizations, data previews, historical trends, and more can be monitored. Visit [https://databand.ai/](https://databand.ai/) to learn more! If Databand is not available, metrics will be stored in Airflow logs. 



