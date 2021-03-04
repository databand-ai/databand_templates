from os import path
from io import StringIO
import airflow
import pandas as pd
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from dbnd import log_dataframe, log_metric

# Retreive Airflow Variables
AWS_CONN_ID = Variable.get("AWS_s3_conn_id")
DAG_ID = Variable.get("s3_bucket_monitor_DAG_id")
S3_BUCKET_MONITOR_SCHEDULE = Variable.get("s3_bucket_monitor_schedule")
TARGETS = Variable.get("s3_monitor_target_buckets")

DEFAULT_ARGS = {
    "owner": "databand",
    "start_date": airflow.utils.dates.days_ago(0),
    "provide_context": True,
}

dag = DAG(
    dag_id=DAG_ID,
    schedule_interval="{}".format(S3_BUCKET_MONITOR_SCHEDULE),
    default_args=DEFAULT_ARGS,
    tags=["s3", "dbnd_monitor"],
)

def parse_s3_uri(URIs):
    '''parses S3 URIs, seperating out bucket names'''
    buckets = []
    for URI in URIs:
        uri_path = path.normpath(URI).split(
            "/"
        )
        buckets.append(uri_path[-1])

    return buckets

def monitor_S3_bucket(**context):
    '''
    This S3 monitor takes a niave approach and is not suitable for large buckets
    S3 monitor will log metrics for the target key, collecting the following metrics: 
    - Total bucket size (GB)
    - Largest key name 
    - Largest key (MB)
    - Pandas DataFrame with the following metrics on each object inside bucket:
        - key
        - size (MB)
        - last modified timestamp
    '''
    MB = 1048576 
    GB = 1073741824
    bucket_name = context['bucket_name']

    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    bucket = s3_hook.get_bucket(bucket_name)

    bucket_info = {
        "{}-key".format(bucket_name): [], 
        "{}-size(MB)".format(bucket_name): [], 
        "{}-last_modified".format(bucket_name): []
    }

    bucket_size = 0
    for s3_object in bucket.objects.all():
        bucket_size += s3_object.size
        bucket_info["{}-key".format(bucket_name)].append(s3_object.key)
        bucket_info["{}-size(MB)".format(bucket_name)].append(s3_object.size/MB)
        bucket_info["{}-last_modified".format(bucket_name)].append(s3_object.last_modified)

    bucket_info_df = pd.DataFrame(bucket_info)

    num_objects = bucket_info_df.shape[0]
    largest_key_size = max(bucket_info["{}-size(MB)".format(bucket_name)])
    largest_key_size_idx = bucket_info["{}-size(MB)".format(bucket_name)].index(largest_key_size)
    largest_key_name = bucket_info["{}-key".format(bucket_name)][largest_key_size_idx]
    
    log_metric("{}-largest_key_size_(MB)".format(bucket_name), largest_key_size)
    log_metric("{}-largest_key_name".format(bucket_name), largest_key_name)
    log_dataframe(
        "{}-full_bucket_information".format(bucket_name), 
        bucket_info_df,
        with_histograms=True,
        with_stats=True,
        with_schema=True,
        path="s3://{}".format(bucket_name),

    )
    log_metric("{}-total_bucket_size(GB)".format(bucket_name), bucket_size/GB)
    log_metric("{}-number_of_objects".format(bucket_name), num_objects)

    key_metrics = {
        "{}-largest_key_size(MB)".format(bucket_name): largest_key_size,
        "{}-largest_key_name".format(bucket_name): largest_key_name, 
        "{}-total_bucket_size".format(bucket_name): bucket_size/GB,
        "{}-number_of_objects".format(bucket_name): num_objects
    }
    return key_metrics

def compare_metrics(**context):
    '''
    compare metrics passed through xcom. This task will log the following metrics
    - largest bucket by number of objects
    - largest bucket by memory size
    '''
    bucket_names = context['bucket_names']
    task_ids = ["{}_monitor".format(bucket_name) for bucket_name in bucket_names]
    log_metric("task_ids", task_ids)

    # extract the metric dictionaries from xcom and join them by bucket name
    aggregated_metrics = {"buckets": bucket_names}
    for task_id in task_ids:
        task_metric = context["ti"].xcom_pull(task_ids=task_id)
        for task_metric_name, task_metric_value in task_metric.items():
            metric_name = task_metric_name.split('-')[-1]
            if metric_name in aggregated_metrics:
                aggregated_metrics[metric_name].append(task_metric_value)
            else:
                aggregated_metrics[metric_name] = [task_metric_value]

    # log interesting metrics
    largest_num_objs= max(aggregated_metrics['number_of_objects'])
    largest_bucket_by_obj = aggregated_metrics['buckets'][aggregated_metrics['number_of_objects'].index(largest_num_objs)]
    log_metric("largest_obj_count", largest_num_objs)
    log_metric("largest_bucket_by_obj_count", largest_bucket_by_obj)

    largest_bucket_mem = max(aggregated_metrics['total_bucket_size'])
    largest_bucket_by_mem = aggregated_metrics['buckets'][aggregated_metrics['total_bucket_size'].index(largest_bucket_mem)]
    log_metric("largest_bucket_by_memory", largest_bucket_by_mem)
    log_metric("largest_memory_consumed", largest_bucket_mem)
    

with dag as s3_bucket_template_dag:

    target_buckets = parse_s3_uri(TARGETS.split(','))
    AirflowTasks, bucketnames = [], []

    for target_bucket in target_buckets:
        bucketnames.append(target_bucket)
        AirflowTasks.append(
            PythonOperator(
                task_id="{}_monitor".format(target_bucket),
                python_callable=monitor_S3_bucket,
                op_kwargs={"bucket_name" : target_bucket}
            )
        )
    
    compare_metrics_task = PythonOperator(
        task_id="compare_metrics",
        python_callable=compare_metrics,
        op_kwargs={"bucket_names" : bucketnames}

    )

    for task in AirflowTasks:
        task >> compare_metrics_task
