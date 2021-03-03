import os

from io import StringIO
from os import path
from pathlib import Path

import airflow
import pandas as pd

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from dbnd import log_dataframe, log_metric, task
from dbnd._core.constants import DbndTargetOperationType


# retrive Airflow Variables 
AWS_CONN_ID = Variable.get("AWS_s3_conn_id")
DAG_ID = Variable.get("s3_key_monitor_DAG_id")
S3_KEY_MONITOR_SCHEDULE = Variable.get("s3_key_monitor_schedule")
TARGETS = Variable.get("s3_monitor_target_URIs")

DEFAULT_ARGS = {
    "owner": "databand",
    "start_date": airflow.utils.dates.days_ago(0),
    "provide_context": True,
}

dag = DAG(
    dag_id=DAG_ID,
    schedule_interval="{}".format(S3_KEY_MONITOR_SCHEDULE),
    default_args=DEFAULT_ARGS,
    tags=["s3", "dbnd_monitor"],
)


def parse_s3_uri(URIs):
    '''parses S3 URIs, seperating out buckets and keys from URI'''
    buckets, keys = [], []
    for URI in URIs:
        uri_path = path.normpath(URI).split(
            "/"
        )
        buckets.append(uri_path[1])
        keys.append(uri_path[2:])

    return buckets, keys


def monitor_S3_key(**context):
    '''
    S3 monitor will log metrics for the target key, collecting the following metrics: 
    - size (MB)
    - context type (MIME type)
    - last modified timestamp
    - metadata associated with the key
    - parts count 
    - storage class 
    '''
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    
    # target_URIs = Variable.get("s3_monitor_target_URIs").split(',')
    target_path = context["target_s3_path"]
    basename = context["path_basename"]
    # full_path = os.path.join("s3://", target_bucket, target_key)
    log_metric("target file", target_path)
    boto3_key_object = s3_hook.get_key(key=target_path)

    key_metrics = {
        "{}-size(MB)".format(basename): (boto3_key_object.content_length / 1048576),
        "{}-content_type".format(basename): boto3_key_object.content_type,
        "{}-last_modified".format(basename): boto3_key_object.last_modified,
        "{}-metadata".format(basename): boto3_key_object.metadata,
        "{}-parts count".format(basename): boto3_key_object.parts_count,
    }

    if boto3_key_object.storage_class:
        key_metrics[
            "{}-storage_class".format(basename)
        ] = boto3_key_object.storage_class
    else:
        key_metrics["{}-storage_class".format(basename)] = "standard"

    for metric_name, value in key_metrics.items():
        log_metric(metric_name, value)

    context["ti"].xcom_push("{}_key_metrics".format(basename), key_metrics)


def aggregate_and_compare_metrics(**context):
    '''
    Aggregate and compare metrics. This task will log the following metrics:
    - largest key 
    - largest key size
    '''
    # use the basename to build the correct task_ids:
    basenames = context['key_basenames']
    task_ids = ["{}_monitor".format(basename) for basename in basenames]

    # make a list of metrics hash tables pulled from xcom:
    metrics_list = []
    for basename, task_id in zip(basenames, task_ids):
        metrics_list.append(context["ti"].xcom_pull(
            task_ids=task_id, key="{}_key_metrics".format(basename)
        ))
    log_metric("key_metrics",metrics_list)

    # join the hash tables by targets, place into 
    aggregated_metrics = {'targets' : basenames}
    for metrics in metrics_list:
        for metric_name, metric_value in metrics.items():
            metric_name = metric_name.split('-')[-1]
            if metric_name in aggregated_metrics:
                aggregated_metrics[metric_name].append(metric_value)
            else:
                aggregated_metrics[metric_name] = [metric_value] 

    # log largest file size metric
    largest_key_size = max(aggregated_metrics['size(MB)'])
    log_metric("largest_key_size(MB)", largest_key_size)
    largest_key = aggregated_metrics['targets'][aggregated_metrics['size(MB)'].index(largest_key_size)]
    log_metric("largest_key", largest_key)

   

with dag as s3_bucket_template_dag:

    target_URIs = TARGETS.split(",")
    target_buckets, target_keys = parse_s3_uri(target_URIs)
    AirflowTasks, basenames = [], []

    for URI, key in zip(target_URIs, target_keys):
        basename = key[-1]
        basenames.append(basename)

        AirflowTasks.append(
            PythonOperator(
                task_id="{}_monitor".format(basename),
                python_callable=monitor_S3_key,
                op_kwargs={
                    "target_s3_path": URI,
                    "path_basename": basename
                },
            )
        )

    compare_metrics_task = PythonOperator(
        task_id="aggregate_and_compare_metrics",
        python_callable=aggregate_and_compare_metrics,
        op_kwargs={"target_URIs": target_URIs, "key_basenames": basenames},
    )

    for task in AirflowTasks:
        task >> compare_metrics_task
