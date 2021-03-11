from os import path

import airflow

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from dbnd import log_dataframe, log_metric, task
from dbnd._core.constants import DbndTargetOperationType


# Retreive Airflow Variables
AWS_CONN_ID = Variable.get("AWS_s3_conn_id")
DAG_ID = Variable.get("s3_key_monitor_DAG_id")
S3_KEY_MONITOR_SCHEDULE = Variable.get("s3_key_monitor_schedule")
try:
    TARGET_KEYS = Variable.get("s3_monitor_target_keys")
except:
    TARGET_KEYS = ""
try:
    TARGET_PREFIXES = Variable.get("s3_monitor_target_prefixes")
except:
    TARGET_PREFIXES = ""

MB = 1048576  # conversion factor from Byte to MB

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
    """parses S3 URIs, seperating out buckets and keys from URI"""
    buckets, keys = [], []
    for URI in URIs:
        uri_path = path.normpath(URI).split("/")
        buckets.append(uri_path[1])
        keys.append(uri_path[2:])

    return buckets, keys


def monitor_S3_key(**context):
    """
    S3 monitor will log metrics for the target key, collecting the following metrics:
    - size (MB)
    - context type (MIME type)
    - last modified timestamp
    - metadata associated with the key
    - parts count
    - storage class
    """
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)

    target_path = context["target_s3_path"]
    basename = context["path_basename"]
    log_metric("target file", target_path)

    boto3_key_object = s3_hook.get_key(key=target_path)

    key_metrics = {
        "{}-size(MB)".format(basename): (boto3_key_object.content_length / MB),
        "{}-content_type".format(basename): boto3_key_object.content_type,
        "{}-last_modified".format(basename): boto3_key_object.last_modified,
        "{}-metadata".format(basename): boto3_key_object.metadata,
        "{}-parts_count".format(basename): boto3_key_object.parts_count,
    }

    key_metrics["{}-storage_class".format(basename)] = (
        boto3_key_object.storage_class
        if boto3_key_object.storage_class
        else "s3 standard"
    )

    for metric_name, value in key_metrics.items():
        log_metric(metric_name, value)

    context["ti"].xcom_push("{}_key_metrics".format(basename), key_metrics)


def monitor_S3_prefix(**context):
    """
    S3 monitor will monitor for the target prefix(s), collecting the following metrics:
    - total size of prefix (MB)
    - mean key size of prefix (MB)
    - largest key with prefix (MB)
    """
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    target_prefix = "/".join(context["prefix"])
    log_metric("prefix", target_prefix)

    target_basename = context["prefix_basename"]
    log_metric("basename", target_basename)

    target_bucket = context["bucket"]
    log_metric("bucket", target_bucket)

    bucket = s3_hook.get_bucket(target_bucket)

    total_size, num_objs = 0, 0
    obj_sizes, last_modified = [], []

    # find largest size in loop instead of using max() for optimization on large prefixes
    largest_key_size = 0
    for obj in bucket.objects.filter(Prefix=target_prefix):
        total_size += obj.size
        num_objs += 1
        obj_sizes.append(obj.size)
        if obj.size >= largest_key_size:
            largest_key_size = obj.size
        last_modified.append(obj.last_modified)

    mean_key_size = (total_size / num_objs) / MB

    prefix_metrics = {
        "{}-total_size(MB)".format(target_basename): total_size / MB,
        "{}-largest_key_size(MB)".format(target_basename): largest_key_size / MB,
        "{}-mean_key_size(MB)".format(target_basename): mean_key_size / MB,
        "{}-object_count".format(target_basename): num_objs,
    }

    for metric_name, metric_value in prefix_metrics.items():
        log_metric(metric_name, metric_value)

    context["ti"].xcom_push("{}_prefix_metrics".format(target_basename), prefix_metrics)


def aggregate_and_compare_metrics(**context):
    """
    Aggregate and compare metrics. This task will log the following metrics:
    - largest key
    - largest key size
    - largest prefix size
    - total size of each prefix monitored
    """
    # use the basename to build the correct task_ids:
    key_basenames = context["key_basenames"]
    prefix_basenames = context["prefix_basenames"]
    key_monitor_task_ids = ["{}_monitor".format(basename) for basename in key_basenames]
    prefix_monitor_task_ids = [
        "{}_monitor".format(basename) for basename in prefix_basenames
    ]

    # make a list of metrics hash tables pulled from xcom:
    key_metrics_list = []
    for basename, task_id in zip(key_basenames, key_monitor_task_ids):
        key_metrics = context["ti"].xcom_pull(
            task_ids=task_id, key="{}_key_metrics".format(basename)
        )
        if key_metrics:
            key_metrics_list.append(key_metrics)

    # join the hash tables by targets, place into appropriate keys
    if key_metrics_list:
        log_metric("key_metrics", key_metrics_list)
        aggregated_key_metrics = {"targets": key_basenames}
        for metrics in key_metrics_list:
            for metric_name, metric_value in metrics.items():
                metric_name = metric_name.split("-")[-1]
                if metric_name in aggregated_key_metrics:
                    aggregated_key_metrics[metric_name].append(metric_value)
                else:
                    aggregated_key_metrics[metric_name] = [metric_value]

        # log largest file size metric
        largest_key_size = max(aggregated_key_metrics["size(MB)"])
        log_metric("largest_key_size(MB)", largest_key_size)
        largest_key = aggregated_key_metrics["targets"][
            aggregated_key_metrics["size(MB)"].index(largest_key_size)
        ]
        log_metric("largest_key", largest_key)

    # repreat the process for prefix monitor
    prefix_metrics_list = []
    log_metric("prefix task_ids", prefix_monitor_task_ids)
    log_metric("prefix basenames", prefix_basenames)
    for prefix_basename, task_id in zip(prefix_basenames, prefix_monitor_task_ids):
        prefix_metrics = context["ti"].xcom_pull(
            task_ids=task_id, key="{}_prefix_metrics".format(prefix_basename)
        )
        if prefix_metrics:
            prefix_metrics_list.append(prefix_metrics)

    if prefix_metrics_list:
        log_metric("prefix_metrics", prefix_metrics_list)
        aggregated_prefix_metrics = {"targets": prefix_basenames}
        for metrics in prefix_metrics_list:
            for metric_name, metric_value in metrics.items():
                metric_name = metric_name.split("-")[-1]
                if metric_name in aggregated_prefix_metrics:
                    aggregated_prefix_metrics[metric_name].append(metric_value)
                else:
                    aggregated_prefix_metrics[metric_name] = [metric_value]
        log_metric("aggregated prefix metrics", aggregated_prefix_metrics)

        largest_prefix_by_mem = max(aggregated_prefix_metrics["total_size(MB)"])
        largest_mem_prefix_name = aggregated_prefix_metrics["targets"][
            aggregated_prefix_metrics["total_size(MB)"].index(largest_prefix_by_mem)
        ]
        largest_prefix_by_obj_cnt = max(aggregated_prefix_metrics["object_count"])
        largest_obj_cnt_prefix_name = aggregated_prefix_metrics["targets"][
            aggregated_prefix_metrics["object_count"].index(largest_prefix_by_obj_cnt)
        ]
        log_metric("largest_prefix_name", largest_mem_prefix_name)
        log_metric("largest_prefix_size(MB)", largest_prefix_by_mem)
        log_metric("largest_prefix_by_obj_count", largest_prefix_by_obj_cnt)
        log_metric("largest_obj_cnt_prefix_name", largest_obj_cnt_prefix_name)


with dag as s3_bucket_template_dag:

    target_URIs = TARGET_KEYS.split(",")
    AirflowTasks = []
    if target_URIs:
        target_buckets, target_keys = parse_s3_uri(target_URIs)
        key_basenames = []

        for URI, key in zip(target_URIs, target_keys):
            basename = key[-1]
            key_basenames.append(basename)

            AirflowTasks.append(
                PythonOperator(
                    task_id="{}_monitor".format(basename),
                    python_callable=monitor_S3_key,
                    op_kwargs={"target_s3_path": URI, "path_basename": basename},
                )
            )

    target_prefix_paths = TARGET_PREFIXES.split(",")
    if target_prefix_paths:
        bucket_names, prefixes = parse_s3_uri(target_prefix_paths)
        prefix_basenames = []
        for bucket_name, prefix in zip(bucket_names, prefixes):
            basename = prefix[-1]
            prefix_basenames.append(basename)

            AirflowTasks.append(
                PythonOperator(
                    task_id="{}_monitor".format(basename),
                    python_callable=monitor_S3_prefix,
                    op_kwargs={
                        "prefix": prefix,
                        "prefix_basename": basename,
                        "bucket": bucket_name,
                    },
                )
            )

    compare_metrics_task = PythonOperator(
        task_id="aggregate_and_compare_metrics",
        python_callable=aggregate_and_compare_metrics,
        op_kwargs={
            "target_URIs": target_URIs,
            "key_basenames": key_basenames,
            "prefix_basenames": prefix_basenames,
        },
    )

    for task in AirflowTasks:
        task >> compare_metrics_task
