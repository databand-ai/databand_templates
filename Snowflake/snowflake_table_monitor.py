import os
import airflow
import snowflake.connector
from airflow.models import Variable
from airflow import DAG
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.operators.python_operator import PythonOperator
from dbnd import log_dataframe, log_metric, task
from dbnd._core.constants import DbndTargetOperationType
from dbnd_snowflake import (
    log_snowflake_resource_usage,
    log_snowflake_table,
    snowflake_query_tracker,
)
import json
from numpy import issubdtype, mean, median, number
import pandas as pd

SNOWFLAKE_TABLE_MONITOR_DAG_ID = Variable.get("snowflake_table_monitor_DAG_id")
SNOWFLAKE_CONNECTION = Variable.get("snowflake_connection")
TARGET_TABLES = Variable.get("snowflake_table_monitor_target_tables")

try: 
    SNOWFLAKE_TABLE_MONITOR_SCHEDULE = Variable.get("snowflake_table_monitor_schedule")
    sampling_enabled = Variable.get("enable_snowflake_table_sample")
    if sampling_enabled.lower() == 'true':
        ENABLE_SNOWFLAKE_TABLE_SAMPLE = True
        SNOWFLAKE_TABLE_SAMPLE_ROW_PROB = float(Variable.get("snowflake_table_sample_row_prob"))
    else:
        ENABLE_SNOWFLAKE_TABLE_SAMPLE = False
except:
    SNOWFLAKE_TABLE_MONITOR_SCHEDULE = "@daily"
    ENABLE_SNOWFLAKE_TABLE_SAMPLE = True
    SNOWFLAKE_TABLE_SAMPLE_ROW_PROB = float(1.0)

DEFAULT_ARGS = {
    "owner": "databand",
    "start_date": airflow.utils.dates.days_ago(0),
    "provide_context": True,
}

dag = DAG(
    dag_id="{}".format(SNOWFLAKE_TABLE_MONITOR_DAG_ID),
    schedule_interval="{}".format(SNOWFLAKE_TABLE_MONITOR_SCHEDULE),
    default_args=DEFAULT_ARGS,
    tags=["dbnd_monitor", "snowflake"],
) 

## Queries 
def get_random_sample(hook, database, schema, table, prob):
    random_sample_query = "select * from {}.{}.{} sample block ({});".format(
        database, schema, table, float(prob)
    )
    return hook.get_pandas_df(random_sample_query)
    

def get_record_count(hook, database, table):
    """ gets the row count for a specific table """
    query = '''SELECT t.row_count as record_count
    FROM "{}".information_schema.tables t
    WHERE t.table_name = '{}';'''.format(database, table)
    return hook.get_records(query).pop()[0]

def get_column_info(hook, database, schema, table):
    query = '''show columns in table "{}"."{}"."{}"'''.format(database, schema, table)
    col_info = {"column_names":[], "data_types":[], "autoincrement":[]}
    for record in hook.get_records(query):
        col_info['column_names'].append(record[2])
        col_info['data_types'].append(json.loads(record[3])['type'])
        col_info['autoincrement'].append(record[10])
    return col_info

@task
def snowflake_table_monitor(**context):
    full_table_path = context["target_table"]
    database, schema, table = full_table_path.split(".")
    snowflake_hook = SnowflakeHook(snowflake_conn_id="test_snowflake_conn")
    with snowflake_query_tracker(database=database, schema=schema) as snowflake_qt:
        record_count = get_record_count(snowflake_hook, database, table)
        log_metric("records", record_count)

        col_metadata = get_column_info(snowflake_hook, database, schema, table)
        log_metric("column metadata", col_metadata)

        if ENABLE_SNOWFLAKE_TABLE_SAMPLE:
            data = get_random_sample(snowflake_hook, database, schema, table, SNOWFLAKE_TABLE_SAMPLE_ROW_PROB)

        log_snowflake_table(
            table, 
            connection_string=snowflake_hook.get_uri(),
            database=database, 
            schema=schema, 
            with_preview=True, 
            with_schema=True
        )

    # get difference between last known state of table and the current state
    table_delta = 0
    column_diff = []
    try: 
        previous_record_count = Variable.get("{}_record_cnt".format(full_table_path))
        table_delta = previous_record_count - record_count

        previous_col_names = Variable.get("{}_column_names".format(full_table_path))
        column_diff = list(set(previous_col_names) - set(col_metadata['column_names']))
    except:
        pass

    col_changed = True if column_diff else False 
    log_metric("table_delta", table_delta)
    log_metric("columns_changed", col_changed)
    Variable.set("{}_record_cnt".format(full_table_path), record_count)
    Variable.set("{}_column_names".format(full_table_path), col_metadata['column_names'])
    
    # log metrics of the sampled data (if sampled)
    if ENABLE_SNOWFLAKE_TABLE_SAMPLE:
        log_metric("sample_size(%)", SNOWFLAKE_TABLE_SAMPLE_ROW_PROB)
        for column in data.columns:
            log_metric(
                "{} null record count".format(column), int(data[column].isna().sum())
            )

            if issubdtype(data[column].dtype, number):
                log_metric("{} mean".format(column), round(data[column].mean(), 2))
                log_metric("{} median".format(column), data[column].median())
                log_metric("{} min".format(column), data[column].min())
                log_metric("{} max".format(column), data[column].max())
                log_metric("{} std".format(column), round(data[column].std(), 2))
    
    context['ti'].xcom_push(key="{}_table_delta".format(full_table_path), value=table_delta)
    context['ti'].xcom_push(key="{}_record_count".format(full_table_path), value=record_count)

def aggregate_and_compare_metrics(**context):
    tables = context["target_tables"]
    
    metrics = {
        "table_names":tables,
    }
    table_deltas = []
    record_counts = []

    for table_name in tables:
        task_id = "{}_monitor".format(table_name)
        table_deltas.append(
            context['ti'].xcom_pull(task_ids=task_id, key="{}_table_delta".format(table_name))
        )
        record_counts.append(
            context['ti'].xcom_pull(task_ids=task_id, key = "{}_record_count".format(table_name))
        )
    
    metrics['table_deltas'] = table_deltas
    metrics['record_counts'] = record_counts
    
    log_metric("record_counts", record_counts)
    log_metric("table_deltas", table_deltas)
    log_metric("mean_record_count", round(mean(record_counts),2))
    log_metric("mean_table_deltas", round(mean(table_deltas),2))


    

with dag as dbnd_snowflake_monitor: 
    tables = (TARGET_TABLES.replace(" ", "")).split(",")
    airflow_tasks, tablenames = [], []

    for table in tables:
        tablename = table.split(".")[-1]
        tablenames.append(tablename)

        airflow_tasks.append(
            PythonOperator(
                task_id="{}_monitor".format(table),
                python_callable=snowflake_table_monitor,
                op_kwargs={
                    "target_table":table
                }
            )
        )

    agg_and_compare_metrics_task = PythonOperator(
        task_id="aggregate_and_compare_metrics",
        python_callable=aggregate_and_compare_metrics,
        op_kwargs={"target_tables": tables}
    )

    for base_task in airflow_tasks:
        base_task >> agg_and_compare_metrics_task