import os
import airflow
import snowflake.connector
from airflow import DAG
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.operators.python_operator import PythonOperator
from dbnd import log_metric, task
from dbnd._core.constants import DbndTargetOperationType
from dbnd_snowflake import (
    log_snowflake_resource_usage,
    log_snowflake_table,
    snowflake_query_tracker,
)
from numpy import max, mean, median, min
from pandas import DataFrame
from airflow.models import Variable


SNOWFLAKE_CONNECTION = Variable.get("snowflake_connection")
SNOWFLAKE_DB_MONITOR_DAG_ID = Variable.get("snowflake_db_monitor_DAG_id")
DATABASE = Variable.get("snowflake_db_monitor_target_db")
SCHEMA = Variable.get("snowflake_db_monitor_target_schema")
MONITOR_SCHEDULE = Variable.get("snowflake_db_monitor_schedule")

DEFAULT_ARGS = {
    "owner": "databand",
    "start_date": airflow.utils.dates.days_ago(0),
    "provide_context": False,
}

dag = DAG(
    dag_id="{}".format(SNOWFLAKE_DB_MONITOR_DAG_ID),
    schedule_interval="{}".format(MONITOR_SCHEDULE),
    default_args=DEFAULT_ARGS,
    tags=["dbnd_monitor", "snowflake"],
)
# QUERIES
GET_COLUMNS = """
show columns in database {};
""".format(
    DATABASE
)

GET_DB_ROW_INFO = """
SELECT table_name, row_count
FROM {}.information_schema.tables where TABLE_SCHEMA = '{}';
""".format(
    DATABASE, SCHEMA
)


def snowflake_db_monitor(**op_kwarg):
    snowflake_hook = SnowflakeHook(snowflake_conn_id="test_snowflake_conn")

    with snowflake_query_tracker(database=DATABASE, schema=SCHEMA) as st:
        snowflake_tables = snowflake_hook.get_pandas_df(GET_COLUMNS)
        snowflake_shapes = DataFrame()
        snowflake_tables = snowflake_tables[
            snowflake_tables["schema_name"] == "{}".format(SCHEMA)
        ]       

    snowflake_shapes["column_count"] = snowflake_tables.groupby(
        "table_name"
    ).nunique("column_name")["column_name"]
    snowflake_shapes["table_name"] = snowflake_tables["table_name"].unique()

    table_row_info = {}
    snowflake_rows = snowflake_hook.get_records(GET_DB_ROW_INFO)
    for tablename, row_count in snowflake_rows:
        table_row_info[tablename] = row_count

    row_counts = list(table_row_info.values())
    log_metric("Max table row count", max(row_counts))
    log_metric("Min table row count", min(row_counts))
    log_metric("Mean table row count", round(mean(row_counts), 2))
    log_metric("Median table row count", median(row_counts))

    snowflake_shapes["row_count"] = (
        snowflake_shapes["table_name"].map(table_row_info).fillna(0).astype(int)
    )

    for _, row in snowflake_shapes.iterrows():
        log_metric(
            "{} shape".format(row["table_name"]),
            (row["column_count"], row["row_count"]),
        )

    log_metric("Max table column count", snowflake_shapes["column_count"].max())
    log_metric("Min table column count", snowflake_shapes["column_count"].max())
    log_metric(
        "Mean table column count", round(snowflake_shapes["column_count"].mean(), 2)
    )
    log_metric(
        "Median table column count", snowflake_shapes["column_count"].median()
    )

    # return snowflake_shapes


with dag as dbnd_template_dag:

    snowflake_monitor = PythonOperator(
        task_id="monitor_snowflake_db", python_callable=snowflake_db_monitor
    )
