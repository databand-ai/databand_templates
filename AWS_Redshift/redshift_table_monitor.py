import os
from warnings import warn
import airflow
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from dbnd import log_dataframe, log_metric
from dbnd._core.constants import DbndTargetOperationType
from numpy import issubdtype, mean, median, number
from pandas import DataFrame
from airflow.models import Variable

REDSHIFT_CONN_ID = Variable.get("redshift_table_monitor_conn_id")
DAG_ID = Variable.get("redshift_table_monitor_DAG_id")
MONITOR_SCHEDULE = Variable.get("redshift_table_monitor_schedule")
TARGET_TABLE = Variable.get("target_redshift_table")
REDSHIFT_MONITOR_QUERY_LIMIT = Variable.get("redshift_monitor_view_limit")

# query:
if REDSHIFT_MONITOR_QUERY_LIMIT:
    SELECT_DATA = "SELECT * FROM {} LIMIT %s;".format(TARGET_TABLE)
else:
    SELECT_DATA = "SELECT * FROM {}".format(TARGET_TABLE)

DEFAULT_ARGS = {
    "owner": "databand",
    "start_date": airflow.utils.dates.days_ago(0),
    "provide_context": False,
}

dag = dag = DAG(
    dag_id=DAG_ID,
    schedule_interval="{}".format(MONITOR_SCHEDULE),
    default_args=DEFAULT_ARGS
)


def monitor_redshift_table(**op_kwarg):
    """Redshift table monitor collects the following metrics:
        - record count
        - duplicate records
        - Null/NaN record counts in each column
        - mean, median, min, max, std of each numeric column
    """

    hook = PostgresHook(REDSHIFT_CONN_ID)
    if REDSHIFT_MONITOR_QUERY_LIMIT:
        data = hook.get_pandas_df(SELECT_DATA, parameters=[REDSHIFT_MONITOR_QUERY_LIMIT])
    else:
        data = hook.get_pandas_df(SELECT_DATA)

    log_dataframe(
        "{}".format(TARGET_TABLE),
        data,
        with_histograms=True,
        with_stats=True,
        with_schema=True,
    )

    log_metric("record count", data.shape[0])
    log_metric("Duplicate records", data.shape[0] - data.drop_duplicates().shape[0])
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


with dag as dbnd_template_dag:

    redshift_monitor = PythonOperator(
        task_id="monitor_redshift_table", python_callable=monitor_redshift_table
    )
