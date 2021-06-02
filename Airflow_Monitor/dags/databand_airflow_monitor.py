import logging
import os

from datetime import timedelta

from airflow import settings
from airflow.hooks.base_hook import BaseHook
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago


CHECK_INTERVAL = 10  # Sleep time (in seconds) between sync iterations
AUTO_RESTART_TIMEOUT = 30 * 60  # Restart after this number of seconds
# We're using FORCE_RESTART_TIMEOUT as backup mechanism for the case monitor is
# stuck for some reason. Normally it should auto-restart by itself after
# AUTO_RESTART_TIMEOUT, but in case it's not - we'd like to kill it.
FORCE_RESTART_TIMEOUT = timedelta(seconds=AUTO_RESTART_TIMEOUT + 5 * 60)

LOG_LEVEL = "WARN"
DATABAND_AIRFLOW_CONN_ID = "dbnd_config"
"""
Airflow-Monitor-as-DAG expects airflow connection with dbnd configuration
to be define:
connection id: dbnd_config
extra:
{
  "core": {
    "databand_url": "<dbnd webserver url>",
    "databand_access_token": "<access token>"
  },
  "airflow_monitor": {
    "syncer_name": "<airflow syncer name>"
  }
}
"""

args = {
    "owner": "Databand",
    "start_date": days_ago(2),
}

logger = logging.getLogger(__name__)


class MonitorOperator(BashOperator):
    def pre_execute(self, context):
        dbnd_conn_config = BaseHook.get_connection(DATABAND_AIRFLOW_CONN_ID)
        json_config = dbnd_conn_config.extra_dejson

        dbnd_config = self.to_env(
            self.flatten(json_config, parent_key="DBND", sep="__")
        )

        self.env = os.environ.copy()
        self.env.update(dbnd_config)
        self.env.update(
            {
                "DBND__LOG__LEVEL": LOG_LEVEL,
                "DBND__AIRFLOW_MONITOR__SQL_ALCHEMY_CONN": settings.SQL_ALCHEMY_CONN,
                "DBND__AIRFLOW_MONITOR__LOCAL_DAG_FOLDER": settings.DAGS_FOLDER,
                "DBND__AIRFLOW_MONITOR__FETCHER": "db",
            }
        )

    def flatten(self, d, parent_key="", sep="_"):
        """
        Flatten input dict to env variables:
        { "core": { "conf1": "v1", "conf2": "v2" } } =>
        { "dbnd__core__conf1": "v1", "dbnd__core__conf2": "v2" }

        source: https://stackoverflow.com/a/6027615/15495440
        """
        items = []
        for k, v in d.items():
            new_key = parent_key + sep + k if parent_key else k
            if isinstance(v, dict):
                items.extend(self.flatten(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        return dict(items)

    def to_env(self, d):
        """
        convert dict to be env friendly - uppercase keys and stringify values
        """
        return {k.upper(): str(v) for k, v in d.items()}


dag = DAG(
    dag_id="databand_airflow_monitor_v2",
    default_args=args,
    schedule_interval="* * * * *",
    dagrun_timeout=None,
    tags=["project:airflow-monitor"],
    max_active_runs=1,
    catchup=False,
)

with dag:
    # show_env = BashOperator(task_id="env", bash_command="env")
    opts = " --interval %d " % CHECK_INTERVAL
    if AUTO_RESTART_TIMEOUT:
        opts += " --stop-after %d " % AUTO_RESTART_TIMEOUT

    run_monitor = MonitorOperator(
        task_id="monitor",
        task_concurrency=1,
        retries=10,
        bash_command="python3 -m dbnd airflow-monitor-v2 %s" % opts,
        retry_delay=timedelta(seconds=1),
        retry_exponential_backoff=False,
        max_retry_delay=timedelta(seconds=1),
        execution_timeout=FORCE_RESTART_TIMEOUT,
    )

if __name__ == "__main__":
    dag.cli()
