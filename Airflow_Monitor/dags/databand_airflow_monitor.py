from airflow_monitor.monitor_as_dag import get_monitor_dag
​
​
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
​
dag = get_monitor_dag()
​
if __name__ == "__main__":
    dag.cli()
    
