![Databand & AWS Redshift Logo](https://raw.githubusercontent.com/kalebinn/dbnd_doc_resources/main/redshift%2Bdbnd.png)
# AWS Redshift Monitoring Templates
- [Overview](#overview)
- [Scope of Observibility](#scope_of_observibility)
- [Requirements](#requirements)
- [Set up Instructions](#setup-instructions)
    - [Defining the AWS Airflow Connection](#airflow-connections)
    - [Defining the required Airflow Variables](#airflow-variables)
    - [Starting the DAG](#dag-start)



## [Overview](#overview) 
Our AWS Redshift Monitoring Templates provide out of the box observibility into your Redshift database and tables without any modification to existing data pipelines. They are written as standalone Airflow DAGs but can be easily extracted to be additional tasks in other DAGs, if desired. 

## [Scope of Observibility](#scope_of_observibility)
These are the metrics that will be collected out of the box. Additional metrics can be added with minimal changes.
 
- [Redshift Database Monitor](./redshift_database_monitor.py)
    - Number of tables in database 
    - Minimum column and row count of tables 
    - Maximum column and row count of tables 
    - Mean column and row count of tables 
    - Median column and row count of tables 
    - Disk usage of Redshift Instance (Capacity, free, and used in GB)
- [Redshift Table Monitor](./redshift_table_monitor.py)<sup>*</sup>
    - Record count of the target table 
    - Duplicate record count 
    - `Null`/`Nan` record count for each column 
    - Maximum of `numeric` columns
    - Minimum of `numeric` columns
    - Mean of `numeric` columns 
    - Median of `numeric` columns

<sup>*</sup> The `Redshift Table Monitor` takes a naive approach in calcuating statistics. For large Redshift tables, it is recommended to define the optional variables listed to reduce any cluster compute time. 

## [Requirements](#requirements)
Your Airflow environment should be running Python 3.6+ and have the following requirements installed.
- `dbnd-airflow-auto-tracking`
- `pandas` 

## [Set up Instructions](#setup-instructions)
After installing the requirements, the monitoring templates can be set up in three short steps. 

### [1. Defining the AWS Airflow Connection](#airflow-connections)
First, define an Airflow connection to AWS. This can be [done through the CLI of your Airflow environment](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html), or through Airflow's web UI.

Note that the AWS Redshift connection you define must have access to the database or table you are monitoring. 

### [2. Defining the required Airflow Variables](#airflow-variables)
After defining the connection, you must define several Airflow variables. These can be defined in your code using `airflow.models.Variable`, through your Airflow environment's CLI, or through Airflow's web UI.

The required variables are different for each monitor. 

#### Variables for [Redshift database](./redshift_database_monitor.py)
| Variable Name | Description | Example |
|---------------|-------------|---------|
|`redshift_database_monitor_DAG_id`| `DAG ID` for the monitor | `redshift_database_monitor_DAG_id`: `sample_redshift_db_monitor`|
|`redshift_db_monitor_conn_id`| Connection ID of Airflow connection to Redshift database. | `redshift_db_monitor_conn_id`:`redshift_conn`|
|`redshift_db_monitor_schedule`| Cron or Airflow schedule to run monitor | `redshift_db_monitor_schedule`:`0 1 * * *`| 
| `target_redshift_schema` | Target schema to collect table metrics on | `target_redshift_schema`:`public`|

#### Variables for [Redshift table monitor](./redshift_table_monitor.py)
| Variable Name | Description | Example | 
|---------------|-------------|---------|
|`redshift_table_monitor_DAG_id`| `DAG ID` for the monitor | `redshift_table_monitor_DAG_id`: `sample_redshift_table_monitor`|
|`redshift_table_monitor_conn_id` | Connection ID of Airflow connection to Redshift database. This database must contain the `target_table`| `redshift_table_monitor_conn_id`:`redshift_conn`|
|`redshift_table_monitor_schedule`| Cron or Airflow schedule to run monitor | `redshift_table_monitor_schedule`:`@daily`|
|`target_redshift_table`| Target table to monitor | `target_redshift_table`: `sample_table`|
| | | |
|***Optional Variables*** | - | - |
|`redshift_monitor_view_limit` | Limit on number of rows to collect data on. Highly recommended variable for large tables | `redshift_monitor_view_limit`:`1000`|

    
### [3. Starting the DAG](#dag-start)
The final step is to move the monitor into the `dag` directory of your Airflow environment, and enable the monitoring DAG. 

---
The monitoring template most useful when paired with Databand's monitoring system. Metrics will be automatically collected and send to Databand's monitoring system where alerts, visualizations, data previews, historical trends, and more can be monitored. Visit [https://databand.ai/](https://databand.ai/) to learn more! If Databand is not available, metrics will be stored in Airflow logs. 



