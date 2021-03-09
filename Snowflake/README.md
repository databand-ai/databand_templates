![Databand & Snowflake](https://raw.githubusercontent.com/kalebinn/dbnd_doc_resources/main/snowflake%2Bdbnd.png)
# Snowflake Monitoring Templates
- [Overview](#overview)
- [Scope of Observibility](#scope_of_observibility)
- [Requirements](#requirements)
- [Set up Instructions](#setup-instructions)
    - [Defining the Snowflake Airflow Connection](#airflow-connections)
    - [Defining the required Airflow Variables](#airflow-variables)
    - [Starting the DAG](#dag-start)



## [Overview](#overview) 
Our Snowflake Monitoring Templates provide out of the box observibility into your Snowflake database and tables without any modification to existing data pipelines. They are written as standalone Airflow DAGs but can be easily extracted to be additional tasks in other DAGs, if desired. 

## [Scope of Observibility](#scope_of_observibility)
These are the metrics that will be collected out of the box. Additional metrics can be added with minimal changes.
 
- [Snowflake Table Monitor](./snowflake_table_monitor.py)
    - record (row) count of each target table 
    - column names and data type of each target table
    - delta in record count since last monitor execution 
    - column/schema changes since last monitor execution 
    - statistics from a random sampling of each target table<sup>*</sup> 
        - includes mean, median, minimum, maximum and standard deviation of each numeric column in random sample 
    
- [Snowflake Database Monitor](./snowflake_database_monitor.py)
    - column count of each table
    - row count of each table
    - maximum, minimum, mean, and median table row count 
    - maximum, minimum, mean, and median column count

<sup>*</sup> The statistics calculated with the random sampling in the `Snowflake Table Monitor` is intended for use on relatively uniformly distributed data to detect sudden skews in data.  

## [Requirements](#requirements)
Your Airflow environment should be running Python 3.6+ and have the following requirements installed.
- `dbnd-airflow-auto-tracking`
- `dbnd-snowflake`
- `pandas` 

## [Set up Instructions](#setup-instructions)
After installing the requirements, the monitoring templates can be set up in three short steps. 

### [1. Defining the Snowflake Airflow Connection](#airflow-connections)
First, define an Airflow connection to Snowflake. This can be [done through the CLI of your Airflow environment](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html), or through Airflow's web UI.

Note that the Snowflake connection you define must have access to the database or table you are monitoring. 

### [2. Defining the required Airflow Variables](#airflow-variables)
After defining the connection, you must define several Airflow variables. These can be defined in your code using `airflow.models.Variable`, through your Airflow environment's CLI, or through Airflow's web UI.

The required variables are different for each monitor. 

#### Variables for [Snowflake table monitor](./snowflake_table_monitor.py)
| Variable Name | Description | Example | 
|---------------|-------------|---------|
|`snowflake_table_monitor_DAG_id`| `DAG ID` for the monitor | `snowflake_table_monitor_DAG_id`: `sample_snowflake_table_monitor`|
|`snowflake_connection` | Connection ID of Airflow connection to Snowflake.| `snowflake_connection`:`snowflake_conn_id`|
|`snowflake_table_monitor_target_tables`| Target tables to monitor. Multiple tables can be monitored with comma seperated values | `snowflake_table_monitor_target_tables`: `MY_DB1.PUBLIC.SF_TABLE1,MY_DB2.PUBLIC.SF_TABLE2`|
| | | |
|***Optional Variables*** | - | - |
|`snowflake_table_monitor_schedule`| Cron or Airflow schedule to run monitor. Default is `0 0 * * *`. | `snowflake_table_monitor_schedule`:`@weekly`|
|`enable_snowflake_table_sample`| Enabled random sampling of tables for statistics calculation. Default is `True`. | `snowflake_table_monitor_schedule`:`False`|
|`snowflake_table_sample_row_prob`| Probability of each row being included in random sample. This is also the expected proportional size for statistics, i.e. the random sample will include this percent of the total record count. Default is `1.0`%.| `snowflake_table_sample_row_prob`:`0.1`|


#### Variables for [Snowflake database monitor](./snowflake_database_monitor.py)
| Variable Name | Description | Example |
|---------------|-------------|---------|
|`snowflake_db_monitor_DAG_id`| `DAG ID` for the monitor | `snowflake_db_monitor_DAG_id`: `sample_snowflake_db_monitor`|
|`snowflake_connection`| Connection ID of Airflow connection to Snowflake. | `snowflake_connection`:`snowflake_conn_id`|
|`snowflake_db_monitor_schedule`| Cron or Airflow schedule to run monitor | `snowflake_db_monitor_schedule`:`0 1 * * *`| 
| `snowflake_db_monitor_target_db` | Target database to collect table metrics on | `snowflake_db_monitor_target_db`:`SNOWFLAKE_SAMPLE_DATA`|
| `snowflake_db_monitor_target_schema`:`public`|
    
### [3. Starting the DAG](#dag-start)
The final step is to move the monitor into the `dag` directory of your Airflow environment, and enable the monitoring DAG. 

---
The monitoring template most useful when paired with Databand's monitoring system. Metrics will be automatically collected and send to Databand's monitoring system where alerts, visualizations, data previews, historical trends, and more can be monitored. Visit [https://databand.ai/](https://databand.ai/) to learn more! If Databand is not available, metrics will be stored in Airflow logs. 



