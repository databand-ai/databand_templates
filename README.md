# Monitoring Templates
The files in this repository are template Airflow DAGs for monitoring your data lakes. You can integrate them into your Airflow environment and run them as you would any other DAG. The templates are built using Databand's DBND tracking library and will report metrics about your database clusters, databases, and tables. The metrics will be reported to Databand's monitoring system. The templates are completely customizable, you can modify them to build any required metric reporting and data quality checks.

![Monitoring Template Overview](https://raw.githubusercontent.com/kalebinn/dbnd_doc_resources/main/MonitoringTemplatesOverview.png)

Contents
- [AWS Redshift](.)
- [AWS S3](./AWS_S3/)
    - [S3 Bucket Monitor](./AWS_S3/s3_bucket_monitor.py)
    - [S3 Key Monitor](./AWS_S3/s3_key_monitor.py)
- [Snowflake](.)