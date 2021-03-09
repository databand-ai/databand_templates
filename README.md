![Databand Logo](https://raw.githubusercontent.com/kalebinn/dbnd_doc_resources/main/dbnd.png)
# Monitoring Templates
The files in this repository are templated python processes for monitoring your data lakes. You can integrate them into your Airflow environment and run them as you would any other DAG. The templates are built using Databand's tracking library ("DBND") and will report metrics about your database clusters, databases, and tables. The metrics will be reported to Databand's monitoring system where you can monitor trends and define alerts. The templates are completely customizable, you can modify them to collect any required metric reporting and data quality checks.

![Monitoring Template Overview](https://raw.githubusercontent.com/kalebinn/dbnd_doc_resources/main/MonitoringTemplatesOverview.png)

Contents
- [AWS Redshift](./AWS_Redshift)
    - [Redshift Database Monitor](./AWS_Redshift)
    - [Redshift Table Monitor](./AWS_Redshift)
- [AWS S3](./AWS_S3/)
    - [S3 Bucket Monitor](./AWS_S3/)
    - [S3 Key Monitor](./AWS_S3/)
- [Snowflake](./Snowflake)
    - [Snowflake Database Monitor](./Snowflake)
    - [Snowflake Table Monitor](./Snowflake)
