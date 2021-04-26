![Databand & Python Logo](https://raw.githubusercontent.com/kalebinn/dbnd_doc_resources/main/python%2Bdbnd.png)
# Tracking Databases and DataFrames with Python and Databand
- [Overview](#overview)
- [Scope of Observability](#scope_of_observability)
- [Requirements](#requirements)
- [Set up Instructions](#setup-instructions)



## [Overview](#overview) 
Our DataFrame Tracking template allows you to query data from your database, extract the data as a Pandas DataFrame, and track metadata and statistics right in Databand. This method will also work wtih Spark DataFrames. 

## [Scope of Observability](#scope_of_observability)
These are the metrics that will be collected out of the box. Users can additional metrics with minimal changes.
 
- Column Count 
- Record Count 
- For each column:
  - Data Type
  - Non-null count 
  - Null count 
  - Distinct/Unique count
  - For numeric columns: 
    - 25, 50, 75 percentile 
    - max, mean, min, and standard deviation

## [Requirements](#requirements)
Your environment should be running Python 3.6+ with: 
- `dbnd>=0.37.1`
- `pandas>=1.1`
- `SQLAlchemy`

You can use the included `requirements.txt` file to install all dependencies at once. 

## [Set up Instructions](#setup-instructions)
After installing the requirements, you can start running the Python Template first exporting the required environment variables: 
```bash
export DBND__CORE__DATABAND_URL=<your Databand URL>
export DBND__CORE__DATABAND_ACCESS_TOKEN=<your access token>
export DBND__TRACKING=True
```  
Next modify lines 6-7 in `databand_tracking_template.py`: 
```python
DB_CONNECTION=<your database connection string> 
QUERY=<query>
```
`DB_CONNECTION` is the SQLAlchemy connection string and `QUERY` is the query you would like to execute.
  
Finally, you can run the file normally with `Python`, or schedule your Python script with a scheduler (e.g. `crontab` or `Airflow`). 





