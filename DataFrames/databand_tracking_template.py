import pandas as pd
from sqlalchemy import create_engine 
from dbnd import log_metric, log_dataframe

QUERY = ""
DB_CONNECTION = ""

def track_database():
    engine = create_engine(DB_CONNECTION)
    log_metric("query executed", QUERY)

    with engine.connect() as connection:
        result = connection.execute(QUERY).keys()
        header = [row for row in result]

        result = connection.execute(QUERY)
        data = [row for row in result]
    
    df = pd.DataFrame(data, columns=header)

    log_dataframe(
        "DataFrame", 
        df,
        with_histograms=True, 
        with_schema=True, 
        with_size=True, 
        with_stats=True,
        with_preview=True
    )
    log_metric("row_count", df.shape[0])
    log_metric("column_count", df.shape[1])

def main():
    track_database()

if __name__ == '__main__':
    main()