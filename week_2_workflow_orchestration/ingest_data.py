from time import time
import pandas as pd
from sqlalchemy import create_engine
import os
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta


@task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_data(url):
    if url.endswith(".csv.gz"):
        csv_name = "yellow_tripdata_2021-01.csv.gz"
    else:
        csv_name = "output.csv"

    os.system("wget {} -O {}".format(url, csv_name))

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    return df


@task(log_prints=True)
def transform_data(df):
    missing_passenger_count = df["passenger_count"].isin([0]).sum()
    print("Pre: missing passenger count: {}".format(missing_passenger_count))
    df = df[df['passenger_count'] != 0]
    missing_passenger_count = df["passenger_count"].isin([0]).sum()
    print("Post: missing passenger count: {}".format(missing_passenger_count))

    return df




@task(log_prints=True, retries=3)
def ingest_data(user, password, host, port, db, table_name, df):
    engine  = create_engine('postgresql://{}:{}@{}:{}/{}'.format(user,password,host,port,db))

    df.head(0).to_sql(name=table_name, con=engine, if_exists="replace")
    df.to_sql(name=table_name, con=engine, if_exists="append")


    # while True:
    #     try:
    #         t_start = time()
    #         df = next(df_iter)
    #         df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    #         df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    #         df.to_sql(name=table_name, con=engine, if_exists="append")

    #         t_end = time()
    #         print("inserted another chunk. Took %.3f second" % (t_end - t_start))
    #     except StopIteration:
    #         print("Finished ingesting data into the postgres database")
    #         break

@flow(name="Ingest Flow")
def main():
    user="root"
    password="root"
    host="localhost"
    port="8080"
    db="ny_taxi"
    table_name="yellow_taxi_trips"
    url="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"

    raw_data = extract_data(url)
    data = transform_data(raw_data)
    ingest_data(user, password, host, port, db, table_name, data)

if __name__ == "__main__":
    main()