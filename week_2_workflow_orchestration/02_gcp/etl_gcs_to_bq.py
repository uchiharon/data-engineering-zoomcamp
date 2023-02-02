from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash
from datetime import timedelta
from random import randint
from prefect_gcp import GcpCredentials


@task(retries=3)
def extract_from_gcs(color:str, year:int, month:int) -> Path:
    """Download trip data from GCS"""
    gcs_path =  f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path="./")

    return Path(gcs_path).as_posix()


@task()
def transform(path:Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    print(f"Pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    df['passenger_count'].fillna(0, inplace=True)
    print(f"Post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df

@task()
def write_bq(df:pd.DataFrame) -> None:
    """Write DataFrame to BigQuery"""

    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")
    df.to_gbq(
        destination_table="trips_data_all.yellow_taxi_trips",
        project_id="climate-team-1",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500000,
        if_exists="append"
    )


@flow()
def gcs_to_bq():
    """Main ETL to load data into Big Query"""
    color = "yellow"
    year = 2021
    month = 1
    path = extract_from_gcs(color, year, month)
    df = transform(path)
    write_bq(df)
    

if __name__=="__main__":
    gcs_to_bq()
    