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
def load_data(path:Path) -> pd.DataFrame:
    df = pd.read_parquet(path)
    return df

@task(retries=3)
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
def gcs_to_bq(
    color:str, year:int, month:int
) -> int:
    """Main ETL to load data into Big Query"""
    path = extract_from_gcs(color, year, month)
    df = load_data(path)
    write_bq(df)
    return int(df.shape[0])


@flow(log_prints=True)
def el_parent_flow(
    color:str = "yellow", year:int = 2021, months:list[int] = [1,2]
) -> None:
    total_row = 0
    for month in months:
        current_rows = gcs_to_bq(color,year,month)
        total_row += current_rows
        print(f"The total rows uploaded at the moment is: {total_row}")
    print(f"Completed uploading a total of {total_row} rows to bq")
    return  

if __name__=="__main__":
    el_parent_flow()
    