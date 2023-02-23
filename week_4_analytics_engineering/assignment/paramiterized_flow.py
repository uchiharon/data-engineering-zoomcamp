from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash
from datetime import timedelta
from random import randint


@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read data from web into pandas DataFrame"""
    # if randint(0,1) > 0:
    #     raise Exception
    df = pd.read_csv(dataset_url)
    return df

@task(log_prints=True)
def clean(df: pd.DataFrame, color:str) -> pd.DataFrame:
    """Fix dtype issues"""
    """Different columns"""
    if color == 'yellow':
        df['tpep_pickup_datetime'] =pd.to_datetime(df['tpep_pickup_datetime'])
        df['tpep_dropoff_datetime'] =pd.to_datetime(df['tpep_dropoff_datetime'])
    elif color == 'green':
        df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
        df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])
        df['ehail_fee'] = df['ehail_fee'].astype(float)
        df['trip_type'] = df['trip_type'].astype(str)

    """Common columns"""
    df['VendorID'] = pd.to_datetime(df['VendorID'])
    df['passenger_count'] = df['passenger_count'].astype(int)
    df['trip_distance'] = df['trip_distance'].astype(float)
    df['RatecodeID'] = df['RatecodeID'].astype(str)
    df['store_and_fwd_flag'] = df['store_and_fwd_flag'].astype(str)
    df['PULocationID'] = df['PULocationID'].astype(str)
    df['DOLocationID'] = df['DOLocationID'].astype(str)
    df['payment_type'] = df['payment_type'].astype(str)
    df['fare_amount'] = df['fare_amount'].astype(float)
    df['extra'] = df['extra'].astype(float)
    df['mta_tax'] = df['mta_tax'].astype(float)
    df['tip_amount'] = df['tip_amount'].astype(float)
    df['tolls_amount'] = df['tolls_amount'].astype(float)
    df['improvement_surcharge'] = df['improvement_surcharge'].astype(float)
    df['total_amount'] = df['total_amount'].astype(float)
    df['congestion_surcharge'] = df['congestion_surcharge'].astype(float)

    print(df.head(2))
    print(f'columns dtype: {df.dtypes}')
    print(f'columns: {df.shape[1]}')
    print(f'rows: {df.shape[0]}')
    return df

@task()
def write_local(df:pd.DataFrame, color:str, dataset_file:str) -> Path:
    """Write dataFrame out locally as a csv file"""
    path = Path(f"data/{color}/{dataset_file}.parquet").as_posix()
    df.to_csv(path)
    return path

@task(retries=3)
def write_gcs(path:Path) ->None:
    """Uploading local parquet file to gcs"""

    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(
        from_path=path,
        to_path=path,
        timeout=600
    )
    return

@flow()
def etl_web_to_gcs(color:str, year:int, month:int) -> None:
    """The main ETL function"""
    datafile = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{datafile}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df,color)
    path = write_local(df_clean,color,datafile)
    write_gcs(path)

@flow()
def etl_parent_flow(
    months: list[int] =[i+1 for i in range(12)], year:int =2021, color:str="yellow"
):
    for month in months:
        etl_web_to_gcs(color,year,month)
    
    
if __name__=='__main__':
    color = "yellow"
    months = [1,2,3]
    year = 2021
    etl_parent_flow(months,year,color)