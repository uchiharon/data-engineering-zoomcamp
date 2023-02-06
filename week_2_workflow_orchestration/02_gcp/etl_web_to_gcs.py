from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash
from datetime import timedelta
from random import randint

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1), retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read data from web into pandas DataFrame"""
    # if randint(0,1) > 0:
    #     raise Exception
    df = pd.read_csv(dataset_url)
    return df

@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    df['tpep_pickup_datetime'] =pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] =pd.to_datetime(df['tpep_dropoff_datetime'])
    print(df.head(2))
    print(f'columns dtype: {df.dtypes}')
    print(f'columns: {df.shape[1]}')
    print(f'rows: {df.shape[0]}')
    return df

@task()
def write_local(df:pd.DataFrame, color:str, dataset_file:str) -> Path:
    """Write dataFrame out locally as a parquet file"""
    path = Path(f"data/{color}/{dataset_file}.parquet").as_posix()
    df.to_parquet(path,compression="gzip")
    return path

@task()
def write_gcs(path:Path) ->None:
    """Uploading local parquet file to gcs"""

    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(
        from_path=path,
        to_path=path
        
    )
    return

@flow()
def etl_web_to_gcs() -> None:
    """The main ETL function"""
    color = "yellow"
    year = 2021
    month = 1
    datafile = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{datafile}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean,color,datafile)
    write_gcs(path)
    
if __name__=='__main__':
    etl_web_to_gcs()