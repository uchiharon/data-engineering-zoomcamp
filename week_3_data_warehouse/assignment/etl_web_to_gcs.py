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
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    df['dispatching_base_num'] = df['dispatching_base_num'].astype(str)
    df['pickup_datetime'] =pd.to_datetime(df['pickup_datetime'])
    df['dropOff_datetime'] =pd.to_datetime(df['dropOff_datetime'])
    df['PUlocationID'] = df['PUlocationID'].astype(float)
    df['DOlocationID'] = df['DOlocationID'].astype(float)
    df['SR_Flag'] = df['SR_Flag'].astype(float)
    df['Affiliated_base_number'] = df['Affiliated_base_number'].astype(str)

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
        to_path=path,
        timeout=700
        
    )
    return

@flow()
def etl_web_to_gcs(color:str, year:int, month:int) -> None:
    """The main ETL function"""
    datafile = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{datafile}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean,color,datafile)
    write_gcs(path)

@flow()
def etl_parent_flow(
    months: list[int] =[i + 1 for i in range(12)], year:int =2019, color:str="fhv"
):
    for month in months:
        etl_web_to_gcs(color,year,month)
    
    
if __name__=='__main__':
    color = "fhv"
    months = [i + 1 for i in range(12)]
    year = 2019
    etl_parent_flow(months,year,color)