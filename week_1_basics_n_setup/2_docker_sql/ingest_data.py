#!/usr/bin/env python
# coding: utf-8



import pandas as pd
from sqlalchemy import create_engine
import argparse
import os

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db =params.db
    table_name =params.table_name
    url = params.url

    parquet_name = 'output.parquet'

    os.system("wget {} -O {}".format(url, parquet_name))

    # download the parque

    engine  = create_engine('postgresql://{}:{}@{}:{}/{}'.format(user,password,host,port,db))

    df = pd.read_parquet(parquet_name)

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists="replace")

    df.to_sql(name=table_name, con=engine, if_exists="append")

    return  "Data ingestion completed"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Ingest parque data to Postgres')

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of the table where we will write the result to')
    parser.add_argument('--url', help='url of the parquet file')

    args = parser.parse_args()

    main(args)