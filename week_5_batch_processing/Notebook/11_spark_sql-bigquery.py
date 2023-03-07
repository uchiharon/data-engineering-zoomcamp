#!/usr/bin/env python
# coding: utf-8


# Import all require modules for the ETL process
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import argparse




parser = argparse.ArgumentParser(description='Ingest parque data to Postgres')
parser.add_argument('--input_green', help='green_tripdata source file')
parser.add_argument('--input_yellow', help='yellow_tripdata source file')
parser.add_argument('--output', help='output file')

args = parser.parse_args()

green_input = args.input_green
yellow_input = args.input_yellow
output = args.output


# Create connection to master spark cluster 
spark = SparkSession.builder \
    .appName('test') \
    .getOrCreate()

bucket = "dataproc-temp-europe-west6-928127929501-44dwvjry"
spark.conf.set('temporaryGcsBucket', bucket)


# read the parquet dataset
df_green = spark.read.parquet(green_input)
df_yellow = spark.read.parquet(yellow_input)


# Get summary of dataset columns and rows
print(f'Number of columns: \nYellow_tripdata: {len(df_yellow.columns)}\nGreen_tripdata: {len(df_green.columns)}')

print(f'Number of rows: \nYellow_tripdata: {df_yellow.count()}\nGreen_tripdata: {df_green.count()}')


# Start transformation of dataset
## Pre transformation dataset preview
print(f'Pre Transform: The common columns is {len(set(df_yellow.columns) & set(df_green.columns))}')

# transfromation of yellow taxi data by renaming the datetime columns
df_yellow = df_yellow \
    .withColumnRenamed('tpep_pickup_datetime','pickup_datetime') \
    .withColumnRenamed('tpep_dropoff_datetime','dropoff_datetime')

# transfromation of green taxi data by renaming the datetime columns
df_green = df_green \
    .withColumnRenamed('lpep_pickup_datetime','pickup_datetime') \
    .withColumnRenamed('lpep_dropoff_datetime','dropoff_datetime')


## Post transformation dataset preview
print(f'Post Transform: The common columns is {len(set(df_yellow.columns) & set(df_green.columns))}')

# Store common columns because we would use just them for the rest part of the data transformation
common_columns  = list(sorted(set(df_yellow.columns) & set(df_green.columns), key=df_yellow.columns.index))


# Select essential columns
## A new column service_type was created to help use identify the 
## service type and dataset the record comes from
df_green_req = df_green.select(common_columns) \
    .withColumn('service_type', F.lit('green'))

df_yellow_req = df_yellow.select(common_columns) \
    .withColumn('service_type', F.lit('yellow'))


# Combine the green and yellow trip datasets
df_trip_data = df_green_req.unionAll(df_yellow_req)


# Preview the combined dataset
print(df_trip_data.groupby('service_type').count().show())


### Spark SQL
# Create a temporary view on the cluster
df_trip_data.createOrReplaceTempView ('trip_data')


# Write a sql query to perform the last transformation of the ETL process
df_result = spark.sql("""

SELECT 
    -- Reveneue grouping 
    PULocationID AS revenue_zone,
    date_trunc('month', pickup_datetime) AS revenue_month, 
    service_type, 

    -- Revenue calculation 
    SUM(fare_amount) AS revenue_monthly_fare,
    SUM(extra) AS revenue_monthly_extra,
    SUM(mta_tax) AS revenue_monthly_mta_tax,
    SUM(tip_amount) AS revenue_monthly_tip_amount,
    SUM(tolls_amount) AS revenue_monthly_tolls_amount,
    SUM(improvement_surcharge) AS revenue_monthly_improvement_surcharge,
    SUM(total_amount) AS revenue_monthly_total_amount,
    SUM(congestion_surcharge) AS revenue_monthly_congestion_surcharge,

    -- Additional calculations
    AVG(passenger_count) AS avg_montly_passenger_count,
    AVG(trip_distance) AS avg_montly_trip_distance
FROM
    trip_data
GROUP BY
    1, 2, 3



""")


# To write the result to one partition
df_result.write.format('bigquery') \
    .option('table', output) \
    .save()

print("Completed ETL workflow")