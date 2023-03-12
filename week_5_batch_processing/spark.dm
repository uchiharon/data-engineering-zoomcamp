How to spark standalone cluster is run on windows OS

Change the working directory to the spark directory:
if you have setup up your SPARK_HOME variable, use the following;
cd %SPARK_HOME%    
if not, use the following;
cd <path to spark installation>

Creating a Local Spark Cluster

To start Spark Master:

bin\spark-class org.apache.spark.deploy.master.Master --host localhost

Starting up a cluster:

bin\spark-class org.apache.spark.deploy.worker.Worker spark://localhost:7077 --host localhost



python 09_spark_sql-script.py \
	--input_green='data/green/*.parquet' \
	--input_yellow='data/yellow/*.parquet' \
	--output='data/monthly_revenue'


URL="spark://localhost:7077"

spark-submit \
	--master=${URL} \
	--num-executors 2 
	python 09_spark_sql-script.py \
		--input_green='data/green/*.parquet' \
		--input_yellow='data/yellow/*.parquet' \
		--output='data/monthly_revenue'



--input_green=gs://dtc_data_lake_climate-team-1/spark_data/green/*.parquet
--input_yellow=gs://dtc_data_lake_climate-team-1/spark_data/yellow/*.parquet
--output=gs://dtc_data_lake_climate-team-1/spark_data/monthly_revenue
gs://dtc_data_lake_climate-team-1/

gcloud dataproc jobs submit pyspark \
	gs://dtc_data_lake_climate-team-1/code/10_spark_sql-dataproc.py \
	--cluster=de-zoomcamp-cluster \
	--region=europe-west6 \
	-- \
		--input_green=gs://dtc_data_lake_climate-team-1/spark_data/green/*.parquet \
		--input_yellow=gs://dtc_data_lake_climate-team-1/spark_data/yellow/*.parquet \
		--output=gs://dtc_data_lake_climate-team-1/spark_data/monthly_revenue


gcloud dataproc jobs submit pyspark \
	gs://dtc_data_lake_climate-team-1/code/11_spark_sql-bigquery.py \
	--cluster=de-zoomcamp-cluster \
	--jars=gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.23.2.jar \
	--region=europe-west6 \
	-- \
		--input_green=gs://dtc_data_lake_climate-team-1/spark_data/green/*.parquet \
		--input_yellow=gs://dtc_data_lake_climate-team-1/spark_data/yellow/*.parquet \
		--output=trips_data_all.monthly_revenue


$ gsutil cp fhvhv_tripdata_2021-06.csv.gz gs://dtc_data_lake_climate-team-1/spark_data/fhvhv/fhvhv_tripdata_2021-06.csv.gz

