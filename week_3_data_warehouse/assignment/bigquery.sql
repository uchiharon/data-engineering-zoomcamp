 -- CREATE EXTERNAL TABLE referring to the gcp path
CREATE OR REPLACE EXTERNAL TABLE
  `climate-team-1.trips_data_all.external_fhv_2019` OPTIONS( format="parquet",
    uris=[ "gs://dtc_data_lake_climate-team-1/data/fhv/fhv_tripdata_2019-*.parquet"] );

-- Preview external table
SELECT * FROM `trips_data_all.external_fhv_2019` ORDER BY pickup_datetime LIMIT 10;

-- CREATE non partitioned TABLE
CREATE OR REPLACE TABLE
  trips_data_all.fhv_2019_nonpartitioned AS
SELECT
  *
FROM
  trips_data_all.external_fhv_2019;


-- Preview table
SELECT * FROM `trips_data_all.fhv_2019_nonpartitioned` ORDER BY pickup_datetime LIMIT 10;


-- Question 1
-- What is the count for fhv vehicle records for year 2019?

SELECT COUNT(*) FROM `trips_data_all.fhv_2019_nonpartitioned`

-- Answer
-- 43244696



-- Question 2
-- Write a query to count the distinct number of affiliated_base_number for the entire dataset on both the tables.
-- What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

-- Count for external table
SELECT COUNT(DISTINCT affiliated_base_number) FROM `trips_data_all.external_fhv_2019`

-- Count for table
SELECT COUNT(DISTINCT affiliated_base_number) FROM `trips_data_all.fhv_2019_nonpartitioned`


-- Answer
-- Pick 0, 0
-- External table: Count- 3164, Duration- 2 sec, Bytes processed- 329.75 MB, Bytes billed- 330 MB
-- Table:          Count- 3164, Duration- 0 sec, Bytes processed- 329.75 MB, Bytes billed- 330 MB


-- Question 3
-- How many records have both a blank (null) PUlocationID and DOlocationID in the entire dataset?

SELECT COUNT(*) FROM `trips_data_all.fhv_2019_nonpartitioned` WHERE PUlocationID IS NULL AND DOlocationID IS NULL

-- Answer
-- 717748



-- Question 4 
-- What is the best strategy to optimize the table if query always filter by pickup_datetime and order by affiliated_base_number?

-- Answer
-- Partition by pickup_datetime Cluster on affiliated_base_number




-- Question 5
-- Implement the optimized solution you chose for question 4. Write a query to retrieve the distinct affiliated_base_number between pickup_datetime 2019/03/01 and 2019/03/31 (inclusive).
-- Use the BQ table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values? Choose the answer which most closely matches.


-- CREATE TABLE that is Partition by pickup_datetime and Cluster on affiliated_base_number
CREATE OR REPLACE TABLE
  trips_data_all.fhv_2019_partitioned_and_clustered 
  PARTITION BY DATE(pickup_datetime)
  CLUSTER BY affiliated_base_number AS
SELECT
  *
FROM
  trips_data_all.external_fhv_2019;


-- Preview the partition and cluster tabler
SELECT * FROM `trips_data_all.fhv_2019_partitioned_and_clustered` LIMIT 10

-- Query the distinct affiliated_base_number between pickup_datetime 2019/03/01 and 2019/03/31 (inclusive) for the non partition table
SELECT COUNT(DISTINCT affiliated_base_number) FROM `trips_data_all.fhv_2019_nonpartitioned` 
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31'

-- Query the distinct affiliated_base_number between pickup_datetime 2019/03/01 and 2019/03/31 (inclusive) for the partition and clustered table
SELECT COUNT(DISTINCT affiliated_base_number) FROM `trips_data_all.fhv_2019_partitioned_and_clustered` 
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31'


-- Answer
-- Pick 0, 0
-- Partition table: Count- 725, Duration- 0 sec, Bytes processed- 23.09 MB, Bytes billed- 24 MB
-- Table:           Count- 725, Duration- 0 sec, Bytes processed- 659.68 MB, Bytes billed- 660 MB




-- Question 6
-- Where is the data stored in the External Table you created?

-- Answer
-- GCP Bucket




-- Question 7
-- It is best practice in Big Query to always cluster your data?

-- Answer
-- False






