import pyspark.sql.types as T

INPUT_DATA_PATH_fhv = 'data/fhv_tripdata_2019-01.csv.gz'
INPUT_DATA_PATH_green = 'data/green_tripdata_2019-01.csv.gz'

BOOTSTRAP_SERVERS = 'localhost:9092'

TOPIC_WINDOWED_FHV_PUID_COUNT = CONSUME_TOPIC_FHV_CSV = 'fhv_puid_counts_windowed'
TOPIC_WINDOWED_GREEN_PUID_COUNT = CONSUME_TOPIC_GREEN_CSV = 'green_puid_counts_windowed'



FHV_SCHEMA = T.StructType(
    [T.StructField("dispatching_base_num", T.StringType()),
     T.StructField('pickup_datetime', T.TimestampType()),
     T.StructField('dropOff_datetime', T.TimestampType()),
     T.StructField("PUlocationID", T.IntegerType()),
     T.StructField("DOlocationID", T.IntegerType()),
     T.StructField("SR_Flag", T.FloatType()),
     T.StructField("Affiliated_base_number", T.StringType()),
     ])




GREEN_SCHEMA = T.StructType(
    [T.StructField("VendorID", T.IntegerType()),
     T.StructField('lpep_pickup_datetime', T.TimestampType()),
     T.StructField('lpep_dropoff_datetime', T.TimestampType()),
     T.StructField("PULocationID", T.IntegerType()),
     T.StructField("DOLocationID", T.IntegerType()),
     T.StructField("passenger_count", T.IntegerType()),
     T.StructField("trip_distance", T.FloatType()),
     ])