{{config(materialized='view')}}

with tripdata as (
    SELECT *, row_number() over(partition by cast(VendorID as int), tpep_pickup_datetime) as rn
from {{source('staging','yellow_tripdata')}}  where VendorID is not null
)

select

-- identifiers

    {{ dbt_utils.generate_surrogate_key(['VendorID','tpep_pickup_datetime']) }} as tripid,
    cast(VendorID as INTEGER) as vendorid,
    cast(PULocationID	as INTEGER) as pickup_locationid,			
    cast(DOLocationID	as INTEGER) as	dropoff_locationid,	
    cast(RatecodeID	as INTEGER) as ratecodeid,

-- timestamps
    
    cast(tpep_pickup_datetime as	TIMESTAMP) as pickup_datetime,			
    cast(tpep_dropoff_datetime as	TIMESTAMP) as dropoff_datetime,

-- trip info  
    store_and_fwd_flag,
    cast(passenger_count as	INTEGER) as		passenger_count	,	
    cast(trip_distance as	numeric) as	trip_distance,
-- add part
    1 as trip_type,

-- payment info

    cast(payment_type as INTEGER) as payment_type,

    {{ get_payment_type_description('payment_type') }} as get_payment_type,	

    cast(fare_amount as numeric) as fare_amount,			
    cast(extra as numeric) as extra	,		
    cast(mta_tax as numeric) as mta_tax	,		
    cast(tip_amount as numeric) as tip_amount,
    cast(tolls_amount as numeric) as tolls_amount,			
    cast(improvement_surcharge as numeric) as improvement_surcharge,		
    cast(total_amount as numeric) as total_amount,		
    cast(congestion_surcharge as numeric) as congestion_surcharge,
    0  as ehail_fee


from tripdata
where rn = 1

-- dbt build -m <model.sql> --var 'is_test_run: false'

{% if var('is_test_run', default=true) %}

limit 200

{% endif %}