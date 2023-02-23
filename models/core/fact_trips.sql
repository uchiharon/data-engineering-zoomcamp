{{config(materialized='table')}}

with green_data as (
    select *,
    'Green' as service_type
    from {{ref('stg_green_tripdata')}}
),

yellow_data as (
    select *,
    'Yellow' as service_type
    from {{ref('stg_yellow_tripdata')}}
),

trip_unioned as (
    select * from green_data
    union all
    select * from yellow_data
),

dim_zone as (
    select * from {{ref('dim_zone')}}
    where borough != 'Unknown'
)

select trip_unioned.tripid,
    trip_unioned.vendorid,
    trip_unioned.service_type,
    trip_unioned.ratecodeid,
    trip_unioned.pickup_locationid,
    pickup_zone.borough as pickup_borough,
    pickup_zone.zone as pickup_zone,
    trip_unioned.dropoff_locationid,
    dropoff_zone.borough as dropoff_borough,
    dropoff_zone.zone as dropoff_zone,
    trip_unioned.pickup_datetime,
    trip_unioned.dropoff_datetime,
    trip_unioned.store_and_fwd_flag,
    trip_unioned.passenger_count,
    trip_unioned.trip_distance,
    trip_unioned.trip_type,
    trip_unioned.fare_amount,
    trip_unioned.extra,
    trip_unioned.mta_tax,
    trip_unioned.tip_amount,
    trip_unioned.tolls_amount,
    trip_unioned.ehail_fee,
    trip_unioned.improvement_surcharge,
    trip_unioned.total_amount,
    trip_unioned.payment_type,
    trip_unioned.get_payment_type as payment_type_description,
    trip_unioned.congestion_surcharge

from trip_unioned
inner join dim_zone as pickup_zone
on trip_unioned.pickup_locationid = pickup_zone.locationid
inner join dim_zone as dropoff_zone
on trip_unioned.dropoff_locationid = dropoff_zone.locationid


