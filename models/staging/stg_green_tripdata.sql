{{config(materialized='view')}}

select * from {{source('staging','yellow_taxi_trips')}}
limit 100