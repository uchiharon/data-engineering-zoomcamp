{{config(materialized='view')}}

select * from {{(source_data('staging','yellow_taxi_trips'))}}
limit 100