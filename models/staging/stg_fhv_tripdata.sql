{{config(materialized='view')}}		
	

select

-- identifiers
    cast(Affiliated_base_number as STRING) as affiliated_base_number,
    cast(dispatching_base_num as STRING) as dispatching_base_num,
    cast(PULocationID	as INTEGER) as pickup_locationid,			
    cast(DOLocationID	as INTEGER) as	dropoff_locationid,	

-- timestamps
    
    cast(pickup_datetime as	TIMESTAMP) as pickup_datetime,			
    cast(dropOff_datetime as	TIMESTAMP) as dropoff_datetime,

-- trip info  
    SR_Flag as sr_flag,


from {{source('staging','fhv_2019_nonpartitioned')}}


-- dbt build -m <model.sql> --var 'is_test_run: false'

{% if var('is_test_run', default=true) %}

limit 200

{% endif %}