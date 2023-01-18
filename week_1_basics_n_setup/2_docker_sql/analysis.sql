SELECT index, to_char(lpep_pickup_datetime,'MM-DD') pickup, to_char(lpep_dropoff_datetime,'MM-DD') dropoff
FROM green_taxi_trips 
WHERE to_char(lpep_pickup_datetime,'MM')='01-15' 
LIMIT 100


SELECT COUNT(*) FROM (SELECT *
FROM green_taxi_trips 
WHERE to_char(lpep_pickup_datetime,'MM-DD')='01-15')sub
WHERE to_char(lpep_dropoff_datetime,'MM-DD')='01-15'


SELECT to_char(lpep_pickup_datetime,'YYYY-MM-DD') pickup, MAX(trip_distance) total_distance
FROM green_taxi_trips
GROUP BY to_char(lpep_pickup_datetime,'YYYY-MM-DD')
ORDER BY total_distance DESC

SELECT passenger_count, COUNT(*)
FROM green_taxi_trips
WHERE to_char(lpep_pickup_datetime,'YYYY-MM-DD')='2019-01-01'
GROUP BY passenger_count

SELECT * FROM green_taxi_trips LIMIT 100
SELECT * FROM zones LIMIT 100

SELECT dropoff_zone, MAX(tip_amount) FROM (SELECT gt.*, z1."Zone" pickup_zone, z."Zone" dropoff_zone
FROM green_taxi_trips gt
JOIN zones z
ON gt."DOLocationID" = z."LocationID"
JOIN zones z1
ON gt."PULocationID" = z1."LocationID")sub
WHERE pickup_zone='Astoria'
GROUP BY dropoff_zone
ORDER BY MAX(tip_amount) DESC



