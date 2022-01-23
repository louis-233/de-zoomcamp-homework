select * from yellow_taxi_data limit 10;

-- Question 3: Count records *
-- all trips on 15 January:
select count(*) 
from yellow_taxi_data as ytd
where
EXTRACT(DAY FROM ytd.tpep_pickup_datetime) = '15' 
and
EXTRACT(MONTH FROM ytd.tpep_pickup_datetime) = '01';

-- Question 4: Largest tip for each day *
-- On which day it was the largest tip in January? (note: it's not a typo, it's "tip", not "trip")
select tpep_pickup_datetime, tpep_dropoff_datetime,tip_amount from yellow_taxi_data
where
EXTRACT(MONTH FROM tpep_pickup_datetime) = '01' 
order by tip_amount desc
limit 10
;

-- Question 5: Most popular destination 
-- What was the most popular destination for passengers picked up in central park on January 14?
-- 		Enter the zone name (not id). If the zone name is unknown (missing), write "Unknown"
select
	-- 1. Things we [must] GROUP BY
	concat(zpu."Borough",' / ', zpu."Zone") as pu_loc
	,concat(zdo."Borough",' / ', zdo."Zone") as do_loc
	,CAST(tpep_dropoff_datetime AS DATE) as do_day
	-- 2. Our AGGREGATES over the group by's
	,count(1) as drop_offs
from
	yellow_taxi_data as ytd
	JOIN zones as zpu
		ON ytd."PULocationID" = zpu."LocationID" 
	JOIN zones as zdo
		ON ytd."DOLocationID" = zdo."LocationID"
where
	zpu."Zone" = 'Central Park'
	and CAST(ytd.tpep_dropoff_datetime AS DATE) = '2021-01-14'
group by
	do_loc
	,CAST(ytd.tpep_dropoff_datetime AS DATE)
	,pu_loc
order by
 drop_offs desc
;

-- Question 6: Most expensive route *
-- What's the pickup-dropoff pair with the largest average price for a ride
-- (calculated based on total_amount)?
-- Enter two zone names separated by a slashFor example: "Jamaica Bay / Clinton East"
-- If any of the zone names are unknown (missing), write "Unknown". For example, "Unknown / Clinton East".
select
	-- 1. Things/cols we [must] GROUP BY
	concat(zpu."Borough",' / ', zpu."Zone") as pu_loc
	,concat(zdo."Borough",' / ', zdo."Zone") as do_loc
	-- 2. Our AGGREGATES over the group by's
	,avg(ytd.total_amount) as my_avg
from
	yellow_taxi_data as ytd
	JOIN zones as zpu
		ON ytd."PULocationID" = zpu."LocationID" 
	JOIN zones as zdo
		ON ytd."DOLocationID" = zdo."LocationID"
group by
	do_loc
	,pu_loc
order by
 my_avg desc
;
