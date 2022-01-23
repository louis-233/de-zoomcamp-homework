-- 1.
-- INNER JOIN yellow_taxi_data and zone tables
select
tpep_pickup_datetime,tpep_dropoff_datetime,"PULocationID","DOLocationID"
,concat(zpu."Borough",' / ', zpu."Zone") as "PickUpBorough/Zone"
,concat(zdo."Borough",' / ', zdo."Zone") as "DropOffBorough/Zone"
from
-- so we get all possible combinations of records from these tables:
yellow_taxi_data as ytd,
zones as zpu,
zones as zdo
where
-- and we limit them those records with this expression:
-- Only records where pickoff-loc-id same as as zpu's pickup-loc-id which results in 
-- 		just a single record within the set of all combinations of one ytd record with all 
--		records from zones/zpu, and if the locationID does exist in zpu we don't show the row, ?I guess,
-- 		bc there is no valid combination
-- this is called an inner join
ytd."PULocationID" = zpu."LocationID" 
and
ytd."DOLocationID" = zdo."LocationID"
limit 10;

-- 2.
-- INNER JOIN
-- using 'JOIN' keyword, same as above but written in different way (no real perf diff or anything, does the same thing)
select
tpep_pickup_datetime,tpep_dropoff_datetime,"PULocationID","DOLocationID"
,concat(zpu."Borough",' / ', zpu."Zone") as "PickUpBorough/Zone"
,concat(zdo."Borough",' / ', zdo."Zone") as "DropOffBorough/Zone"
from
yellow_taxi_data as ytd
JOIN zones as zpu
	ON ytd."PULocationID" = zpu."LocationID" 
JOIN zones as zdo
	ON ytd."DOLocationID" = zdo."LocationID"
limit 10;

-- 3
-- Because with the inner join we won't see any records where PULocationID does not exist in zones
-- it might be good to check this doesn't occur in our dataset
-- so PULocationID could be NULL or PULocationID could be an ID that doesn't exist in the zones table
-- (same goes for DOLocationID)
-- (if 0 records then the inner join isn't missing out on anything)
-- [if there was some missing data we'd use LEFT, RIGHT or OUTER JOIN to fix this (?somehow)]
select * from yellow_taxi_data
where
"PULocationID" is NULL or
"DOLocationID" is NULL; -- 0 records

select * from yellow_taxi_data
where "PULocationID" not in (select "LocationID" from zones)
or "DOLocationID" not in (select "LocationID" from zones); -- 0 records

-- [ytd] LEFT JOIN [zones]
-- show records from table on left if matching record for table on the right is missing then show record from left
-- table but show NULL for record of the right table
-- [ytd] RIGHT JOIN [zones]
-- show records from table on right if matching records for table on the left then
-- for the right tables's record it shows something but for the left tables's record it shows NULL
-- 'matching'= matching on the `ON` expression (or not)
-- [ytd] OUTER JOIN [zones]
-- combination of LEFT JOIN and RIGHT JOIN

-- 4.
-- GROUP BY
-- group records together, based on if they have the same value or perhaps a different condition
--		then form example perform an aggregate operation on the grouped column(s)
select
	-- DATE_TRUNC('DAY', tpep_dropoff_datetime), -- remove time
	CAST(tpep_dropoff_datetime AS DATE) as day -- remove time, alternative solution
	,count(1) as num_trips
	,max(total_amount)
	,max(passenger_count)
from
	yellow_taxi_data
group by
	day
order by
	num_trips desc
;

select
	CAST(tpep_dropoff_datetime AS DATE) as day
	,"DOLocationID" as dol
	,count(1) as num_trips
	,max(total_amount) as max_amount_monnies
	,max(passenger_count) as max_num_passengers
from
	yellow_taxi_data
group by
	day, dol -- !Grouping by multiple columns
order by
	day asc
	,dol asc
;


