-- Q1) Count fhv trips in 2019
SELECT count(*) FROM `massive-tuner-338911.trips_data_all.fhv_tripdata_external_table`;

-- Q2) Count fhv trips in 2019 with `dispatching_base_num`
SELECT count(DISTINCT dispatching_base_num) FROM `massive-tuner-338911.trips_data_all.fhv_tripdata_external_table`;

-- Q4) Creates daily partition by default
CREATE OR REPLACE TABLE `massive-tuner-338911.trips_data_all.fhv_tripdata_partitioned_clustered`
PARTITION BY DATE(dropoff_datetime)
CLUSTER BY dispatching_base_num AS
SELECT * FROM `massive-tuner-338911.trips_data_all.fhv_tripdata_external_table` ;

select count(*) from
`massive-tuner-338911.trips_data_all.fhv_tripdata_partitioned_clustered`
WHERE
DATE(dropoff_datetime) BETWEEN '2019-01-01' AND '2019-03-31'
AND
dispatching_base_num IN ('B00987', 'B02060', 'B02279');

-- Q5) 
SELECT SR_Flag FROM `massive-tuner-338911.trips_data_all.fhv_tripdata_external_table` group by SR_Flag;
SELECT count(distinct SR_Flag) FROM `massive-tuner-338911.trips_data_all.fhv_tripdata_external_table`;
