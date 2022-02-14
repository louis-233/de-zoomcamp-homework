## Homework
[Form](https://forms.gle/ytzVYUh2RptgkvF79)  
We will use all the knowledge learned in this week. Please answer your questions via form above.  
**Deadline** for the homework is 14th Feb 2022 17:00 CET.

### Question 1: 
**What is count for fhv vehicles data for year 2019**  
Can load the data for cloud storage and run a count(*)

42_084_899

### Question 2: 
**How many distinct dispatching_base_num we have in fhv for 2019**  
Can run a distinct query on the table from question 1

792

### Question 3: 
**Best strategy to optimise if query always filter by dropoff_datetime and order by dispatching_base_num**  
Review partitioning and clustering video.   
We need to think what will be the most optimal strategy to improve query 
performance and reduce cost.

* We should create a `daily` sized partition on `dropoff_datetime`
    * If we always query `dropoff_datetime` still will speed up querying and
      cost because less data will be queried, i.e. only data from the partition
      which date we're querying/filtering on.

* Secondly we should cluster on `dispatching_base_num` as well to make the
  ordering of this always the same, within the partitions.

#### My notes

* `dropoff_datetime` has a resolution of seconds, e.g. `2019-09-01 00:59:00 UTC`
So I guess it is not easily partitioned or clustered? Perhaps per day, but not
per second.
    * `daily` sized partition because our data is evenly spaced.
* partitions limited to 4000, so if hourly than we want an expiry strategy
* ---------------------------------
* clustering ordering matters: clustering col_a -> col_b then we sort first on
  col_a and then on col_b 
* can specify 4 columns to cluster on
* clustering: improves filter and aggregate queries
* ---------------------------------
* Ordering by `dis...` probably means we want to cluster on this.


### Question 4: 
**What is the count, estimated and actual data processed for query which counts trip between 2019/01/01 and 2019/03/31 for dispatching_base_num B00987, B02060, B02279**  
Create a table with optimized clustering and partitioning, and run a 
count(*). Estimated data processed can be found in top right corner and
actual data processed can be found after the query is executed.

select count(*) from
`massive-tuner-338911.trips_data_all.fhv_tripdata_partitioned_clustered`
WHERE
DATE(dropoff_datetime) BETWEEN '2019-01-01' AND '2019-03-31';
AND
dispatching_base_num IN ('B00987', 'B02060', 'B02279');

estimate: This query will process 400.1 MiB when run.
actual: 134.9 MB
count: 26643


### Question 5: 
**What will be the best partitioning or clustering strategy when filtering on dispatching_base_num and SR_Flag**  
Review partitioning and clustering video. 
Partitioning cannot be created on all data types.

We can only partition on datetime or integer columns so:
* we can partition on the integer column `SR_Flag`
    * this also has only 43 distinct value so well within the limit of 4000 partitions
* we cannot partition on `dispatching_base_num` (column is string categorical)
    * we can use clustering for this one.

### Question 6: 
**What improvements can be seen by partitioning and clustering for data size less than 1 GB**  
Partitioning and clustering also creates extra metadata.  
Before query execution this metadata needs to be processed.

For clustered data no significant difference in query performance can be seen.
They might incur more cost as well, because partition and cluster metadata has
to been written (to keep it up to date).


### (Not required) Question 7: 
**In which format does BigQuery save data**  
Review big query internals video.

In cheap columnar storage called Colossus.