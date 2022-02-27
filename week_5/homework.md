## Week 5 Homework

In this homework we'll put what we learned about Spark
in practice.

We'll use high volume for-hire vehicles (HVFHV) dataset for that.

## Question 1. Install Spark and PySpark

* Install Spark
* Run PySpark
* Create a local spark session 
* Execute `spark.version`

What's the output?

### answer

```py
spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()
spark.version
```

`'3.2.1'`


## Question 2. HVFHW February 2021

Download the HVFHV data for february 2021:

```bash
wget https://nyc-tlc.s3.amazonaws.com/trip+data/fhvhv_tripdata_2021-02.csv
```

Read it with Spark using the same schema as we did 
in the lessons. We will use this dataset for all
the remaining questions.

Repartition it to 24 partitions and save it to
parquet.

What's the size of the folder with results (in MB)?

### answer

```py
from pyspark.sql import types

schema = types.StructType([
    types.StructField('hvfhs_license_num', types.StringType(), True),
    types.StructField('dispatching_base_num', types.StringType(), True),
    types.StructField('pickup_datetime', types.TimestampType(), True),
    types.StructField('dropoff_datetime', types.TimestampType(), True),
    types.StructField('PULocationID', types.IntegerType(), True),
    types.StructField('DOLocationID', types.IntegerType(), True),
    types.StructField('SR_Flag', types.StringType(), True)
])

df = spark.read \
    .option("header", "true") \
    .schema(schema) \
    .csv('fhvhv_tripdata_2021-02.csv')

df = df.repartition(24)

df.write.parquet('fhvhv/2021/02/')
```

Next I looked up the size of the `fhvhv/2021/02` directory in the file explorer:

* `du -h` -> `221`
* `finder -> get info` -> `218.4 MB`
* `finder -> get info -> size on disk` -> `231.2 MB`

Each part is `9.0MB` when using `ls -lah` in the `fhvhv/2021/02` directory.
`9` (file size per partition) times `24` (number of paritions) is `216MB`.


## Question 3. Count records 

How many taxi trips were there on February 15?

Consider only trips that started on February 15.

### answer

```py
from pyspark.sql.functions import dayofmonth

df = spark.read.parquet('fhvhv/2021/02/')
df.filter(dayofmonth(df.pickup_datetime) == 15).count()
```

count is `367170`


## Question 4. Longest trip for each day

Now calculate the duration for each trip.

Trip starting on which day was the longest? 


### answer

```py
from pyspark.sql.functions import desc, unix_timestamp

df\
.withColumn(
    "duration",
    unix_timestamp(df.dropoff_datetime) -  unix_timestamp(df.pickup_datetime)
)\
.orderBy(desc("duration"))\
.limit(1).show()
```

the longest trip:

```txt
+-----------------+--------------------+-------------------+-------------------+------------+------------+-------+--------+
|hvfhs_license_num|dispatching_base_num|    pickup_datetime|   dropoff_datetime|PULocationID|DOLocationID|SR_Flag|duration|
+-----------------+--------------------+-------------------+-------------------+------------+------------+-------+--------+
|           HV0005|              B02510|2021-02-11 13:40:44|2021-02-12 10:39:44|         247|          41|   null|   75540|
+-----------------+--------------------+-------------------+-------------------+------------+------------+-------+--------+
```

75540 seconds long trip on February 11th.

## Question 5. Most frequent `dispatching_base_num`

Now find the most frequently occurring `dispatching_base_num` 
in this dataset.

How many stages this spark job has?

> Note: the answer may depend on how you write the query,
> so there are multiple correct answers. 
> Select the one you have.


### answer

```py
df.groupBy(df.dispatching_base_num).count().orderBy(desc("count")).limit(5).show()
```

```txt
+--------------------+-------+
|dispatching_base_num|  count|
+--------------------+-------+
|              B02510|3233664|
|              B02764| 965568|
|              B02872| 882689|
|              B02875| 685390|
|              B02765| 559768|
+--------------------+-------+
```

dispatching base num `B02510` is most frequently occurring.

it has `1` stage for me, but it has `2` jobs though. Not sure why.


## Question 6. Most common locations pair

Find the most common pickup-dropoff pair. 

For example:

"Jamaica Bay / Clinton East"

Enter two zone names separated by a slash

If any of the zone names are unknown (missing), use "Unknown". For example, "Unknown / Clinton East". 


### answer

#### Write zones to partitioned parquet files

```py
from pyspark.sql import types

schema = types.StructType([
    types.StructField('LocationID', types.IntegerType(), True),
    types.StructField('Borough', types.StringType(), True),
    types.StructField('Zone', types.StringType(), True),
    types.StructField('service_zone', types.StringType(), True),
])
df = spark.read \
    .option("header", "true") \
    .schema(schema) \
    .csv('taxi+_zone_lookup.csv')
df = df.repartition(4)
df.write.parquet('taxi_zones/')
```

#### Joining zones and hvfhv 2021-02

```py
from pyspark.sql.functions import col, lit, concat

df_zones = df = spark.read.parquet('taxi_zones/')
df = spark.read.parquet('fhvhv/2021/02/')

df\
.join(df_zones.alias("zones_pickup"), df.PULocationID == col("zones_pickup.LocationID"), "left")\
.join(df_zones.alias("zones_dropoff"), df.DOLocationID == col("zones_dropoff.LocationID"), "left")\
.withColumn("PU / DO Zone", concat(col("zones_pickup.Zone"), lit(" / "), col("zones_dropoff.Zone")) )\
.groupBy(col("PU / DO Zone"))\
.count()\
.select("count", col("PU / DO Zone"))\
.orderBy(desc(col("count")))\
.show(20, False)
```

#### Output

```txt
+-----+-----------------------------------------------------+
|count|PU / DO Zone                                         |
+-----+-----------------------------------------------------+
|45041|East New York / East New York                        |
|37329|Borough Park / Borough Park                          |
|28026|Canarsie / Canarsie                                  |
|25976|Crown Heights North / Crown Heights North            |
|17934|Bay Ridge / Bay Ridge                                |
|14688|Astoria / Astoria                                    |
|14688|Jackson Heights / Jackson Heights                    |
|14481|Central Harlem North / Central Harlem North          |
|14424|Bushwick South / Bushwick South                      |
|13976|Flatbush/Ditmas Park / Flatbush/Ditmas Park          |
|13716|South Ozone Park / South Ozone Park                  |
|12829|Brownsville / Brownsville                            |
|12542|JFK Airport / NA                                     |
|11814|Prospect-Lefferts Gardens / Crown Heights North      |
|11548|Forest Hills / Forest Hills                          |
|11491|Bushwick North / Bushwick South                      |
|11487|Bushwick South / Bushwick North                      |
|11462|Crown Heights North / Prospect-Lefferts Gardens      |
|11342|Crown Heights North / Stuyvesant Heights             |
|11308|Prospect-Lefferts Gardens / Prospect-Lefferts Gardens|
+-----+-----------------------------------------------------+
```

Most common pickup-dropoff pair: `East New York / East New York`



## Bonus question. Join type

(not graded) 

For finding the answer to Q6, you'll need to perform a join.

What type of join is it?

And how many stages your spark job has?

### answer

It is a left join.
With an outer join we would get 'additional' rows for all location IDs that do
not show up in the hvfhv 2021-02 dataset, which we do not want

For me the above defined spark call runs 3 jobs with each 1 stage (the last job has 2 stages but 1 is skipped)
```txt
215	
showString at <unknown>:0
showString at <unknown>:0	2022/02/27 15:53:01	42 ms	1/1 (1 skipped)	
8/8 (8 skipped)

214	
showString at <unknown>:0
showString at <unknown>:0	2022/02/27 15:52:59	1 s	1/1	
8/8

213 (2d59308d-3e2c-4e34-b3a4-7cc440315223)	
broadcast exchange (runId 2d59308d-3e2c-4e34-b3a4-7cc440315223)
$anonfun$withThreadLocalCaptured$1 at FutureTask.java:266	2022/02/27 15:52:59	9 ms	1/1	
4/4
```

I don't remember seeing this in the lecture videos, not sure if this is expected behavior.


## Submitting the solutions

* Form for submitting: https://forms.gle/dBkVK9yT8cSMDwuw7
* You can submit your homework multiple times. In this case, only the last submission will be used. 

Deadline: 02 March (Wednesday), 22:00 CET
