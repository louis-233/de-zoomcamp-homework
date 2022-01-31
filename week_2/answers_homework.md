## Question 1: Start date for the Yellow taxi data (1 point)

You'll need to parametrize the DAG we created in the videos. 

What should be the start date for this dag?

* 2019-01-01 **this one**
* 2020-01-01
* 2021-01-01
* days_ago(1)


## Question 2: Frequency (1 point)

How often do we need to run it?

* Daily
* Monthly **this one**
* Yearly
* Once


## Re-running the DAGs for past dates

If you have problems executing your DAG for past dates, try this:

* First, delete your DAG from the web interface (the bin icon)
* Set the `catchup` parameter to `True`
* Rename the DAG to something like `data_ingestion_gcs_dag_v02` 
* Execute it from the Airflow GUI (the play button)

Also, there's no data for the recent months, but `curl` will exit successfully.
To make it fail on 404, add the `-f` flag:

```bash
curl -sSLf { URL } > { LOCAL_PATH }
```


## Question 3: DAG for FHV Data (2 points)

Now create another DAG - for uploading the FHV data. 

We will need three steps: 

* Donwload the data
* Parquetize it 
* Upload to GSC

(Or Download -> Ingest for local ingestion)

Use the same frequency and the start date as for the yellow taxi dataset

Question: how many DAG runs are green after finishing everything? 

### ** My answer: **

yellow_taxi: 24
    * one for each month 2019 and 2020
FHV: 12
    * one for each month in 2019



## Question 4: DAG for Zones (2 points)


Create the final DAG - for Zones:

* Download it
* Parquetize 
* Upload to GCS

(Download -> Ingest for local ingestion)

How often does it need to run?

* Daily
* Monthly
* Yearly
* Once **I guess this one, let's double check**


## Submitting the solutions

* Form for submitting: TBA
* You can submit your homework multiple times. In this case, only the last submission will be used. 

Deadline: February 4, 22:00 CET 
