import os

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateExternalTableOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
# BUCKET = os.environ.get("GCP_GCS_BUCKET")
BUCKET = os.environ.get("GCP_GCS_BUCKET", "ukraine_tweet_data_bucket")

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", "ukraine_tweets_all")

DATASET = "tripdata"
COLOUR_RANGE = {
    "yellow": "tpep_pickup_datetime",
    "green": "lpep_pickup_datetime",
}
INPUT_PART = "raw"
INPUT_FILETYPE = "parquet"

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="gcs_2_bq_dag",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=["tweets"],
) as dag:
    # for colour, ds_col in COLOUR_RANGE.items():
    # colour = "black"
    # ds_col = "ltet"

    # move_files_gcs_task = GCSToGCSOperator(
    #     # task_id=f"move_{colour}_{DATASET}_files_task",
    #     task_id=f"move_tweet_files_task",
    #     source_bucket=BUCKET,
    #     # source_object=f"{INPUT_PART}/{colour}_{DATASET}*.{INPUT_FILETYPE}",
    #     source_object=f"raw/*.csv.gzip",
    #     destination_bucket=BUCKET,
    #     # destination_object=f"{colour}/{colour}_{DATASET}",
    #     destination_object=f"parquet",
    #     move_object=True,
    # )

    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id=f"bq_ukraine_tweets_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": f"all_ukraine_tweets_external_table",
            },
            "externalDataConfiguration": {
                "autodetect": "True",
                "sourceFormat": INPUT_FILETYPE,
                "sourceUris": [f"gs://{BUCKET}/parquet/*"],
            },
        },
    )

    CREATE_BQ_TBL_QUERY = f"""\
        CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.all_ukraine_tweets_partitioned \
        PARTITION BY language \
        AS \
        SELECT * FROM {BIGQUERY_DATASET}.all_ukraine_tweets_external_table; \
    """

    # Create a partitioned table from external table
    bq_create_partitioned_table_job = BigQueryInsertJobOperator(
        task_id=f"bq_create_partitioned_table_task",
        configuration={
            "query": {
                "query": CREATE_BQ_TBL_QUERY,
                "useLegacySql": False,
            }
        },
    )

    (
        # move_files_gcs_task
        # >>
        bigquery_external_table_task
        >> bq_create_partitioned_table_job
    )
