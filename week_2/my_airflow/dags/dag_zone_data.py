import logging
import os

import pyarrow.csv as pv
import pyarrow.parquet as pq
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateExternalTableOperator,
)
from airflow.utils.dates import days_ago
from google.cloud import storage


PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", "trips_data_all")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

# Single url/file/table, no jinja template needed
DATASET_URL = "https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv"
DL_FILE = "taxi_zone_lookup.csv"
OUTPUT_FILE = "taxi_zone_lookup.parquet"
TABLE_NAME = "taxi_zone_lookup"


def format_to_parquet(src_file):
    if not src_file.endswith(".csv"):
        logging.error(
            "Can only accept source files in CSV format, for the moment"
        )
        return

    logging.info("pv (pyarrow csv) read_csv call")
    table = pv.read_csv(src_file)
    logging.info("pq (pyarrow parquet) write_table call")
    pq.write_table(table, src_file.replace(".csv", ".parquet"))


# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


default_args = dict(
    start_date=days_ago(1),
    retries=1,
)

dag_workflow = DAG(
    dag_id="zone_data_ingestion_gcs_dag",
    schedule_interval="@once",
    catchup=True,
    default_args=default_args,
)

with dag_workflow:
    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"curl -sSLf {DATASET_URL} > {AIRFLOW_HOME}/{DL_FILE}",
    )

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{AIRFLOW_HOME}/{DL_FILE}",
        },
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{OUTPUT_FILE}",
            "local_file": f"{AIRFLOW_HOME}/{OUTPUT_FILE}",
        },
    )

    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": TABLE_NAME,
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/{OUTPUT_FILE}"],
            },
        },
    )

    (
        download_dataset_task
        >> format_to_parquet_task
        >> local_to_gcs_task
        >> bigquery_external_table_task
    )
