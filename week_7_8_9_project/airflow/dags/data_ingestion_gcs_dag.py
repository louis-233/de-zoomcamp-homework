import os
import csv
import subprocess
import zipfile

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateExternalTableOperator,
)
import pyarrow.csv as pv
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET", "ukraine_tweet_data_bucket")
KAGGLE_DATASET_NAME = (
    "bwandowando/ukraine-russian-crisis-twitter-dataset-1-2-m-rows"
)
PATH_AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", "trips_data_all")


# def format_to_parquet(src_file):
#     if not src_file.endswith(".csv"):
#         logging.error(
#             "Can only accept source files in CSV format, for the moment"
#         )
#         return
#     table = pv.read_csv(src_file)
#     pq.write_table(table, src_file.replace(".csv", ".parquet"))


# # NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
# def upload_to_gcs(bucket, object_name, local_file):
#     """
#     Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
#     :param bucket: GCS bucket name
#     :param object_name: target path & file-name
#     :param local_file: source path & file-name
#     :return:
#     """
#     # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
#     # (Ref: https://github.com/googleapis/python-storage/issues/74)
#     storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
#     storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
#     # End of Workaround

#     client = storage.Client()
#     bucket = client.bucket(bucket)

#     blob = bucket.blob(object_name)
#     blob.upload_from_filename(local_file)


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}


def get_kaggle_datasets_file_names(kaggle_dataset_name: str) -> list[str]:
    cmd = f"kaggle datasets files {kaggle_dataset_name}"
    output = subprocess.run(
        cmd,
        shell=True,
        check=True,
        capture_output=True,
    )
    output_str = output.stdout.decode("utf-8")
    files = [
        line.split(" ")[0] for line in output_str.splitlines() if ".csv" in line
    ]
    return files


def download_kaggle_dataset_file(kaggle_dataset_name: str, file_name: str):
    cmd = f'kaggle datasets download --file "{file_name}" {kaggle_dataset_name}'
    output = subprocess.run(cmd, shell=True, check=True)
    print("Download kaggle dataset file output:", output)
    return file_name + ".zip"


def unzip_dataset_file(dl_fpath):
    print(f"Unzipping file {dl_fpath}")
    with zipfile.ZipFile(dl_fpath, "r") as zip_ref:
        zip_ref.extractall()
    print(f"Removing zip file {dl_fpath}")
    os.remove(dl_fpath)


def convert_to_parquet(gzip_csv_fpath: str) -> str:
    df = pd.read_csv(
        gzip_csv_fpath,
        compression="gzip",
        index_col=0,
        encoding="utf-8",
        quoting=csv.QUOTE_ALL,
    )
    # Convert to datetime (from string)
    df.usercreatedts = pd.to_datetime(df.usercreatedts)
    df.extractedts = pd.to_datetime(df.extractedts)
    df.tweetcreatedts = pd.to_datetime(df.tweetcreatedts)
    table = pa.Table.from_pandas(df)
    pq_fpath = gzip_csv_fpath.replace(".csv.gzip", ".parquet")
    pq.write_table(table, pq_fpath)
    return pq_fpath


def upload_blob(
    bucket_name: str,
    source_file_name: str,
    destination_blob_name: str,
):
    """Uploads a file to the bucket."""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"
    # The path to your file to upload
    # source_file_name = "local/path/to/file"
    # The ID of your GCS object
    # destination_blob_name = "storage-object-name"

    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    print("Creating blob object")
    blob = bucket.blob(destination_blob_name)
    print("Uploading from filename")
    blob.upload_from_filename(source_file_name)
    print(
        "File {} uploaded to {}.".format(
            source_file_name, destination_blob_name
        )
    )


with DAG(
    dag_id="data_ingestion_gcs_dag",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=["ukraine-tweets"],
) as dag:
    for file_name in get_kaggle_datasets_file_names(KAGGLE_DATASET_NAME):
        cmd = f"""\
            kaggle datasets download \
                --file "{file_name}" \
                --path "{PATH_AIRFLOW_HOME}" \
                {KAGGLE_DATASET_NAME}
        """
        download_dataset_task = BashOperator(
            task_id="download_dataset_file_task",
            # bash_command=f"curl -sS {dataset_url} > {path_to_local_home}/{dataset_file}"
            bash_command=cmd,
        )

        unzip_dataset_file_task = PythonOperator(
            task_id="unzip_dataset_file_task",
            python_callable=unzip_dataset_file,
            op_kwargs={
                "dl_fpath": f"{PATH_AIRFLOW_HOME}/{file_name}.zip",
            },
        )

        local_raw_to_gcs_task = PythonOperator(
            task_id="local_to_gcs_task",
            python_callable=upload_blob,
            op_kwargs={
                "bucket_name": BUCKET,
                "source_file_name": file_name,
                "destination_blob_name": f"raw/{file_name}",
            },
        )

        conv_to_pq_task_id = "convert_to_parquet_task"

        convert_to_parquet_task = PythonOperator(
            task_id=conv_to_pq_task_id,
            python_callable=convert_to_parquet,
            op_kwargs={
                "gzip_csv_fpath": f"{PATH_AIRFLOW_HOME}/{file_name}",
            },
        )

        pq_file_name = (
            f"{PATH_AIRFLOW_HOME}/{file_name.replace('.csv.gzip', 'parquet')}"
        )
        local_parquet_to_gcs_task = PythonOperator(
            task_id="local_to_gcs_task",
            python_callable=upload_blob,
            op_kwargs={
                "bucket_name": BUCKET,
                "source_file_name": pq_file_name,
                "destination_blob_name": f"parquet/{pq_file_name}",
            },
        )

        # bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        #     task_id="bigquery_external_table_task",
        #     table_resource={
        #         "tableReference": {
        #             "projectId": PROJECT_ID,
        #             "datasetId": BIGQUERY_DATASET,
        #             "tableId": "external_table",
        #         },
        #         "externalDataConfiguration": {
        #             "sourceFormat": "PARQUET",
        #             "sourceUris": [f"gs://{BUCKET}/raw/{parquet_file}"],
        #         },
        #     },
        # )

        (
            download_dataset_task
            >> local_raw_to_gcs_task
            >> convert_to_parquet_task
            >> local_parquet_to_gcs_task
            # >> bigquery_external_table_task
        )
