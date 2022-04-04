from glob import glob
import time
import os
import csv
import pathlib
import zipfile
import subprocess
from google.cloud import storage
import gzip
import shutil
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

BUCKET_NAME = "ukraine_tweet_data_bucket"
KAGGLE_DATASET_NAME = (
    "bwandowando/ukraine-russian-crisis-twitter-dataset-1-2-m-rows"
)


def main():
    # Download full dataset
    fdl_fpath = "ukraine-russian-crisis-twitter-dataset-1-2-m-rows.zip"
    if not Path(fdl_fpath).exists():
        download_full_dataset()

    # Unzip
    # print(f"Unzipping file {fdl_fpath}")
    # with zipfile.ZipFile(fdl_fpath, "r") as zip_ref:
    #     zip_ref.extractall()

    # Globbing files
    files = sorted(glob("*.csv.gzip"))
    print("Files in dataset:")
    [print(f) for f in files]
    print("-" * 50)

    for idx, file_name in enumerate(files):
        if idx != 8 :
            continue

        print(f"({idx + 1} out of {len(files)}) Processing file '{file_name}'")
        start = time.time()
        destination_blob_name = f"raw/{file_name}"

        # print(f"Downloading file '{file_name}' from kaggle.")
        # dl_fpath = download_kaggle_dataset_file(KAGGLE_DATASET_NAME, file_name)
        # print(f"Unzipping file {dl_fpath}")
        # with zipfile.ZipFile(dl_fpath, "r") as zip_ref:
        #     zip_ref.extractall()
        # print(f"Removing file {dl_fpath}")
        # os.remove(dl_fpath)

        print("Converting to parquet")
        pq_fpath = convert_to_parquet(file_name)

        print(f"Uploading .csv.gzip blob to gcloud '{file_name}'")
        s = time.time()
        upload_blob(BUCKET_NAME, file_name, destination_blob_name)
        print(f"\ttook: {time.time() - s} secs")

        print(f"Uploading .parquet blob to gcloud '{pq_fpath}'")
        s = time.time()
        upload_blob(BUCKET_NAME, pq_fpath, f"parquet/{pq_fpath}")
        print(f"\ttook: {time.time() - s} secs")

        print(f"Processing file {file_name} took {time.time() - start} secs")
        print("-" * 50)


def download_full_dataset():
    cmd = "kaggle datasets download -d bwandowando/ukraine-russian-crisis-twitter-dataset-1-2-m-rows"
    output = subprocess.run(cmd, shell=True, check=True)
    print(output)


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


def download_kaggle_dataset_file(kaggle_dataset_name: str, file_name: str):
    cmd = f'kaggle datasets download --file "{file_name}" {kaggle_dataset_name}'
    output = subprocess.run(cmd, shell=True, check=True)
    print(output)
    return file_name + ".zip"


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


def check_file_exists(bucket_name: str, destination_blob_name: str):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    if not bucket.exists():
        raise Exception("Bucket does not exist.")
    bl = bucket.get_blob(destination_blob_name)
    return bool(bl)


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
    # storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    # storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._MAX_MULTIPART_SIZE = 50 * 1024 * 1024
    storage.blob._DEFAULT_CHUNKSIZE = 50 * 1024 * 1024

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


if __name__ == "__main__":
    main()
