import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage
import json


def main():
    df = extract_tags_and_descriptions()
    upload(df)
    

def extract_tags_and_descriptions():
    dist_lang_path = "/Users/sebas/code/de-zoomcamp/week-7-8-9/ukraine-tweets/data/language_tags/distinct_languages.json"
    lang_subtag_reg = "/Users/sebas/code/de-zoomcamp/week-7-8-9/ukraine-tweets/data/language_tags/language-subtag-registry.txt"
    
    with open(dist_lang_path) as fp:
        lang_tags = json.load(fp)
        
    subtags_and_description = []
    pws = False
    with open(lang_subtag_reg) as fp:
        for line in fp.readlines():
            if "Subtag" in line:
                pws = True
                subtags_and_description.append(line)
            elif "Description" in line and pws:
                subtags_and_description.append(line)
                pws = False

    res = []
    for st, ds in zip(subtags_and_description[0::2], subtags_and_description[1::2]):
        tag = st.split()[1]
        desc = ds.split()[1] 
        res.append((tag, desc))
        # if tag in ["cbk", "und", "it", "en"]:
        #     print(tag, desc)

    x = set(lang_tags).issubset(set(i[0] for i in res)) 
    assert x, "Expected lang_tags to be subset of subtags from reg!"

    df = pd.DataFrame(res, columns=["tag", "description"])
    # print(df)
    return df


def upload(df: pd.DataFrame):
    pq_fpath =  "language_tags.parquet"

    # Create pyarrow table
    table = pa.Table.from_pandas(df)
    pq.write_table(table, pq_fpath)

    # Upload to gcloud 
    bucket_name = "ukraine_tweet_data_bucket"
    destination_blob_name = "language_tags_bcp_47.parquet"
    source_file_name = pq_fpath
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


if __name__ == "__main__":
    main()

