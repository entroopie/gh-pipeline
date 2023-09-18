import pandas as pd
from prefect import flow, task
from prefect_gcp import GcsBucket
import os
from pathlib import Path
import json

@task(retries=3)
def fetch_from_web(year: int, month: int, day: int, hour: int) -> pd.DataFrame:

    dataset_url = f"https://data.gharchive.org/{year}-{month:02}-{day:02}-{hour}.json.gz"    
    header = {'User-Agent': 'pandas'}
    return pd.read_json(dataset_url, storage_options=header, lines=True, compression="gzip")

@task()
def clean(df: pd.DataFrame) -> pd.DataFrame:
    
    df["type"] = df["type"].astype("string")
    df["id"] = df["id"].astype("Int64")
    df["payload"] = df["payload"].apply(lambda x: json.dumps(x)).astype("string")
    df["created_at"] = pd.to_datetime(df["created_at"])

    df["year"] = df["created_at"].apply(lambda x: x.year)
    df["month"] = df["created_at"].apply(lambda x: x.month)
    df["day"] = df["created_at"].apply(lambda x: x.day)

    print(len(df))
    print(df.dtypes)

    return df

@task()
def write_locally(df: pd.DataFrame, path: Path) -> None:
    df.to_parquet(path, engine="pyarrow", compression="gzip")

@task()
def write_gcs(path: Path, gcp_bucket_name: str) -> None:
    gcs_bucket = GcsBucket.load(gcp_bucket_name)    
    gcs_bucket.upload_from_path(
        from_path = path.as_posix(),
        to_path = path.as_posix(),
    )
    return

@flow(log_prints=True)
def ingest(year: int, months: list[int], days: list[int], hours: list[int], gcp_bucket_name: str) -> None:
    
    for month in months:
        for day in days:
            for hour in hours:
                file = f"{year}-{month:02}-{day:02}-{hour}.parquet.gz"
                dirpath = Path(f"gh_data/{year}/{month:02}/{day:02}/")

                if not os.path.exists(dirpath):
                    os.makedirs(dirpath, exist_ok=True)
                    
                full_path = Path(os.path.join(dirpath, file))

                df = fetch_from_web(year, month, day, hour)
                df_clean = clean(df)
                write_locally(df_clean, full_path)
                write_gcs(full_path, gcp_bucket_name)

if __name__ == "__main__":
    year = 2015
    months = [4]
    days = [7]
    hours = list(range(0, 23))
    gcp_bucket_name = "test1"
    ingest(year, months, days, hours, gcp_bucket_name)