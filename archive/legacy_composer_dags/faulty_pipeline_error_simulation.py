from datetime import datetime, timedelta
import json
import time
import pandas as pd
import requests
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.gcs import GCSHook

# Configuration / Flags to trigger failures intentionally
FAIL_API = True               # Simulates 404 / connection timeout
FAIL_RESOURCE = True          # Simulates OOM (Out of Memory)
FAIL_SCHEMA = True            # Simulates unexpected upstream schema changes
FAIL_LONG_QUERY = True        # Simulates a hung/long-running query hitting timeout

GCS_BUCKET = "my-analytics-raw-bucket"
GCS_OBJECT_PATH = "raw/api_posts.json"


@dag(
    dag_id="faulty_pipeline_error_simulation",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["testing", "chaos-engineering", "error-simulation"],
    default_args={
        "owner": "data_engineers",
        "retries": 1,
        "retry_delay": timedelta(seconds=10),
    },
)
def faulty_etl_pipeline():

    # -------------------------------------------------------------
    # LAYER 1: Ingestion Layer (GCS)
    # Error: API Failure (Broken endpoint / non-200 status)
    # -------------------------------------------------------------
    @task
    def ingest_to_gcs() -> str:
        # Toggleable target URL: broken endpoint simulates HTTP 404/500
        url = (
            "https://jsonplaceholder.typicode.com/non_existent_endpoint"
            if FAIL_API
            else "https://jsonplaceholder.typicode.com/posts"
        )

        print(f"Fetching data from: {url}")
        response = requests.get(url, timeout=10)

        # Triggers an HTTPError/AirflowException on bad response
        if response.status_code != 200:
            raise AirflowException(
                f"[API Failure] Upstream API call failed with status {response.status_code}: {response.text}"
            )

        raw_records = response.json()

        # Upload raw JSON to GCS
        gcs_hook = GCSHook()
        gcs_hook.upload(
            bucket_name=GCS_BUCKET,
            object_name=GCS_OBJECT_PATH,
            data=json.dumps(raw_records),
        )
        return GCS_OBJECT_PATH

    # -------------------------------------------------------------
    # LAYER 2: Data Cleaning & Processing Layer
    # Errors: Resource Exhaustion (OOM) & Schema Drift / Type Mismatch
    # -------------------------------------------------------------
    @task
    def clean_and_process_data(gcs_path: str) -> list[dict]:
        gcs_hook = GCSHook()
        file_content = gcs_hook.download(bucket_name=GCS_BUCKET, object_name=gcs_path)
        data = json.loads(file_content.decode("utf-8"))
        df = pd.DataFrame(data)

        # ERROR 1: RESOURCE EXHAUSTION (Memory Leak / OOM)
        if FAIL_RESOURCE:
            print("[Simulating Resource Exhaustion] Rapid memory expansion...")
            bloat = []
            # Exponentially allocates massive byte blocks to crash the worker container
            while True:
                bloat.append(bytearray(100 * 1024 * 1024))  # 100 MB per append

        # ERROR 2: SCHEMA DRIFT / COLUMN MISMATCH
        if FAIL_SCHEMA:
            print("[Simulating Schema Mismatch] Renaming upstream fields...")
            # Simulate upstream API removing or renaming 'userId' to 'account_id'
            if "userId" in df.columns:
                df = df.rename(columns={"userId": "account_id"})

        # Processing logic that expects strict contract: 'userId' must exist
        if "userId" not in df.columns:
            raise KeyError(
                "[Schema Change Error] Missing mandatory column 'userId'. "
                f"Actual schema received: {list(df.columns)}"
            )

        # Cleaning step
        df["cleaned_title"] = df["title"].str.strip().str.lower()
        return df[["userId", "id", "cleaned_title"]].to_dict(orient="records")

    # -------------------------------------------------------------
    # LAYER 3: Final Layer (Destination / Data Warehouse)
    # Error: Long-Running / Hanging Query (Breaches Task Timeout)
    # -------------------------------------------------------------
    @task(execution_timeout=timedelta(seconds=15))  # Strict SLA trigger
    def load_final_layer(cleaned_data: list[dict]):
        print(f"Loading {len(cleaned_data)} records into target destination...")

        # ERROR: LONG RUNNING QUERY / HUNG CONNECTION
        if FAIL_LONG_QUERY:
            print("[Simulating Long-Running Query] Simulating heavy table lock / runaway query...")
            # Airflow will kill the task after 15 seconds due to execution_timeout
            time.sleep(300)

        # Typical persistence (e.g., BigQuery, Postgres, Snowflake insert)
        print("Final insert successful.")

    # Task dependencies
    raw_path = ingest_to_gcs()
    cleaned = clean_and_process_data(raw_path)
    load_final_layer(cleaned)


faulty_etl_pipeline()