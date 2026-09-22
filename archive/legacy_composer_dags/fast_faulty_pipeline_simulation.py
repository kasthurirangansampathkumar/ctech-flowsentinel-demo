from datetime import datetime, timedelta
import json
import os
import time
import requests
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException

# -------------------------------------------------------------
# QUICK TOGGLES FOR TESTING (Turn ON/OFF as needed)
# -------------------------------------------------------------
TRIGGER_API_FAILURE = False      # Instant 404 HTTP failure
TRIGGER_RESOURCE_EXHAUSTION = False  # Rapid MemoryError (takes < 1 sec)
TRIGGER_SCHEMA_CHANGE = True    # Instant KeyError on missing required field
TRIGGER_LONG_RUNNING_QUERY = False  # Task fails via Airflow timeout in 5 seconds

LOCAL_STAGING_PATH = "/tmp/fast_sim_raw.json"


@dag(
    dag_id="fast_faulty_pipeline_simulation",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["quick-test", "chaos"],
    default_args={
        "retries": 0,  # 0 retries to finish execution immediately
    },
)
def fast_pipeline():

    # -------------------------------------------------------------
    # LAYER 1: Ingestion Layer (API -> Storage)
    # -------------------------------------------------------------
    @task
    def ingestion_layer() -> str:
        # Broken URL causes instant failure; valid URL fetches only 5 records
        url = (
            "https://jsonplaceholder.typicode.com/invalid_endpoint_fast"
            if TRIGGER_API_FAILURE
            else "https://jsonplaceholder.typicode.com/posts?_limit=5"
        )

        response = requests.get(url, timeout=3)
        if response.status_code != 200:
            raise AirflowException(
                f"[API Failure] Upstream API returned HTTP {response.status_code}"
            )

        with open(LOCAL_STAGING_PATH, "w") as f:
            json.dump(response.json(), f)

        return LOCAL_STAGING_PATH

    # -------------------------------------------------------------
    # LAYER 2: Data Cleaning Layer
    # -------------------------------------------------------------
    @task
    def data_cleaning_layer(file_path: str) -> list[dict]:
        with open(file_path, "r") as f:
            records = json.load(f)

        # 1. Fast Resource Exhaustion (< 1 second to throw MemoryError)
        if TRIGGER_RESOURCE_EXHAUSTION:
            print("[Simulating Resource Exhaustion] Fast Memory Allocation...")
            _ = [bytearray(1024 * 1024 * 500) for _ in range(100)]

        # 2. Fast Schema Change (< 1 millisecond to throw KeyError)
        if TRIGGER_SCHEMA_CHANGE:
            for row in records:
                if "userId" in row:
                    row["account_id"] = row.pop("userId")  # Column dropped/renamed

        # Strict validation
        for row in records:
            if "userId" not in row:
                raise KeyError(
                    f"[Schema Change Error] Expected column 'userId' missing! Available columns: {list(row.keys())}"
                )

        return records

    # -------------------------------------------------------------
    # LAYER 3: Final Layer (Destination)
    # -------------------------------------------------------------
    @task(execution_timeout=timedelta(seconds=5))  # Fails at 5 seconds sharp
    def final_layer(cleaned_data: list[dict]):
        if TRIGGER_LONG_RUNNING_QUERY:
            print("[Simulating Long Query] Query hanging, awaiting timeout...")
            time.sleep(15)  # Breaches the 5-second execution_timeout

        print(f"Successfully processed {len(cleaned_data)} records to destination.")
        if os.path.exists(LOCAL_STAGING_PATH):
            os.remove(LOCAL_STAGING_PATH)

    # Pipeline dependencies
    raw_file = ingestion_layer()
    cleaned = data_cleaning_layer(raw_file)
    final_layer(cleaned)


fast_pipeline()