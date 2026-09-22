from datetime import datetime
import pandas as pd
import requests
from airflow.decorators import dag, task

API_URL = "https://jsonplaceholder.typicode.com/posts"  # Returns exactly 100 records


@dag(
    dag_id="multi_layer_api_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["etl", "layers"],
)
def pipeline():
    # 1. Ingestion Layer: Fetch raw data from API
    @task
    def ingestion_layer() -> list[dict]:
        response = requests.get(API_URL, timeout=10)
        response.raise_for_status()
        raw_data = response.json()  # Exactly 100 rows
        return raw_data

    # 2. Staging Layer: Clean, type-cast, and structure data
    @task
    def staging_layer(raw_data: list[dict]) -> list[dict]:
        df = pd.DataFrame(raw_data)
        staged_df = df[["userId", "id", "title", "body"]].copy()
        staged_df["userId"] = staged_df["userId"].astype(int)
        staged_df["id"] = staged_df["id"].astype(int)
        staged_df["body_length"] = staged_df["body"].str.len()
        return staged_df.to_dict(orient="records")

    # 3. Aggregate Layer: Summarize metrics by user
    @task
    def aggregate_layer(staged_data: list[dict]) -> list[dict]:
        df = pd.DataFrame(staged_data)
        agg_df = (
            df.groupby("userId")
            .agg(
                post_count=("id", "count"),
                avg_body_length=("body_length", "mean"),
            )
            .reset_index()
        )
        return agg_df.to_dict(orient="records")

    # 4. Final Layer: Load/export to target destination
    @task
    def final_layer(aggregated_data: list[dict]):
        final_df = pd.DataFrame(aggregated_data)
        # In practice: write to PostgreSQL, Snowflake, BigQuery, or S3/Parquet
        output_path = "/tmp/user_post_aggregations.csv"
        final_df.to_csv(output_path, index=False)
        print(
            f"Successfully wrote {len(final_df)} aggregated records to {output_path}"
        )

    # Dependency chaining across the 4 layers
    raw = ingestion_layer()
    staged = staging_layer(raw)
    aggregated = aggregate_layer(staged)
    final_layer(aggregated)


pipeline()