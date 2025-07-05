from airflow.sdk import asset

@asset(
    schedule="@daily",
    description="test assets",
    tags=["asset"]
)
def user():
    return {
        "score": 75,
        "update_date": "2025-05-01"
    }