import os
from pathlib import Path

from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import DuckDBUserPasswordProfileMapping
from pendulum import datetime

dbt_projects_dir = Path("/usr/local/airflow/dags/dbt")

dbt_project_path = Path(dbt_projects_dir) / "airflow_duckdb"

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=DuckDBUserPasswordProfileMapping(
        conn_id="duckdb_default",
    ),
)

execution_config = ExecutionConfig(
    dbt_executable_path="/usr/local/airflow/dbt_venv/bin/dbt",
)

project_config = ProjectConfig(
    dbt_project_path=dbt_project_path,
)

dbt_cosmos_duckdb = DbtDag(
    dag_id="dbt_duckdb_workflow",
    project_config=project_config,
    profile_config=profile_config,
    execution_config=execution_config,
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["dbt", "duckdb", "cosmos"],
    render_config=None,
)
