from airflow.decorators import dag, task
from airflow.models import DagBag
from datetime import datetime

@dag(
    dag_id="inspect_dagbag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["dag_bag", "monitoring"],
)
def dag_bag_check():

    @task
    def dag_bag_information():
        dagbag = DagBag(include_examples=False)
        total_dags = len(dagbag.dags)
        print(f"Total DAGs: {total_dags}")

    @task
    def dag_bag_task_count():
        dagbag = DagBag(include_examples=False)
        for dag_id, dag in dagbag.dags.items():
            task_count = len(dag.tasks)
            print(f"• {dag_id}: {task_count} tasks \n")

    @task
    def dag_bag_import_errors():
        dagbag = DagBag(include_examples=False)
        if dagbag.import_errors:
            print("Import errors found:")
            for path, err in dagbag.import_errors.items():
                print(f" - {path}: {err}")
        else:
            print("No import errors!")

    dag_bag_information()
    dag_bag_task_count()
    dag_bag_import_errors()

dag_bag_check()
