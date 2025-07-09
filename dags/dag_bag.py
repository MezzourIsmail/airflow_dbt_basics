from airflow.decorators import dag, task
from airflow.models import DagBag
from datetime import datetime

@dag(
    dag_id="inspect_dagbag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["debug"],
)
def inspect_dagbag():

    @task
    def list_erros_dags():
        dagbag = DagBag(include_examples=False)
        errors = dagbag.import_errors
        print(errors)
        # if errors:
        #     print(f"Import errors:")
        #     for path, err in errors.items():
        #         print(f"  • {path}: {err}")
        # return {"errors": errors}

    @task
    def list_dags_and_task_counts():
        dagbag = DagBag(include_examples=False)
        dag_ids = list(dagbag.dags.keys())
        errors = dagbag.import_errors

        # Build a mapping of dag_id → number of tasks
        task_counts = {
            dag_id: len(dag.tasks)
            for dag_id, dag in dagbag.dags.items()
        }
        print("task_counts >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
        print(task_counts)

        print(f"Loaded DAGs ({len(dag_ids)}): {dag_ids}")
        print("Task counts per DAG:")
        for dag_id, count in task_counts.items():
            print(f"  • {dag_id}: {count} tasks")

        if errors:
            print("Import errors:")
            for path, err in errors.items():
                print(f"  • {path}: {err}")

        return {
            "loaded_dags": dag_ids,
            "task_counts": task_counts,
            "import_errors": errors,
        }


    list_erros_dags() >> list_dags_and_task_counts()



inspect_dagbag()
