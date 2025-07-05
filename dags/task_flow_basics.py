from airflow.decorators import dag, task
from datetime import datetime
from airflow.exceptions import AirflowException, AirflowFailException


def _handle_failed_dag_run(context):
    print("DAG failed here the information about the failed run")
    print(context)


@dag(
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
    on_failure_callback=_handle_failed_dag_run,
)
def test_task_flow():
    @task
    def test_ismail():
        print("hello Ismail")

    @task
    def test_alice():
        print("hello Alice")
        raise AirflowFailException("Task failed for testing the callback")

    test_ismail() >> test_alice()

test_task_flow()
