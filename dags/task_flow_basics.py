from airflow.decorators import dag, task
from datetime import datetime

@dag(
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
)
def test_task_flow():
    @task
    def test_ismail():
        print("hello Ismail")

    @task
    def test_alice():
        print("hello Alice")

    test_ismail() >> test_alice()

test_task_flow()
