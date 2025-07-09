from airflow import settings
from airflow.models import TaskInstance
from datetime import datetime, timedelta
from sqlalchemy import desc
from airflow.decorators import dag, task



@dag(
    start_date=datetime(2021, 12, 1),
    schedule='@daily',
    catchup=False,
    dagrun_timeout=timedelta(minutes=2),
    max_active_runs=1,
    tags=["airflow monitoring",]
)
def airflow_monitoring():
    @task
    def get_db_info(execution_date):
        from airflow import settings
        from airflow.models import DagRun
        from sqlalchemy import and_
        from datetime import timedelta

        session = settings.Session()

        # define window based on execution_date
        start = execution_date - timedelta(days=1)
        end = execution_date

        runs = (
            session.query(DagRun)
            .filter(and_(DagRun.execution_date >= start,
                         DagRun.execution_date < end))
            .order_by(DagRun.execution_date)
            .all()
        )
        for dr in runs:
            print(f"{dr.execution_date} | {dr.dag_id} | {dr.run_id} | {dr.state}")

        session.close()

    get_db_info()
airflow_monitoring()


