FROM astrocrpublic.azurecr.io/runtime:3.0-4

RUN python3 -m venv /usr/local/airflow/dbt_venv \
    && /bin/bash -c "source /usr/local/airflow/dbt_venv/bin/activate && pip install --no-cache-dir dbt-duckdb==1.9.4 && deactivate"