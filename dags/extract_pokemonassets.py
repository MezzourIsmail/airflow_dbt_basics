from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.sdk import asset
from airflow.datasets import Dataset
import requests
import json

dataset_uri = Dataset("/tmp/pokemon.json")

@dag(
    start_date=datetime(2020, 1, 1),
    schedule=None,
    catchup=False,
    tags=['pokemon'],
    dagrun_timeout=timedelta(minutes=5),
)
def extract_pokemon_assets():

    @task
    def generate_json_file():
        response = requests.get("https://pokeapi.co/api/v2/pokemon/")
        data = response.json()
        with open(dataset_uri.uri, "w") as f:
            f.write(json.dumps(data, indent=2))
        return f




extract_pokemon_assets()
