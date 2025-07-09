from airflow.decorators import task, dag
from datetime import datetime, timedelta
from airflow.sdk import PokeReturnValue


@dag(
    start_date=datetime(2021, 12, 1),
    schedule='@daily',
    catchup=False,
    dagrun_timeout=timedelta(minutes=2),
    max_active_runs=1,
    tags=["sensor", "branch", "check_api", "xcoms"]
)
def first_use_case():
    @task.sensor(poke_interval=60, timeout=300, mode="poke")
    def check_odd_minutes() -> PokeReturnValue:
        actual_minute = datetime.now().minute
        print("actual_minute", actual_minute)
        if actual_minute % 2 == 0:
            return PokeReturnValue(is_done=True)
        return PokeReturnValue(is_done=False)

    @task.branch
    def check_hour():
        actual_hour = datetime.now().hour
        if actual_hour % 2 == 0:
            return "odd_hour"
        return "even_hour"

    @task
    def odd_hour():
        print("odd_hour")

    @task
    def even_hour():
        print("even_hour")

    @task
    def go_check_pokemon_api():
        print("go_check_pokemon_api")

    @task
    def check_pokemon_api_available():
        import requests
        response = requests.get("https://pokeapi.co/api/v2/pokemon/")
        if response.status_code == 200:
            print("check_pokemon_api_available")
            data = response.json()
            pokemon_names = [pokemon["name"] for pokemon in data["results"]]
            print(pokemon_names)
        else:
            print("API IS NOT AVAILABLE")
            raise Exception("API IS NOT AVAILABLE")
        return pokemon_names

    @task
    def print_name(pokemon_name: str):
        print(f"Pokémon name: {pokemon_name}")
    """
    .expand() is used in Airflow to run the same task multiple times
    print_name.expand(pokemon_name=["pikachu", "charizard", "bulbasaur"])
    is equivalent to:
    for name in ["pikachu", "charizard", "bulbasaur"]:
        print_name(pokemon_name=name)
    """
    go_check = go_check_pokemon_api()
    check_odd_minutes() >> check_hour() >> [odd_hour(), even_hour()] >> go_check
    go_check >> print_name.expand(pokemon_name=check_pokemon_api_available())

first_use_case()
