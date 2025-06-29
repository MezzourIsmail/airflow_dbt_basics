from airflow.decorators import task, dag
import datetime

from airflow.sdk import PokeReturnValue


@dag(
    start_date=datetime.datetime(2021, 12, 1),
    schedule='@daily',
    catchup=False,
)
def first_workflow():
    @task.sensor(poke_interval=60, timeout=300, mode="poke")
    def check_odd_minutes() -> PokeReturnValue:
        actual_minute = datetime.datetime.now().minute
        print("actual_minute", actual_minute)
        if actual_minute % 2 == 0:
            return PokeReturnValue(is_done=True)
        else:
            return PokeReturnValue(is_done=False)

    @task
    def print_hello_ismail():
        print("hello Ismail")


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
    check_odd_minutes() >> print_hello_ismail() >> print_name.expand(pokemon_name=check_pokemon_api_available())

first_workflow()
