import json
import importlib_resources

def read_config() -> dict:
    
    my_resources = importlib_resources.files("willthisfreeze")
    data = json.loads(my_resources.joinpath("config", "config.json").read_bytes())

    return data

def read_secret() -> dict:
    
    my_resources = importlib_resources.files("willthisfreeze")
    data = json.loads(my_resources.joinpath("config", "secret.json").read_bytes())

    return data

