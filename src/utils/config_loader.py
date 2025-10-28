import os
import json
import yaml
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
CONFIG_DIR = os.path.join(BASE_DIR, "config")

# Carregar variáveis do .env
load_dotenv(dotenv_path=os.path.join(BASE_DIR, ".env"), override=True)


def load_json(file_name: str):
    """Carrega ficheiro JSON do diretório config/"""
    path = os.path.join(CONFIG_DIR, file_name)
    if not os.path.exists(path):
        raise FileNotFoundError(f"Config file not found: {file_name}")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_yaml(file_name: str):
    """Carrega ficheiro YAML do diretório config/"""
    path = os.path.join(CONFIG_DIR, file_name)
    if not os.path.exists(path):
        raise FileNotFoundError(f"Config file not found: {file_name}")
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def get_env_var(name: str, required=True):
    """Obtém variável de ambiente do .env"""
    value = os.getenv(name)
    if required and not value:
        raise EnvironmentError(f"Missing environment variable: {name}")
    return value


def load_config(source_name: str):
    """
    Carrega configuração completa de uma fonte (ex: DRR, SAS_AML)
    combinando sources.json + db_config.json + .env
    """
    sources = load_json("sources.json")
    dbs = load_json("db_config.json")

    if source_name not in sources:
        raise KeyError(f"Fonte '{source_name}' não encontrada em sources.json")

    source_conf = sources[source_name]

    # Se for do tipo database, resolve user/password do .env
    if source_conf.get("type") == "database":
        conn_name = source_conf["connection"]
        db_conf = dbs[conn_name]
        user = get_env_var(db_conf["user_env"])
        password = get_env_var(db_conf["password_env"])
        db_conf.update({"user": user, "password": password})
        source_conf["db_config"] = db_conf

    return source_conf
