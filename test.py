from utils.config_loader import load_config
conf = load_config("DRR")
print(conf)

from utils.config_loader import load_config
conf = load_config("PRECARIO")
print(conf)

from utils.config_loader import load_config
from extract.csv_extractor import extract_csv
config = load_config("PRECARIO")
df = extract_csv(
    path=config["path"],
    columns=config.get("columns")
)
print(df.head())

from utils.config_loader import load_config
from extract.api_extractor import extract_api
config = load_config("API_TRANSACOES")
df = extract_api(
    base_url=config["base_url"],
    params=config.get("params"),
    pagination_key=config.get("pagination_key"),
    max_pages=config.get("max_pages", 5),
    columns=config.get("columns"),
)
print(df.head())

from dotenv import load_dotenv
from utils.config_loader import load_config
from extract.db_extractor import get_connection
load_dotenv()
config = load_config("SAS_AML")
conn = get_connection(config)
print("Conexão bem-sucedida!")
conn.close()

from extract.db_extractor import extract_db
from utils.config_loader import load_config
load_dotenv()
config = load_config("SAS_AML")
df = extract_db(config, params={"id_tempo": 20250925}, limit=100)
print(df.head())

import pandas as pd
from load.load_to_staging import load_to_staging
data = {
    "id": [1, 2, 3],
    "nome": ["Ana", "Bruno", "Carlos"],
    "valor": [100.0, 200.5, 150.7]
}
df = pd.DataFrame(data)
config = {"target_table": "TMP_CLIENTES", "staging_format": "parquet"}
load_to_staging(df, config)

import pandas as pd
from transform.cleaning import apply_cleaning_rules
data = {
    "DataTransacao": ["2025-10-01", "INVALIDO", None],
    "ContaOrigem": ["123", "123", None],
    "ContaDestino": ["456", "456", "789"],
    "Valor": [100, None, 300]
}
rules = {
    "normalize_dates": ["DataTransacao"],
    "drop_duplicates": ["ContaOrigem", "ContaDestino", "Valor"],
}
df = pd.DataFrame(data)
df_clean = apply_cleaning_rules(df, rules)
print(df_clean)

from model.star_builder import build_star_schema
from utils.config_loader import load_config
aml_model = load_config("SAS_AML")
ddls = build_star_schema(aml_model)
for table, ddl in ddls.items():
    print(f"\n--- {table} ---\n{ddl}")

