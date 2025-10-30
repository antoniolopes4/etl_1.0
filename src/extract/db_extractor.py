import os
from typing import Optional, Dict

import pandas as pd
import pyodbc
from loguru import logger

def get_connection(cfg: Dict):
    db_cfg = cfg["db_config"]
    conn_str = (
        f"DRIVER={{{db_cfg['driver']}}};"
        f"SERVER={db_cfg['server']};"
        f"DATABASE={db_cfg['database']};"
        f"UID={db_cfg['user']};"
        f"PWD={db_cfg['password']}"
    )
    return pyodbc.connect(conn_str)


def extract_db(
        source_cfg: Dict,
        params: Optional[Dict] = None,
        limit: Optional[int] = None,
        save_csv: bool = True
) -> pd.DataFrame:
    """
    Extrai dados de base de dados via pyodbc.
    Permite query parametrizada e extração incremental.
    """
    conn_name = source_cfg["connection"]
    query = source_cfg["query"]
    columns = source_cfg.get("columns")

    if params:
        for k, v in params.items():
            query = query.replace(f":{k}", str(v))

    if limit:
        query = f"SELECT TOP {limit} * FROM ({query}) AS limited"

    logger.info(f"Executando query na conexão '{conn_name}'...")
    conn = get_connection(source_cfg)

    try:
        df = pd.read_sql(query, conn)
        logger.info(f"Extraídos {len(df)} registos da base de dados.")

        # Filtra colunas se especificadas
        if columns:
            missing = [c for c in columns if c not in df.columns]
            if missing:
                logger.warning(f"Colunas não encontradas: {missing}")
            df = df[[c for c in columns if c in df.columns]]

        if save_csv:
            os.makedirs("data/staging", exist_ok=True)
            file_path = os.path.join("data/staging", f"{conn_name}_extract.csv")
            df.to_csv(file_path, index=False)
            logger.info(f"Dados salvos em {file_path}")

        return df

    except Exception as e:
        logger.error(f"Erro ao extrair dados: {e}")
        raise
    finally:
        conn.close()
