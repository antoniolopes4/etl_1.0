import os
import glob
import chardet
import pandas as pd
from loguru import logger
from typing import Optional, List, Dict


def detect_encoding(file_path: str, n_bytes: int = 10000) -> str:
    """Detecta encoding de um ficheiro CSV."""
    with open(file_path, 'rb') as f:
        raw = f.read(n_bytes)
    enc = chardet.detect(raw)['encoding']
    logger.info(f"Encoding detectado para {os.path.basename(file_path)}: {enc}")
    return enc or 'utf-8'


def read_csv_file(file_path: str, schema: Optional[Dict[str, str]] = None) -> pd.DataFrame:
    """Lê um CSV com detecção automática de encoding e valida schema."""
    encoding = detect_encoding(file_path)

    try:
        df = pd.read_csv(file_path, encoding=encoding)
        logger.info(f"Lido ficheiro: {file_path} | Registos: {len(df)}")
    except Exception as e:
        logger.error(f"Erro ao ler {file_path}: {e}")
        raise

    # Validação de schema (colunas obrigatórias)
    if schema:
        missing_cols = [col for col in schema.keys() if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Faltam colunas obrigatórias no CSV: {missing_cols}")
        logger.info(f"Schema validado com sucesso: {list(schema.keys())}")

    return df


def extract(path: str, pattern: str = "*.csv", schema: Optional[Dict[str, str]] = None,
            deduplicate: bool = True, save_sample: bool = False,
            columns: Optional[List[str]] = None) -> pd.DataFrame:
    """
    Extrai CSVs de um diretório e devolve um DataFrame filtrado pelas colunas definidas.
    """
    if os.path.isfile(path):
        files = [path]
    else:
        files = glob.glob(os.path.join(path, pattern))

    if not files:
        logger.warning(f"Nenhum ficheiro encontrado em {path} com pattern {pattern}")
        return pd.DataFrame()

    logger.info(f"{len(files)} ficheiro(s) encontrados para leitura em {path}")

    dfs = []
    for file in files:
        df = read_csv_file(file, schema)
        df['__source_file'] = os.path.basename(file)

        # 🔹 Filtra as colunas se definido
        if columns:
            missing = [c for c in columns if c not in df.columns]
            if missing:
                logger.warning(f"Colunas {missing} não encontradas em {file}")
            df = df[[c for c in columns if c in df.columns]]

        dfs.append(df)

    full_df = pd.concat(dfs, ignore_index=True)
    logger.info(f"Concatenação concluída: {len(full_df)} linhas totais")

    if deduplicate:
        before = len(full_df)
        full_df.drop_duplicates(inplace=True)
        logger.info(f"Removidos {before - len(full_df)} duplicados")

    if save_sample:
        os.makedirs("logs", exist_ok=True)
        sample_path = os.path.join("logs", "csv_sample.csv")
        full_df.head(10).to_csv(sample_path, index=False)
        logger.info(f"Amostra salva em {sample_path}")

    return full_df
