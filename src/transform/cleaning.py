import pandas as pd
import numpy as np
from datetime import datetime
from loguru import logger


def normalize_dates(df: pd.DataFrame, date_cols: list) -> pd.DataFrame:
    """
    Converte colunas de data para datetime, remove formatos inválidos.
    """
    df = df.copy()
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
        else:
            logger.warning(f"Coluna '{col}' não encontrada para normalização de data.")
    logger.info(f"Colunas de data normalizadas: {date_cols}")
    return df


def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Substitui todos os valores ausentes (NaN, None, NaT) por pd.NA.
    """
    df = df.copy()
    total_nulls_before = df.isna().sum().sum()

    if total_nulls_before == 0:
        logger.info("Nenhum valor ausente encontrado.")
        return df

    df = df.convert_dtypes()
    df = df.fillna(pd.NA)

    total_nulls_after = df.isna().sum().sum()
    logger.info(
        f"Valores ausentes substituídos por pd.NA "
        f"(antes: {total_nulls_before}, depois: {total_nulls_after})"
    )

    return df


def deduplicate(df: pd.DataFrame, subset=None) -> pd.DataFrame:
    """
    Remove duplicados, opcionalmente com base em colunas específicas.
    """
    df = df.copy()
    before = len(df)
    df = df.drop_duplicates(subset=subset)
    after = len(df)
    logger.info(f"Removidos {before - after} duplicados (base: {subset})")
    return df


def generate_quality_report(df_before: pd.DataFrame, df_after: pd.DataFrame) -> dict:
    """
    Gera um relatório de qualidade com estatísticas básicas.
    """
    report = {
        "rows_before": len(df_before),
        "rows_after": len(df_after),
        "rows_removed": len(df_before) - len(df_after),
        "columns": list(df_after.columns),
        "null_percent_per_col": df_after.isna().mean().to_dict(),
        "generated_at": datetime.now().isoformat()
    }
    logger.info(f"Relatório de qualidade: {report}")
    return report


def apply_cleaning_rules(df: pd.DataFrame, rules: dict) -> pd.DataFrame:
    """
    Aplica as regras de limpeza configuradas.
    """
    df_clean = df.copy()

    if not rules:
        logger.info("⚙ Nenhuma regra de limpeza definida. Retornando DataFrame original.")
        return df_clean

    logger.info(f"Aplicando regras de limpeza: {rules}")

    # ⚙ Normalizar datas
    if "normalize_dates" in rules:
        df_clean = normalize_dates(df_clean, rules["normalize_dates"])

    # ⚙ Substituir nulos por pd.NA
    df_clean = handle_missing_values(df_clean)

    # ⚙ Remover duplicados
    if "drop_duplicates" in rules:
        df_clean = deduplicate(df_clean, subset=rules["drop_duplicates"])

    # ⚙ Preencher nulos com valor padrão
    if "fill_missing" in rules:
        for col, val in rules["fill_missing"].items():
            if col in df_clean.columns:
                df_clean[col] = df_clean[col].fillna(val)
                logger.info(f"Preenchidos valores nulos em '{col}' com '{val}'")


    logger.info("Limpeza concluída com sucesso.")
    return df_clean
