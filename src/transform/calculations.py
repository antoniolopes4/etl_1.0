import pandas as pd
from datetime import datetime, timedelta
from loguru import logger


def add_id_tempo(df, offset_days=1, fixed_date=None):
    base_date = fixed_date or datetime.now()
    load_date = (base_date - timedelta(days=offset_days)).strftime("%Y%m%d")
    df["ID_TEMPO"] = int(load_date)
    return df


def substring_column(df: pd.DataFrame, col: str, start: int, end: int, new_col: str = None) -> pd.DataFrame:
    """
    Cria uma nova coluna com substring de outra.
    """
    df = df.copy()
    if col not in df.columns:
        logger.warning(f"Coluna '{col}' não encontrada — substring ignorada.")
        return df

    target_col = new_col or col
    df[target_col] = df[col].astype(str).str[start:end]
    logger.info(f"Substring aplicada em '{col}' → '{target_col}' [{start}:{end}].")
    return df


def aggregate_values(df: pd.DataFrame, group_by: list, agg_rules: dict) -> pd.DataFrame:
    """
    Executa agregações com base em colunas e funções especificadas.
    Exemplo de agg_rules: { "Valor": "sum", "ContaOrigem": "nunique" }
    """
    df = df.copy()
    if not set(group_by).issubset(df.columns):
        missing = [c for c in group_by if c not in df.columns]
        logger.warning(f"Colunas de agrupamento não encontradas: {missing}")
        return df

    grouped = df.groupby(group_by).agg(agg_rules).reset_index()
    logger.info(f"Agregação executada por {group_by} com regras {agg_rules}.")
    return grouped


def apply_calculations(df: pd.DataFrame, rules: dict) -> pd.DataFrame:
    """
    Orquestrador de cálculos derivados baseado em configuração JSON.
    """
    df_result = df.copy()
    if not rules:
        logger.info("⚙ Nenhuma regra de cálculo definida. Retornando DataFrame original.")
        return df_result

    logger.info(f"Aplicando regras de cálculo: {rules}")

    if rules.get("add_id_tempo", False):
        df_result = add_id_tempo(df_result, offset_days=rules.get("offset_days", 1))

    if "substring" in rules:
        for s in rules["substring"]:
            df_result = substring_column(df_result, **s)

    if "aggregations" in rules:
        for agg in rules["aggregations"]:
            df_result = aggregate_values(
                df_result,
                group_by=agg["group_by"],
                agg_rules=agg["agg"]
            )

    return df_result
