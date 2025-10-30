from extract.api_extractor import extract_api
from extract.csv_extractor import extract_csv
from extract.db_extractor import extract_db
from loguru import logger


def get_extractor(source_cfg):
    """
    Devolve a função de extração correta com base no tipo de fonte.
    """
    extract_type = source_cfg.get("type")

    if extract_type == "csv":
        return extract_csv
    elif extract_type == "api":
        return extract_api
    elif extract_type == "database":
        return extract_db
    else:
        raise ValueError(f"Tipo de fonte desconhecido: {extract_type}")


def run_extraction(source_name, source_cfg, params=None):
    """
    Executa a extração completa para uma fonte.
    """
    try:
        extractor = get_extractor(source_cfg)
        logger.info(f"Iniciando extração para '{source_name}' ({source_cfg['type']})...")
        df = extractor(source_cfg, params=params)
        logger.info(f"Extração concluída: {len(df)} registos extraídos.")
        return df
    except Exception as e:
        logger.error(f"Erro na extração da fonte {source_name}: {e}")
        raise
