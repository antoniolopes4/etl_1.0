import os
import pandas as pd
from datetime import datetime
from loguru import logger


def load_to_staging(df: pd.DataFrame, cfg: dict, mode: str = "replace"):
    """
    Grava o DataFrame em formato Parquet ou CSV na pasta staging/.
    Cria versões datadas (yyyymmdd_hhmmss).

    Args:
        df: DataFrame a guardar
        cfg: Configuração da fonte (ex: target_table)
        mode: 'replace' (substitui) ou 'append' (acrescenta)
    """

    target_name = cfg.get("target_table", "unknown_table")
    format_type = cfg.get("staging_format", "parquet").lower()

    # Cria diretório staging se não existir
    os.makedirs("staging", exist_ok=True)

    # Cria subpasta por target_table
    table_dir = os.path.join("staging", target_name)
    os.makedirs(table_dir, exist_ok=True)

    # Nome de ficheiro com timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{target_name}_{timestamp}.{format_type}"
    file_path = os.path.join(table_dir, filename)

    try:
        if format_type == "parquet":
            df.to_parquet(file_path, index=False)
        elif format_type == "csv":
            df.to_csv(file_path, index=False, encoding="utf-8")
        else:
            raise ValueError(f"Formato desconhecido: {format_type}")

        logger.info(f"Guardado {len(df)} registos em {file_path}")

        # Se modo replace → remove versões antigas
        if mode == "replace":
            cleanup_old_versions(table_dir, keep_last=1)

        return file_path

    except Exception as e:
        logger.error(f"Erro ao gravar staging {target_name}: {e}")
        raise


def cleanup_old_versions(table_dir: str, keep_last: int = 1):
    """Remove versões antigas, mantendo apenas as mais recentes."""
    files = sorted(
        [f for f in os.listdir(table_dir) if f.endswith(".parquet") or f.endswith(".csv")],
        reverse=True
    )
    for old_file in files[keep_last:]:
        try:
            os.remove(os.path.join(table_dir, old_file))
        except Exception as e:
            logger.warning(f"Não foi possível apagar {old_file}: {e}")
