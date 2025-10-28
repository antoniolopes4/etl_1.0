import requests
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from loguru import logger
from typing import Optional, Dict, Any, List, Union


# ---------------- Retry configuration ----------------
@retry(
    reraise=True,
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    retry=retry_if_exception_type(requests.exceptions.RequestException),
)
def get(url: str, params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None) -> requests.Response:
    """
    Faz uma chamada GET com retry/backoff exponencial.
    """
    logger.info(f"GET {url} | params={params}")
    response = requests.get(url, params=params, headers=headers, timeout=20)
    response.raise_for_status()
    return response


# ---------------- JSON normalization ----------------
def normalize_json(json_data: Any) -> pd.DataFrame:
    """
    Converte JSON em DataFrame, normalizando campos aninhados.
    """
    if isinstance(json_data, list):
        df = pd.json_normalize(json_data)
    elif isinstance(json_data, dict):
        # tenta achar lista dentro do dict (ex: {"data": [...]})
        key = next((k for k in json_data.keys() if isinstance(json_data[k], list)), None)
        if key:
            df = pd.json_normalize(json_data[key])
        else:
            df = pd.DataFrame([json_data])
    else:
        raise ValueError("Formato JSON inválido para normalização.")

    logger.info(f"JSON normalizado: {df.shape[0]} linhas, {df.shape[1]} colunas")
    return df


# ---------------- API Extractor ----------------
def extract_api(
        base_url: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        pagination_key: Optional[str] = None,
        max_pages: Union[int, str] = 5,
        columns: Optional[List[str]] = None,
        page_param_start: int = 1
) -> pd.DataFrame:
    """
    Extrai dados de uma API paginada e devolve um DataFrame único.

    Args:
        base_url: endpoint da API
        params: parâmetros fixos (ex: {"limit": 100})
        headers: cabeçalhos (ex: {"Authorization": "Bearer <token>"})
        pagination_key: nome do parâmetro de página (ex: "page" ou "offset")
        max_pages: número de páginas a extrair (int) ou "all"
        columns: lista de colunas que se deseja manter
        page_param_start: número inicial da página (geralmente 1)
    """
    all_data: List[pd.DataFrame] = []
    page = page_param_start
    total_rows = 0

    while True:
        query_params = params.copy() if params else {}
        if pagination_key:
            query_params[pagination_key] = page

        try:
            response = get(base_url, query_params, headers)
            data = response.json()
            df = normalize_json(data)
            if df.empty:
                logger.info(f"Nenhum dado retornado na página {page}.")
                break
            if columns:
                missing = [c for c in columns if c not in df.columns]
                if missing:
                    logger.warning(f"Colunas não encontradas: {missing}")
                df = df[[c for c in columns if c in df.columns]]
            all_data.append(df)
            total_rows += len(df)
        except Exception as e:
            logger.error(f"Erro na página {page}: {e}")
            break

        logger.info(f"✅ Página {page} processada ({len(df)} registos).")

        # Decide se deve parar
        if not pagination_key:
            break
        if isinstance(max_pages, int) and page >= max_pages:
            break
        if isinstance(max_pages, str) and max_pages.lower() != "all":
            logger.warning("Valor inválido para max_pages — deve ser int ou 'all'.")
            break

        page += 1

    if not all_data:
        logger.warning("Nenhum dado foi extraído.")
        return pd.DataFrame()

    final_df = pd.concat(all_data, ignore_index=True)
    logger.info(f"Extração concluída com {total_rows} registos totais e {final_df.shape[1]} colunas.")
    return final_df
