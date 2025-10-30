import json
from loguru import logger
from typing import Dict, Any

def sql_type(py_type: str) -> str:
    """Mapeia tipos Python para SQL Server"""
    mapping = {
        "int": "INT",
        "float": "FLOAT",
        "str": "NVARCHAR(255)",
        "datetime": "DATETIME",
        "decimal": "DECIMAL(18,2)",
        "bool": "BIT"
    }
    return mapping.get(py_type.lower(), "NVARCHAR(255)")


def generate_table_sql(table_name: str, columns: Dict[str, str], primary_key=None, foreign_keys=None) -> str:
    cols_sql = [f"{col} {dtype}" for col, dtype in columns.items()]
    if primary_key:
        cols_sql.append(f"PRIMARY KEY ({primary_key})")
    if foreign_keys:
        for fk, ref in foreign_keys.items():
            cols_sql.append(f"FOREIGN KEY ({fk}) REFERENCES {ref}")
    ddl = f"CREATE TABLE {table_name} (\n  " + ",\n  ".join(cols_sql) + "\n);"
    return ddl


def build_star_schema(config: Dict[str, Any]) -> Dict[str, str]:
    model = config["dimensional_model"]
    ddls = {}

    naming = model.get("naming", {})
    dim_prefix = naming.get("dimension_prefix", "Dim_")
    fact_prefix = naming.get("fact_prefix", "Fact_")
    surrogate_prefix = naming.get("surrogate_key", "SK_")

    # DIMENSÕES
    for dim_name, dim_info in model["dimensions"].items():
        table_name = f"{dim_prefix}{dim_name}"
        columns = {}

        # Surrogate key
        surrogate_key = f"{surrogate_prefix}{dim_name}"
        columns[surrogate_key] = "INT IDENTITY(1,1)"

        # Natural keys
        for key in dim_info.get("keys", []):
            columns[key] = "NVARCHAR(255)"

        # Attributes with custom types
        for attr, dtype in dim_info.get("attributes", {}).items():
            columns[attr] = dtype

        # Slowly Changing Dimensions
        if dim_info.get("scd_type") == 2:
            columns.update(dim_info.get("date_columns", {}))
            columns["ValidFrom"] = "DATETIME"
            columns["ValidTo"] = "DATETIME"
            columns["IsCurrent"] = "BIT"

        ddl = generate_table_sql(
            table_name,
            columns,
            primary_key=surrogate_key
        )
        ddls[table_name] = ddl
        logger.info(f"DDL gerada para {table_name}:\n{ddl}")

    # FACT TABLE
    fact_name = f"{fact_prefix}{model['fact_table'].replace(fact_prefix, '')}"
    fact_cols = {"ID_FACT": "INT IDENTITY(1,1)"}

    # Foreign keys para dimensões
    foreign_keys = {}
    for dim_name, dim_info in model["dimensions"].items():
        dim_table = f"{dim_prefix}{dim_name}"
        surrogate_key = f"{surrogate_prefix}{dim_name}"
        fact_cols[surrogate_key] = "INT"
        foreign_keys[surrogate_key] = dim_table

    # Fact attributes
    for fact_col, dtype in model["facts"].items():
        fact_cols[fact_col] = dtype

    ddl_fact = generate_table_sql(
        fact_name,
        fact_cols,
        primary_key="ID_FACT",
        foreign_keys=foreign_keys
    )
    ddls[fact_name] = ddl_fact
    logger.info(f"DDL gerada para {fact_name}:\n{ddl_fact}")

    return ddls

