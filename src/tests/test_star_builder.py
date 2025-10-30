# tests/test_star_builder.py
import pytest
from model.star_builder import build_star_schema

@pytest.fixture
def scd2_config():
    return {
        "dimensional_model": {
            "fact_table": "Fact_Transactions",
            "grain": "1 linha por transação",
            "dimensions": {
                "Cliente": {
                    "scd_type": 2,
                    "surrogate_key": "SK_Cliente",
                    "keys": ["ContractPrefix"],
                    "attributes": {
                        "ContractNumber": "NVARCHAR(50)",
                        "Nome": "NVARCHAR(100)"
                    }
                },
                "Tempo": {
                    "scd_type": 1,
                    "surrogate_key": "SK_Tempo",
                    "keys": ["ID_TEMPO"],
                    "attributes": {
                        "Ano": "INT",
                        "Mes": "INT",
                        "Dia": "INT"
                    }
                }
            },
            "facts": {
                "Valor": "DECIMAL(18,2)",
                "TransactionCount": "INT"
            }
        }
    }


def test_build_star_schema_with_scd2(scd2_config):
    ddls = build_star_schema(scd2_config)

    # Deve gerar 3 tabelas
    assert set(ddls.keys()) == {"Dim_Cliente", "Dim_Tempo", "Fact_Transactions"}

    # Verifica se surrogate keys e colunas SCD2 estão na Dim_Cliente
    ddl_cliente = ddls["Dim_Cliente"]
    assert "SK_Cliente INT IDENTITY(1,1)" in ddl_cliente
    assert "ValidFrom DATETIME" in ddl_cliente
    assert "ValidTo DATETIME" in ddl_cliente
    assert "IsCurrent BIT" in ddl_cliente

    # Fact deve referenciar as surrogate keys
    ddl_fact = ddls["Fact_Transactions"]
    assert "SK_Cliente INT" in ddl_fact
    assert "SK_Tempo INT" in ddl_fact
    assert "FOREIGN KEY (SK_Cliente) REFERENCES Dim_Cliente" in ddl_fact

