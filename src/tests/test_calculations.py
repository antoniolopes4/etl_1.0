import pandas as pd
import pytest
from datetime import datetime, timedelta
from transform import calculations


@pytest.fixture
def sample_df():
    return pd.DataFrame({
        "ContractNumber": ["ABC12345", "XYZ67890"],
        "Valor": [100, 200],
        "TransactionID": [1, 2],
        "ID_TEMPO": [20251001, 20251001]
    })


def test_add_id_tempo(monkeypatch, sample_df):
    # Mock datetime para resultado previsível
    fake_today = datetime(2025, 10, 29)
    monkeypatch.setattr(calculations, "datetime", lambda: fake_today)

    df_out = calculations.add_id_tempo(sample_df, offset_days=1, fixed_date=datetime(2025, 10, 29))
    assert "ID_TEMPO" in df_out.columns
    assert df_out["ID_TEMPO"].iloc[0] == 20251028


def test_substring_column(sample_df):
    df_out = calculations.substring_column(
        sample_df, col="ContractNumber", start=0, end=3, new_col="Prefix"
    )
    assert "Prefix" in df_out.columns
    assert list(df_out["Prefix"]) == ["ABC", "XYZ"]


def test_substring_column_missing_col(sample_df):
    df_out = calculations.substring_column(
        sample_df, col="Inexistente", start=0, end=3, new_col="Prefix"
    )
    # Nenhuma coluna nova deve ser criada
    assert "Prefix" not in df_out.columns


def test_aggregate_values_basic(sample_df):
    df_out = calculations.aggregate_values(
        sample_df, group_by=["ID_TEMPO"], agg_rules={"Valor": "sum", "TransactionID": "count"}
    )

    assert len(df_out) == 1
    assert df_out.loc[0, "Valor"] == 300
    assert df_out.loc[0, "TransactionID"] == 2


def test_aggregate_values_missing_group(sample_df):
    df_out = calculations.aggregate_values(
        sample_df, group_by=["Inexistente"], agg_rules={"Valor": "sum"}
    )
    # Deve retornar dataframe original sem crash
    assert isinstance(df_out, pd.DataFrame)
    assert "Valor" in df_out.columns


def test_apply_calculations_end_to_end(sample_df):
    rules = {
        "add_id_tempo": True,
        "offset_days": 0,
        "substring": [
            {"col": "ContractNumber", "start": 0, "end": 3, "new_col": "Prefix"}
        ],
        "aggregations": [
            {"group_by": ["ID_TEMPO"], "agg": {"TransactionID": "count", "Valor": "sum"}}
        ]
    }

    df_out = calculations.apply_calculations(sample_df, rules)
    assert "Valor" in df_out.columns
    assert df_out["Valor"].sum() == 300


def test_apply_calculations_no_rules(sample_df):
    df_out = calculations.apply_calculations(sample_df, None)
    pd.testing.assert_frame_equal(df_out, sample_df)