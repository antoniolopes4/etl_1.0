import pandas as pd
import numpy as np
import pytest
from datetime import datetime
from transform import cleaning  # ajusta o caminho conforme tua estrutura


@pytest.fixture
def sample_df():
    return pd.DataFrame({
        "DateCol": ["2025-01-01", "2025/02/01", "invalid_date"],
        "Value": [10, None, 30],
        "Category": ["A", "A", "B"]
    })


def test_normalize_dates(sample_df):
    df_out = cleaning.normalize_dates(sample_df, ["DateCol"])
    assert pd.api.types.is_datetime64_any_dtype(df_out["DateCol"])
    assert df_out["DateCol"].isna().sum() > 1  # 1 inválida
    assert len(df_out) == len(sample_df)


def test_handle_missing_values(sample_df):
    df_out = cleaning.handle_missing_values(sample_df)
    assert df_out.isna().sum().sum() == 1  # apenas a inválida convertida em pd.NA
    assert df_out.dtypes["Value"].name in ["Int64", "Float64"]  # tipo compatível


def test_deduplicate():
    df = pd.DataFrame({
        "ID": [1, 1, 2],
        "Name": ["A", "A", "B"]
    })
    df_out = cleaning.deduplicate(df, subset=["ID"])
    assert len(df_out) == 2
    assert not df_out.duplicated(subset=["ID"]).any()


def test_generate_quality_report(sample_df):
    df_after = sample_df.drop(index=1)
    report = cleaning.generate_quality_report(sample_df, df_after)
    assert report["rows_before"] == 3
    assert report["rows_after"] == 2
    assert report["rows_removed"] == 1
    assert "generated_at" in report
    assert isinstance(report["null_percent_per_col"], dict)


def test_apply_cleaning_rules(sample_df):
    rules = {
        "normalize_dates": ["DateCol"],
        "drop_duplicates": ["Category"],
        "fill_missing": {"Value": 0}
    }

    df_out = cleaning.apply_cleaning_rules(sample_df, rules)

    # Verifica transformação
    assert pd.api.types.is_datetime64_any_dtype(df_out["DateCol"])
    assert not df_out["Value"].isna().any()
    assert len(df_out) <= len(sample_df)
    assert "Category" in df_out.columns


def test_apply_cleaning_rules_empty(sample_df):
    df_out = cleaning.apply_cleaning_rules(sample_df, {})
    pd.testing.assert_frame_equal(df_out, sample_df)
