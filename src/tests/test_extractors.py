import pandas as pd
import pytest
from extract.extractor_factory import get_extractor, run_extraction


# Mock extrators para teste
def mock_csv_extractor(cfg, params=None):
    return pd.DataFrame({"col1": [1, 2], "col2": ["A", "B"]})


def mock_api_extractor(cfg, params=None):
    return pd.DataFrame({"id": [10, 20], "value": ["X", "Y"]})


def mock_db_extractor(cfg, params=None):
    return pd.DataFrame({"id": [100, 200], "amount": [300, 400]})


@pytest.fixture
def sample_sources():
    return {
        "CSV_TEST": {"type": "csv"},
        "API_TEST": {"type": "api"},
        "DB_TEST": {"type": "database"},
        "FTP_TEST": {"type": "ftp"}
    }


def test_get_extractor_csv(sample_sources, monkeypatch):
    from extract import extractor_factory

    # substitui funções reais por mocks
    monkeypatch.setattr(extractor_factory, "extract_csv", mock_csv_extractor)
    extractor = get_extractor(sample_sources["CSV_TEST"])
    df = extractor(sample_sources["CSV_TEST"])
    assert isinstance(df, pd.DataFrame)
    assert "col1" in df.columns


def test_get_extractor_api(sample_sources, monkeypatch):
    from extract import extractor_factory

    monkeypatch.setattr(extractor_factory, "extract_api", mock_api_extractor)
    extractor = get_extractor(sample_sources["API_TEST"])
    df = extractor(sample_sources["API_TEST"])
    assert "id" in df.columns


def test_get_extractor_db(sample_sources, monkeypatch):
    from extract import extractor_factory

    monkeypatch.setattr(extractor_factory, "extract_db", mock_db_extractor)
    extractor = get_extractor(sample_sources["DB_TEST"])
    df = extractor(sample_sources["DB_TEST"])
    assert "amount" in df.columns


def test_run_extraction(sample_sources, monkeypatch):
    from extract import extractor_factory

    monkeypatch.setattr(extractor_factory, "extract_csv", mock_csv_extractor)
    df = run_extraction("CSV_TEST", sample_sources["CSV_TEST"])
    assert len(df) == 2


def test_invalid_type():
    with pytest.raises(ValueError):
        get_extractor({"type": "unknown"})
