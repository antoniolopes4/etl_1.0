import os
import json
import pytest
from utils import config_loader


@pytest.fixture
def mock_env(tmp_path, monkeypatch):
    # Criar pasta config temporária
    config_dir = tmp_path / "config"
    config_dir.mkdir()

    sources_json = {
        "SAS_AML": {"type": "database", "connection": "sqlserver_aml"}
    }
    db_json = {
        "sqlserver_aml": {"user_env": "DB_USER", "password_env": "DB_PASS"}
    }

    (config_dir / "sources.json").write_text(json.dumps(sources_json))
    (config_dir / "db_config.json").write_text(json.dumps(db_json))

    # Criar .env temporário
    env_file = tmp_path / ".env"
    env_file.write_text("DB_USER=test_user\nDB_PASS=test_pass")

    # Redefinir variáveis de ambiente e paths
    monkeypatch.setattr(config_loader, "BASE_DIR", tmp_path)
    monkeypatch.setattr(config_loader, "CONFIG_DIR", str(config_dir))
    config_loader.load_dotenv(dotenv_path=env_file, override=True)

    return tmp_path


def test_load_config_with_env(mock_env):
    cfg = config_loader.load_config("SAS_AML")

    assert cfg["type"] == "database"
    assert "db_config" in cfg
    assert cfg["db_config"]["user"] == "test_user"
    assert cfg["db_config"]["password"] == "test_pass"
