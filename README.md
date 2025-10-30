## Projeto ETL

## Visão geral

Pipeline ETL modular em Python para ingestão, transformação e carga de dados (SQL Server).
Arquitetura: Raw/Staging → Transform → ODS → Data Warehouse → Power BI.

## Tecnologias

- Python 3.12
- Pandas, SQLAlchemy, PyODBC
- Pytest
- SQL Server
- Power BI

## Estrutura

src/
├── extract/
├── transform/
├── load/
├── utils/
└── pipelines/

## Como configurar ambiente (venv)

```shell
python -m venv .venv
source .venv/bin/activate   # Linux/macOS
.venv\Scripts\Activate.ps1  # Windows PowerShell
pip install -r requirements.txt
```

## Como executar

```shell
python src/pipelines/run_all_sources.py
```

## Executar testes

```shell
set PYTHONPATH=src
pytest -v
```

## Contacto

**António Lopes**
Engenheiro de dados
[antoniolopes4real@gmail.com](mailto:antoniolopes4real@gmail.com)
[LinkedIn](www.linkedin.com/in/antoniolopes4real)
Angola
