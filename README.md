## BEES Breweries Case

Solução em Python + PySpark para consumir a Open Brewery DB e persistir os dados em uma arquitetura medallion com camadas bronze, silver e gold.

## Resumo

- API: Open Brewery DB
- Orquestração: Luigi
- Processamento: PySpark
- Armazenamento: JSON Lines na bronze e parquet na silver/gold
- Monitoramento: logs estruturados, relatório JSON por `run_id` e checks básicos de qualidade
- Testes: `pytest`
- Extra opcional: dashboard em Streamlit

## Como rodar

Fluxo principal do case:

```bash
docker compose up --build
docker compose run --rm spark-app pytest
```

O scheduler do Luigi fica em `http://localhost:8082`.
O `spark-app` é batch: sobe, executa a pipeline e encerra com `exit code 0`.

Artefatos gerados:

- `data/bronze/`
- `data/silver/`
- `data/gold/`
- `data/monitoring/`

## Execução local

```bash
pip install -r requirements.txt
luigid --address 0.0.0.0 --port 8082
luigi --module app.orchestration.luigi_pipeline BreweryMedallionPipeline --scheduler-host localhost --scheduler-port 8082
```

Ou:

```bash
python -m app.jobs.run_pipeline
```

## Streamlit opcional

O dashboard é apenas um adicional de visualização e não faz parte do escopo principal do case.

```bash
docker compose --profile optional up --build streamlit-app
```

Ou:

```bash
pip install -r requirements-streamlit.txt
streamlit run app/ui/streamlit_app.py
```

Depois abra `http://localhost:8501`.

Observação: a interface opcional não usa `pandas`.

## Arquitetura

- Bronze: payload bruto da API em JSON Lines, particionado por data de ingestão e `run_id`
- Silver: dados tratados em parquet, deduplicados por `id` e particionados por `country` e `state`
- Gold: agregado com quantidade de cervejarias por `brewery_type` e localização

## Monitoramento

- Logs estruturados por etapa da pipeline
- Relatório por execução em `data/monitoring/run_id=...json`
- Checks para payload vazio, `id` nulo e consistência entre silver e gold
- Retry da task Luigi e UI do scheduler para acompanhamento local

Em produção, eu adicionaria alertas por Slack/email e métricas centralizadas em uma ferramenta como Prometheus/Grafana ou Datadog.

## Trade-offs

- Usei Luigi por ser simples e suficiente para um case local
- A silver ficou particionada por `country` e `state`, e não por `city`, para evitar partições pequenas demais e melhorar a escrita local do Spark
- O Streamlit ficou separado como extra opcional para não interferir no escopo principal
