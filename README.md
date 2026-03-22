## BEES Breweries Case

Projeto em Python + PySpark para consumir a API Open Brewery DB e persistir os dados em uma arquitetura medallion com camadas bronze, silver e gold.

## O que foi implementado

- Ingestao da API `https://api.openbrewerydb.org/v1/breweries`
- Pipeline medallion com persistencia local em `data/`
- Orquestracao com Luigi
- Containerizacao com Docker
- Testes unitarios para cliente da API e helpers da pipeline

## Estrutura

- `app/clients/open_brewery.py`: cliente HTTP com paginacao e retry simples
- `app/pipeline.py`: regras de ingestao e transformacao das camadas bronze, silver e gold
- `app/orchestration/luigi_pipeline.py`: task Luigi para disparar a pipeline
- `app/jobs/run_pipeline.py`: ponto de entrada alternativo para execucao direta
- `data/bronze`: dados crus em JSON Lines
- `data/silver`: dados tratados em parquet, particionados por localizacao
- `data/gold`: agregado analitico com quantidade de cervejarias por tipo e localizacao

## Como rodar

### Docker Compose

```bash
docker compose up --build
```

### Execucao local

```bash
pip install -r requirements.txt
luigi --module app.orchestration.luigi_pipeline BreweryMedallionPipeline --local-scheduler
```

Ou rode direto com Python:

```bash
python -m app.jobs.run_pipeline
```

## Camadas da arquitetura

### Bronze

- Salva o payload bruto da API em JSON Lines
- Particionamento por data de ingestao e `run_id`

### Silver

- Seleciona e padroniza colunas relevantes
- Deduplica por `id`
- Normaliza campos textuais em lowercase
- Persiste em parquet particionado por `country` e `state`

### Gold

- Gera agregado com quantidade de cervejarias por `brewery_type` e localizacao
- Persiste em parquet para consumo analitico

## Testes

```bash
pytest
```

## Monitoramento e alertas

Uma implementacao mais robusta de monitoramento poderia incluir:

- Alerta em falhas da task Luigi por Slack, Teams ou email
- Validacoes de qualidade de dados, como volume minimo por execucao, unicidade de `id` e percentual de nulos por coluna critica
- Metricas de tempo de execucao, volume ingerido e quantidade de registros por camada
- Dashboards e logs centralizados com Prometheus/Grafana, CloudWatch ou Datadog
- Alertas para quebra de contrato da API ou atraso de atualizacao

## Trade-offs

- Foi usado parquet no silver e gold por simplicidade operacional e compatibilidade com Spark
- O Luigi foi escolhido por ser leve para um case local, sem a sobrecarga de um Airflow
- Os dados sao persistidos localmente em disco para facilitar reproducao do desafio
