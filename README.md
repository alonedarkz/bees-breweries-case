## BEES Breweries Case

Projeto em Python + PySpark para consumir a API Open Brewery DB e persistir os dados em uma arquitetura medallion com camadas bronze, silver e gold.

## O que foi implementado

- Ingestao da API `https://api.openbrewerydb.org/v1/breweries`
- Pipeline medallion com persistencia local em `data/`
- Orquestracao com Luigi, retries de task e scheduler centralizado
- Containerizacao com Docker
- Monitoramento com logs estruturados, relatorio de execucao por `run_id` e dashboard Streamlit
- Testes unitarios para cliente da API e helpers da pipeline

## Estrutura

- `app/clients/open_brewery.py`: cliente HTTP com paginacao e retry simples
- `app/pipeline.py`: regras de ingestao e transformacao das camadas bronze, silver e gold
- `app/orchestration/luigi_pipeline.py`: task Luigi para disparar a pipeline
- `app/monitoring.py`: logs estruturados e persistencia de relatorios de execucao
- `app/jobs/run_pipeline.py`: ponto de entrada alternativo para execucao direta
- `data/bronze`: dados crus em JSON Lines
- `data/silver`: dados tratados em parquet, particionados por localizacao
- `data/gold`: agregado analitico com quantidade de cervejarias por tipo e localizacao
- `data/monitoring`: relatorios JSON com status, tempos e checks de qualidade por execucao

## Como rodar

### Docker Compose

```bash
docker compose up --build
```

Depois abre o dashboard em `http://localhost:8501`.
O scheduler do Luigi fica em `http://localhost:8082`.

### Execucao local

Instale as dependencias:

```bash
pip install -r requirements.txt
```

Execute a pipeline com Luigi:

```bash
luigid --address 0.0.0.0 --port 8082
luigi --module app.orchestration.luigi_pipeline BreweryMedallionPipeline --scheduler-host localhost --scheduler-port 8082
```

Ou rode direto com Python:

```bash
python -m app.jobs.run_pipeline
```

### Streamlit local

Instale as dependencias extras:

```bash
pip install -r requirements-streamlit.txt
```

Suba a interface:

```bash
streamlit run app/ui/streamlit_app.py
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

Ja implementado no projeto:

- Logs estruturados em JSON por etapa da pipeline com `run_id`, duracao e paths gerados
- Relatorio por execucao salvo em `data/monitoring/run_id=...json`
- Validacoes de qualidade para payload vazio, `id` nulo, consistencia entre silver e gold e alertas de colunas criticas ausentes
- Retry da task Luigi e UI do scheduler para acompanhamento de execucoes

Como eu expandiria isso em producao:

- Alerta em falhas da task Luigi por Slack, Teams ou email
- Validacoes de qualidade de dados, como volume minimo por execucao, unicidade de `id` e percentual de nulos por coluna critica
- Metricas de tempo de execucao, volume ingerido e quantidade de registros por camada
- Dashboards e logs centralizados com Prometheus/Grafana, CloudWatch ou Datadog
- Alertas para quebra de contrato da API ou atraso de atualizacao

## Scheduling

- No ambiente local, o Luigi scheduler fica exposto via Docker Compose para observabilidade e controle centralizado
- Para recorrencia em producao, eu agendaria a task com cron, GitHub Actions schedule, ECS Scheduled Task ou outro scheduler externo chamando o job batch
- O `spark-app` permanece batch e encerra com `exit code 0` quando a execucao termina, o que facilita reuso em agendadores

## Trade-offs

- Foi usado parquet no silver e gold por simplicidade operacional e compatibilidade com Spark
- O Luigi foi escolhido por ser leve para um case local, sem a sobrecarga de um Airflow
- Os dados sao persistidos localmente em disco para facilitar reproducao do desafio
