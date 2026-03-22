## BEES Breweries Case

Projeto em Python + PySpark para consumir a API Open Brewery DB e persistir os dados em uma arquitetura medallion com camadas bronze, silver e gold.

## Como esta solução atende o case

- API: consome a Open Brewery DB com paginação e retry simples
- Orquestração: usa Luigi com scheduler centralizado, retry de task e execução batch
- Linguagem: Python + PySpark
- Testes: suíte unitária para cliente da API e regras de pipeline
- Containerização: Docker + Docker Compose
- Medallion: bronze em JSON Lines, silver em parquet particionado por localização e gold agregado por tipo e local
- Monitoramento: logs estruturados, relatório JSON por `run_id`, checks de qualidade e proposta de alerting
- Streamlit: interface opcional, fora do escopo principal do case

## O que foi implementado

- Ingestão da API `https://api.openbrewerydb.org/v1/breweries`
- Pipeline medallion com persistência local em `data/`
- Orquestração com Luigi, retries de task e scheduler centralizado
- Containerização com Docker
- Monitoramento com logs estruturados, relatório de execução por `run_id` e dashboard opcional em Streamlit
- Testes unitários para cliente da API e helpers da pipeline

## Estrutura

- `app/clients/open_brewery.py`: cliente HTTP com paginação e retry simples
- `app/pipeline.py`: regras de ingestão e transformação das camadas bronze, silver e gold
- `app/orchestration/luigi_pipeline.py`: task Luigi para disparar a pipeline
- `app/monitoring.py`: logs estruturados e persistência de relatórios de execução
- `app/jobs/run_pipeline.py`: ponto de entrada alternativo para execução direta
- `data/bronze`: dados crus em JSON Lines
- `data/silver`: dados tratados em parquet, particionados por localização
- `data/gold`: agregado analítico com quantidade de cervejarias por tipo e localização
- `data/monitoring`: relatórios JSON com status, tempos e checks de qualidade por execução
- `app/ui/streamlit_app.py`: visualização opcional da camada gold

## Como rodar

### Docker Compose

Execução principal do case:

```bash
docker compose up --build
```

O scheduler do Luigi fica em `http://localhost:8082`.
O `spark-app` é um job batch: ele sobe, executa a pipeline e encerra com `exit code 0` quando termina.

### Forma de execução

```bash
docker compose up --build
docker compose run --rm spark-app pytest
```

Ao final da execução, os artefatos esperados ficam em:

- `data/bronze/`
- `data/silver/`
- `data/gold/`
- `data/monitoring/`

### Execução local

Instale as dependências:

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

## Opcional: Streamlit

O dashboard em Streamlit é apenas um adicional para visualização da camada gold. Ele não faz parte do fluxo principal pedido no case.

Para subir a interface opcional:

```bash
docker compose --profile optional up --build streamlit-app
```

Ou, localmente:

```bash
pip install -r requirements-streamlit.txt
streamlit run app/ui/streamlit_app.py
```

Depois abra `http://localhost:8501`.

Observação: a interface opcional não usa `pandas`.

## Camadas da arquitetura

### Bronze

- Salva o payload bruto da API em JSON Lines
- Particionamento por data de ingestão e `run_id`

### Silver

- Seleciona e padroniza colunas relevantes
- Deduplica por `id`
- Normaliza campos textuais em lowercase
- Persiste em parquet particionado por `country` e `state`
- O campo `city` continua disponível para análise e para a agregação da camada gold

### Gold

- Gera agregado com quantidade de cervejarias por `brewery_type` e localização
- Persiste em parquet para consumo analítico

## Testes

```bash
pytest
```

## Monitoramento e alertas

Já implementado no projeto:

- Logs estruturados em JSON por etapa da pipeline com `run_id`, duração e paths gerados
- Relatório por execução salvo em `data/monitoring/run_id=...json`
- Validações de qualidade para payload vazio, `id` nulo, consistência entre silver e gold e alertas de colunas críticas ausentes
- Retry da task Luigi e UI do scheduler para acompanhamento de execuções

Como eu expandiria isso em produção:

- Alerta em falhas da task Luigi por Slack, Teams ou email
- Validações de qualidade de dados, como volume mínimo por execução, unicidade de `id` e percentual de nulos por coluna crítica
- Métricas de tempo de execução, volume ingerido e quantidade de registros por camada
- Dashboards e logs centralizados com Prometheus/Grafana, CloudWatch ou Datadog
- Alertas para quebra de contrato da API ou atraso de atualização

## Scheduling

- No ambiente local, o Luigi scheduler fica exposto via Docker Compose para observabilidade e controle centralizado
- Para recorrência em produção, eu agendaria a task com cron, GitHub Actions schedule, ECS Scheduled Task ou outro scheduler externo chamando o job batch
- O `spark-app` permanece batch e encerra com `exit code 0` quando a execução termina, o que facilita reuso em agendadores

## Decisões e trade-offs

- Foi usado parquet no silver e gold por simplicidade operacional e compatibilidade com Spark
- O Luigi foi escolhido por ser leve para um case local, sem a sobrecarga de um Airflow
- Os dados são persistidos localmente em disco para facilitar reprodução do desafio
- A silver ficou particionada por `country` e `state`, e não por `city`, porque na prática isso reduziu muito a quantidade de partições pequenas e melhorou a escrita local do Spark em volume montado
- O monitoramento implementado cobre observabilidade local e qualidade básica dos dados; integrações reais com Slack, email ou ferramentas externas ficaram como próximo passo natural

## Validação executada

- Pipeline validada via `docker compose up --build`
- Scheduler do Luigi validado em `http://localhost:8082`
- Dashboard opcional validado em `http://localhost:8501`
- Testes validados via `docker compose run --rm spark-app pytest`
