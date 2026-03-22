FROM python:3.11-slim AS builder

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV VIRTUAL_ENV=/opt/venv
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

WORKDIR /build

RUN python -m venv "$VIRTUAL_ENV"

COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

FROM python:3.11-slim AS app

RUN apt-get update && apt-get install -y \
    openjdk-21-jre-headless \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV VIRTUAL_ENV=/opt/venv
ENV PYTHONPATH=/app
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

WORKDIR /app

COPY --from=builder /opt/venv /opt/venv
COPY app ./app
COPY requirements.txt .
COPY README.md .

RUN mkdir -p data/bronze data/silver data/gold

CMD ["luigi", "--module", "app.orchestration.luigi_pipeline", "BreweryMedallionPipeline", "--local-scheduler"]
