# NYC Taxi Real-Time Analytics Pipeline

An end-to-end, containerized Big Data platform that turns historical NYC Yellow Taxi trip records into a live event stream, cleans and aggregates them in flight, stores the results for millisecond-latency analytics, and exposes them through a natural-language AI agent.

**Stack:** Apache Kafka · Apache Spark Structured Streaming · ClickHouse · Apache Airflow · FastAPI + LangChain · React · Docker Compose

---

## Overview

The pipeline simulates a real-time production environment on top of publicly available NYC Taxi & Limousine Commission (TLC) trip data:

1. A Python producer reads NYC Yellow Taxi trip records from Parquet files and publishes them to Kafka as JSON messages, at a configurable rate.
2. Apache Spark Structured Streaming consumes the stream, validates and casts every record, and writes clean micro-batches into ClickHouse.
3. ClickHouse stores both raw trip-level records and pre-aggregated rollups for sub-second analytical queries.
4. Apache Airflow orchestrates the pipeline — checking that Kafka and ClickHouse are reachable before triggering the stream, with automatic retries on failure.
5. A FastAPI + LangChain AI agent lets users ask questions about the data in plain English, served through a React frontend.

The dataset covers the full year of **2025** NYC Yellow Taxi trip records, sourced from the official TLC archive.

---

## Architecture

```text
                 ┌───────────────┐
                 │    Airflow    │
                 │ Orchestration │
                 └───────┬───────┘
                         │
                         ▼
          ┌─────────────────────────┐
          │ NYC Taxi Parquet Files  │
          └────────────┬────────────┘
                        │
                        ▼
              ┌───────────────────┐
              │       Kafka       │
              │   (taxi-trips)    │
              └─────────┬─────────┘
                        │
                        ▼
      ┌───────────────────────────────┐
      │  Spark Structured Streaming   │
      └──────────────┬────────────────┘
                      │
                      ▼
              ┌───────────────────┐
              │     ClickHouse    │
              └─────────┬─────────┘
                        │
                        ▼
              ┌───────────────────┐
              │   AI Agent        │
              │ (FastAPI +        │
              │   LangChain)      │
              └─────────┬─────────┘
                        │
                        ▼
              ┌───────────────────┐
              │  React Frontend   │
              └───────────────────┘
```

---

## Technology Stack

| Layer | Technology |
| :--- | :--- |
| Language | Python |
| Event streaming | Apache Kafka |
| Stream processing | Apache Spark Structured Streaming |
| Analytical storage | ClickHouse |
| Orchestration | Apache Airflow (LocalExecutor, PostgreSQL metadata DB) |
| AI agent API | FastAPI + LangChain (SQL agent over ClickHouse, OpenAI-compatible LLM) |
| Frontend | React 18 + Vite |
| Containerization | Docker Compose |

---

## Project Structure

```text
nyc-taxi-real-time-pipeline/
├── ai_agent/               # FastAPI service exposing a LangChain SQL agent over ClickHouse
│   ├── main.py             # API entrypoint (/health, /ask, /ask/stream)
│   ├── agent.py            # LangChain SQL agent setup and streaming logic
│   ├── db.py                # ClickHouse connection helper
│   └── prompts.py          # Agent system prompt
├── airflow/ dags/          # Orchestration DAG (nyc_taxi_pipeline_dag)
├── clickhouse/             # Schema initialization and analytical queries
├── docs/                   # Project documentation and presentation
├── frontend/                # React + Vite chat UI for the AI agent
├── producer/                # Kafka producer, sample-data downloader, tests
├── spark/                   # Spark Structured Streaming job
├── docker-compose.yml       # Full infrastructure orchestration
├── requirements.txt         # Root-level Python dependencies (producer)
└── README.md
```

---

## Pipeline Workflow

### 1. Source data
NYC TLC Yellow Taxi trip records in Parquet format serve as the historical batch source. `producer/download_sample.py` downloads a monthly file and draws a reproducible random sample for local development.

### 2. Event streaming
`producer/taxi_producer.py` reads the Parquet file and publishes each trip as a JSON message to the Kafka topic `taxi-trips`. Traffic can be simulated at different rates:

| Mode | Delay | Description |
| :--- | :--- | :--- |
| `peak` | ~0.05s ± 40% jitter | Rush-hour message rate |
| `normal` (default) | ~0.5s ± 30% jitter | Typical daytime traffic |
| `offpeak` | ~2.0s ± 25% jitter | Overnight, low-demand hours |

### 3. Stream processing
`spark/spark_streaming.py` (Apache Spark Structured Streaming) subscribes to the Kafka topic and, before writing to ClickHouse via JDBC:
- Parses each JSON message against a fixed schema and casts fields to their target types.
- De-duplicates exact duplicate trip records and drops rows with missing pickup/dropoff timestamps.
- Validates: `fare_amount ≥ 0`, `passenger_count ≤ 8`, `dropoff_datetime > pickup_datetime`, pickup/dropoff zone IDs between 1–265, and `payment_type` between 0–6.

### 4. Analytical storage
ClickHouse (database `NYC_TAXI`) stores:
- **`taxi_trips`** — cleaned, trip-level records (`MergeTree`, ordered by `pickup_datetime`).
- **`hourly_demand`** — windowed trip count, average fare, and total revenue rollups (`ReplacingMergeTree`).
- **`zone_statistics`** — windowed trip count, revenue, and average tip by pickup zone (`ReplacingMergeTree`).

### 5. Orchestration
A single Airflow DAG, `nyc_taxi_pipeline_dag`, verifies Kafka and ClickHouse connectivity, then triggers the streaming simulation, with retry-on-failure built in.

### 6. AI agent
The FastAPI service in `ai_agent/` wraps a LangChain SQL agent around the ClickHouse `taxi_trips` schema, exposing:

| Endpoint | Method | Description |
| :--- | :--- | :--- |
| `/health` | GET | Liveness check |
| `/ask` | POST | `{"question": "..."}` → natural-language answer |
| `/ask/stream` | POST | `{"question": "..."}` → streamed text response |
| `/docs` | GET | Auto-generated Swagger UI |

The React frontend (`frontend/`) provides a chat console on top of this API.

---

## Getting Started

### Prerequisites
- Docker and Docker Compose
- Python 3.11+ (for running the producer locally)
- An OpenAI-compatible API key for the AI agent (defaults to an OpenRouter-compatible endpoint)

### 1. Clone the repository
```bash
git clone https://github.com/Sarahhh005/nyc-taxi-real-time-pipeline.git
cd nyc-taxi-real-time-pipeline
```

### 2. Configure environment
The AI agent reads `OPENAI_API_KEY` (and optionally `OPENAI_API_BASE`, `LLM_MODEL`) from the environment. Set these before starting the stack, e.g.:
```bash
export OPENAI_API_KEY=your_key_here
```

### 3. Start the infrastructure
```bash
docker compose up -d
```

This brings up Kafka, ClickHouse, Spark (master + worker), Airflow (Postgres, init, webserver, scheduler), the AI agent, and the frontend. Verify everything is running:
```bash
docker ps
```

| Service | URL |
| :--- | :--- |
| Airflow UI | http://localhost:8081 |
| Spark master UI | http://localhost:8080 |
| ClickHouse HTTP interface | http://localhost:8123 |
| AI Agent API / Swagger docs | http://localhost:8000/docs |
| Frontend | http://localhost:3000 |
| Kafka broker | localhost:9092 |

### 4. Prepare sample data
```bash
pip install -r requirements.txt
python producer/download_sample.py
```

### 5. Start the producer
```bash
python producer/taxi_producer.py --data data/yellow_tripdata_2025-01_sample_10000.parquet
```

Optional traffic modes:
```bash
python producer/taxi_producer.py --data data/yellow_tripdata_2025-01_sample_10000.parquet --mode peak
python producer/taxi_producer.py --data data/yellow_tripdata_2025-01_sample_10000.parquet --mode offpeak
```

---

## Roadmap

- Real-time alerting on unusual demand or revenue movement
- Predictive demand forecasting from historical patterns
- Cloud deployment (AWS / Azure) for elastic scale
- CI/CD integration for automated testing and deployment
- Automated, scheduled reporting to stakeholders
- Conversational memory for the AI agent (multi-agent system)

---

## License

No license file is currently included in this repository. Add one (e.g. MIT, Apache 2.0) before distributing or accepting external contributions.