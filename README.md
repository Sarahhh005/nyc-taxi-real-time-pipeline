# 🚖 NYC Taxi Real-Time Analytics Pipeline

## 📌 Project Overview

The NYC Taxi Real-Time Analytics Pipeline is an end-to-end Big Data platform designed to process, analyze, and monitor NYC Yellow Taxi trip records using modern data engineering technologies.

The system simulates a real-time streaming environment by ingesting NYC Taxi trip data, publishing records through Kafka, processing them using Spark Structured Streaming, storing analytical results in ClickHouse, orchestrating workflows with Apache Airflow, and providing intelligent insights through an AI Agent.

---

## 🎯 Project Objectives

- Build a scalable real-time data pipeline.
- Process large-scale NYC Taxi trip data.
- Perform data cleaning and transformation.
- Store processed data for fast analytical queries.
- Automate workflow orchestration using Airflow.
- Generate business insights and forecasting using AI.
- Demonstrate modern Big Data architecture and best practices.

---

## 🏗️ System Architecture

```text
                            ┌─────────────┐
                            │   Airflow   │
                            └──────┬──────┘
                                   │
                                   ▼
                     ┌─────────────────────────┐
                     │ NYC Taxi Parquet Files  │
                     └───────────┬─────────────┘
                                 │
                                 ▼
                        ┌────────────────┐
                        │ Taxi Producer  │
                        └───────┬────────┘
                                │
                                ▼
                        ┌────────────────┐
                        │     Kafka      │
                        │  taxi-trips    │
                        └───────┬────────┘
                                │
                                ▼
                 ┌──────────────────────────┐
                 │ Spark Structured Stream  │
                 └────────────┬─────────────┘
                              │
                              ▼
                      ┌───────────────────┐
                      │    ClickHouse     │
                      └─────────┬─────────┘
                                │
                                ▼
                         ┌────────────┐
                         │ AI Agent   │
                         └─────┬──────┘
                               │
                               ▼
                    Insights & Forecasting
```

---

## ⚙️ Technology Stack

| Layer | Technology |
|---------|---------|
| Programming Language | Python |
| Data Source | NYC TLC Yellow Taxi Dataset |
| Streaming Platform | Apache Kafka |
| Processing Engine | Apache Spark Structured Streaming |
| Data Storage | ClickHouse |
| Workflow Orchestration | Apache Airflow |
| Containerization | Docker & Docker Compose |
| AI Layer | AI Agent |

---

## 📂 Dataset

The project uses the NYC Taxi & Limousine Commission (TLC) Yellow Taxi Trip Records dataset for the year 2025.

Dataset Source:

https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

Example files:

```text
yellow_tripdata_2025-01.parquet
yellow_tripdata_2025-02.parquet
...
yellow_tripdata_2025-12.parquet
```

Due to dataset size limitations, data files are not stored in this repository.

Place all downloaded files inside:

```text
data/
```

---

## 🔄 Pipeline Workflow

### 1. Data Ingestion

NYC Taxi trip records are stored as Parquet files and serve as the pipeline's data source.

### 2. Producer Layer

A custom Python producer reads trip records and publishes them to Kafka.

### 3. Streaming Layer

Kafka receives trip events through the `taxi-trips` topic and streams them to downstream consumers.

### 4. Processing Layer

Spark Structured Streaming consumes Kafka events and performs:

- Data Cleaning
- Data Validation
- Feature Engineering
- Real-Time Aggregations

### 5. Storage Layer

Processed records and aggregated results are stored in ClickHouse for analytical workloads.

### 6. Workflow Orchestration

Apache Airflow automates:

- Producer execution
- Spark job scheduling
- Data quality checks
- Pipeline monitoring
- Workflow management

### 7. AI Analytics Layer

The AI Agent analyzes processed data and generates intelligent insights, forecasts, and anomaly detection reports.

---

## 🤖 AI Agent Features

The AI Agent enables advanced analytics and business intelligence capabilities.

### Supported Use Cases

- Demand Forecasting
- Revenue Analysis
- Peak Hour Detection
- Trip Pattern Analysis
- Anomaly Detection
- Natural Language Data Exploration

### Example Questions

```text
What was the busiest pickup zone this week?

Which day generated the highest revenue?

Predict taxi demand for next weekend.

Show unusual increases in trip volume.

What are the top pickup locations by trip count?
```

---

## 📊 Business Questions Answered

The platform aims to answer questions such as:

- What are the busiest pickup and drop-off zones?
- Which hours experience the highest taxi demand?
- Which days generate the highest revenue?
- How does trip demand change over time?
- Can future taxi demand be predicted?
- Are there any unusual trip patterns or anomalies?

---

## 📁 Project Structure

```text
nyc-taxi-real-time-pipeline/
│
├── airflow/
│   ├── dags/
│   └── configs/
│
├── producer/
│   └── taxi_producer.py
│
├── spark/
│   └── streaming_job.py
│
├── clickhouse/
│
├── docker/
│
├── docs/
│
├── data/
│
├── docker-compose.yml
├── requirements.txt
├── .gitignore
└── README.md
```

---

## 🚀 Getting Started

### Clone Repository

```bash
git clone https://github.com/Sarahhh005/nyc-taxi-real-time-pipeline.git
cd nyc-taxi-real-time-pipeline
```

### Start Infrastructure

```bash
docker compose up -d
```

### Verify Running Containers

```bash
docker ps
```

Expected services:

```text
Kafka
ClickHouse
Airflow
```

---



## 🔮 Future Enhancements

- Real-Time Dashboard
- Advanced Forecasting Models
- Multi-Agent Analytics System
- Cloud Deployment
- Automated Reporting

---

## 📜 License

This project is developed for educational and academic purposes.