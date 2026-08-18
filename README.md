# 🚖 NYC Taxi Real-Time Analytics Pipeline

## 📌 Project Overview

The NYC Taxi Real-Time Analytics Pipeline is an end-to-end Big Data and AI-powered analytics platform designed to process, analyze, and monitor NYC Yellow Taxi trip records using modern data engineering technologies.

The system simulates a real-time streaming environment by ingesting NYC Taxi trip data, publishing records through Apache Kafka, processing them with Spark Structured Streaming, storing analytical results in ClickHouse, orchestrating workflows with Apache Airflow, visualizing insights through Apache Superset, and generating intelligent business insights using an AI Agent.

This project demonstrates how modern Data Engineering, Business Intelligence, and Artificial Intelligence can work together to build a scalable real-time analytics ecosystem.

---

## 🎯 Project Objectives

- Build a scalable real-time data pipeline.
- Process large-scale NYC Taxi trip data.
- Perform data cleaning, validation, and transformation.
- Store processed data for high-performance analytical queries.
- Automate workflow orchestration using Apache Airflow.
- Deliver interactive dashboards and KPIs.
- Generate AI-powered insights and forecasts.
- Demonstrate modern Big Data architecture and industry best practices.

---

## 🏗️ System Architecture

```text
                            ┌─────────────┐
                            │   Airflow   │
                            │Orchestration│
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
               ┌────────────────┴────────────────┐
               ▼                                 ▼

      ┌─────────────────┐             ┌─────────────────┐
      │ Apache Superset │             │    AI Agent     │
      │   Dashboards    │             │ Analytics Layer │
      └────────┬────────┘             └────────┬────────┘
               │                               │
               └───────────────┬───────────────┘
                               ▼

                Business Insights & Forecasts
```

---

## ⚙️ Technology Stack

| Layer | Technology |
|---------|---------|
| Programming Language | Python |
| Data Source | NYC TLC Yellow Taxi Dataset |
| Streaming Platform | Apache Kafka |
| Processing Engine | Apache Spark Structured Streaming |
| Analytical Database | ClickHouse |
| Workflow Orchestration | Apache Airflow |
| Visualization Layer | Apache Superset |
| AI Layer | FastAPI + LangChain + OpenAI |
| Containerization | Docker & Docker Compose |

---

## 📂 Dataset

The project uses the NYC Taxi & Limousine Commission (TLC) Yellow Taxi Trip Records dataset for the year 2025.

### Dataset Source

https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

### Example Files

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

NYC Taxi trip records are stored as Parquet files and serve as the pipeline's primary data source.

### 2. Producer Layer

A custom Python producer reads taxi trip records from Parquet files and publishes them into Kafka topics.

**Input**

```text
NYC Taxi Parquet Files
```

**Output**

```text
Kafka Messages
```

---

### 3. Streaming Layer

Apache Kafka acts as the central event streaming platform responsible for transporting trip events between pipeline components.

**Topic**

```text
taxi-trips
```

**Input**

```text
Taxi Trip Events
```

**Output**

```text
Real-Time Event Stream
```

---

### 4. Processing Layer

Spark Structured Streaming consumes Kafka events and performs:

- Data Cleaning
- Data Validation
- Missing Value Handling
- Feature Engineering
- Real-Time Aggregations
- Business Metric Computation

**Input**

```text
Kafka Stream
```

**Output**

```text
Processed & Enriched Taxi Data
```

---

### 5. Storage Layer

Processed records and analytical aggregates are stored inside ClickHouse.

Example analytical tables:

- trips
- hourly_demand
- daily_revenue
- zone_statistics

**Input**

```text
Processed Streaming Data
```

**Output**

```text
Analytics-Ready Tables
```

---

### 6. Workflow Orchestration

Apache Airflow automates and manages pipeline execution.

Responsibilities include:

- Producer Scheduling
- Spark Job Scheduling
- Data Quality Checks
- Workflow Monitoring
- Failure Recovery
- Automated Reporting

---

### 7. Analytics Layer

Apache Superset provides interactive dashboards and business intelligence visualizations built directly on top of ClickHouse.

The dashboard layer enables stakeholders to monitor system performance and business metrics in near real time.

---

### 8. AI Analytics Layer

The AI Agent consumes analytical data and generates intelligent insights, forecasts, and recommendations.

Capabilities include:

- Demand Forecasting
- Revenue Analysis
- Trend Detection
- Anomaly Detection
- Natural Language Analytics
- Automated Insight Generation

---

## 📈 Analytics & Visualization

Apache Superset delivers enterprise-grade dashboards and analytical reports.

### Dashboard Capabilities

- Revenue Monitoring
- Demand Analysis
- Peak Hours Analysis
- Pickup Zone Insights
- Drop-off Zone Insights
- Operational KPIs
- Trend Monitoring
- Time-Series Analysis

These dashboards provide decision-makers with near real-time visibility into taxi operations and business performance.

---

## 🤖 AI Agent Features

The AI Agent transforms raw analytics into actionable business intelligence.

### Natural Language Analytics

Users can interact with the platform using natural language rather than writing SQL queries.

#### Example Queries

```text
Which pickup zone generated the highest revenue this month?

What was the busiest pickup location this week?

Show the top 10 zones by trip count.

What is the average trip distance by day?
```

---

### Demand Forecasting

The AI Agent predicts future taxi demand using historical trip patterns.

Examples:

- Predict next week's demand
- Forecast peak traffic periods
- Estimate future trip volumes
- Predict revenue trends

---

### Anomaly Detection

Automatically identify unusual operational patterns such as:

- Sudden demand spikes
- Revenue drops
- Unexpected traffic behavior
- Irregular trip activity

---

### Automated Insight Generation

The AI Agent generates executive-style summaries and recommendations.

Example:

> Taxi demand increased by 18% during weekend evenings, primarily driven by airport-related trips.

---

## 📊 Business Questions Answered

The platform aims to answer questions such as:

- What are the busiest pickup zones?
- What are the busiest drop-off zones?
- Which hours experience the highest demand?
- Which days generate the highest revenue?
- How does trip demand change over time?
- What are the most profitable routes?
- Can future taxi demand be predicted?
- Are there unusual demand spikes?
- How do passenger trends vary across locations?

---

## 🎯 Expected Business Outcomes

This platform enables transportation analytics teams to:

- Monitor taxi demand in real time.
- Identify high-revenue zones.
- Understand passenger behavior.
- Detect operational anomalies.
- Forecast future demand.
- Optimize transportation planning.
- Support data-driven decision making.

---

## 📁 Project Structure

```text
nyc-taxi-real-time-pipeline/
│
├── airflow/
│   ├── dags/
│   ├── configs/
│   └── logs/
│
├── producer/
│   ├── taxi_producer.py
│   ├── download_sample.py
│   ├── README.md
│   └── data/
│       └── .gitkeep
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
Superset
```

### Download Dataset

Download and sample the NYC Yellow Taxi dataset (creates a ~0.3 MB sample):

```bash
pip install -r requirements.txt
python producer/download_sample.py
```

### Run the Kafka Producer

```bash
python producer/taxi_producer.py --data data/yellow_tripdata_2025-01_sample_10000.parquet
```

Available traffic modes:

```bash
python producer/taxi_producer.py --data data/yellow_tripdata_2025-01_sample_10000.parquet --mode peak
python producer/taxi_producer.py --data data/yellow_tripdata_2025-01_sample_10000.parquet --mode offpeak
```

See [producer/README.md](producer/README.md) for full documentation and all options.

---

## 🔮 Future Enhancements

- Multi-Agent Analytics System
- Real-Time Alerting
- Predictive Demand Forecasting
- RAG-Powered Knowledge Assistant
- Cloud Deployment (AWS / Azure)
- CI/CD Integration
- Automated Reporting & Notifications

---

## 👥 Team

Developed as a collaborative Big Data Engineering project focused on real-time analytics, scalable data processing, and AI-powered business intelligence.

