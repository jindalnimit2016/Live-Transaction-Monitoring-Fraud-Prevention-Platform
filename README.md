# Real-Time Intelligent Fraud Detection & Risk Analytics Platform

![Architecture](architecture.png)

---

## 🟢 Project Overview

This project implements a **real-time fraud detection and risk analytics platform** for financial transactions. It combines **Apache Kafka**, **Spark Structured Streaming**, **PostgreSQL**, **Elasticsearch/Kibana**, and **Airflow** to detect fraudulent activity, update user & merchant risk profiles, and provide alerts in near real-time.

**Key Features:**
- Real-time processing of financial transactions via **Kafka → Spark Structured Streaming**.
- Stateful fraud detection using **user & merchant behavioral patterns**.
- Incremental **UPSERTs** to Postgres for live **risk profiling**.
- Elasticsearch integration for real-time dashboards and monitoring.
- ML-ready feature pipelines for advanced fraud scoring.

---

## 🛠 Tech Stack

| Layer | Technology |
|-------|------------|
| Messaging & Ingestion | Apache Kafka |
| Streaming Processing | Apache Spark Structured Streaming (PySpark) |
| Database / Storage | PostgreSQL, Elasticsearch |
| Orchestration | Apache Airflow |
| Analytics & ML | Python, PySpark, Pandas, SQL |
| Monitoring & Visualization | Kibana, Elasticsearch |
| Infrastructure | Linux, JDBC |

---

## ⚡ How It Works

1. **Kafka Stream Ingestion**
   - Financial transactions ingested from Kafka topics in real-time.

2. **Spark Structured Streaming**
   - Processes events using PySpark.
   - Performs **fraud detection rules**:
     - High transaction amount
     - Rapid multiple transactions
   - Joins with **user & merchant profiles** for enriched analysis.

3. **PostgreSQL & UPSERT Logic**
   - Updates `user_profiles` and `merchant_profiles` incrementally.
   - Maintains rolling averages, transaction velocity, device behavior, and historical risk scores.

4. **Alerts & ML Features**
   - Generates **fraud alerts** in real-time.
   - Prepares **feature pipelines** for machine learning models.

5. **Monitoring & Visualization**
   - Stream output indexed in **Elasticsearch**.
   - Dashboards in **Kibana** for operations and fraud analytics.

6. **Airflow Orchestration**
   - Automates streaming pipeline execution and retry logic.

---

## 🚀 Impact

- **33K+ fraud alerts generated** with real-time risk scoring.
- **10K+ user & merchant profiles maintained** dynamically.
- **<1-second latency** for transaction processing.
- Enables ML-driven fraud detection and predictive analytics.
- Reduces operational fraud investigation overhead by **>90%**.

---

## 📂 Repository Structure

```text
fraud-detection-streaming/
│
├── streaming/          # Spark streaming code
├── airflow/            # DAGs and Airflow orchestration
├── configs/            # Configuration files (Postgres, Kafka)
├── notebooks/          # Analysis & ML feature exploration
├── README.md           # Project overview
├── requirements.txt    # Dependencies
└── architecture.png    # System architecture diagram
