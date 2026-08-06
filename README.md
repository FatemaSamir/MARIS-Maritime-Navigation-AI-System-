# 🚢 MARIS – Maritime AI Real-time & Batch Intelligence System

> **An end-to-end Big Data platform that combines Lambda Architecture, Delta Lake, Apache Kafka, Apache Spark, Apache Airflow, Machine Learning, and FastAPI to deliver real-time maritime intelligence and historical analytics.**

<img width="1671" height="941" alt="Architecture" src="https://github.com/user-attachments/assets/a180d3da-080a-4bbf-a1f1-0fcc18afd1fe" />

---

## 📖 Overview

MARIS is a scalable maritime intelligence platform designed to process Automatic Identification System (AIS) vessel data through both **real-time streaming** and **batch processing** pipelines.

The system combines a **Lambda Architecture** with a **Delta Lake Medallion Architecture**, enabling low-latency vessel monitoring while maintaining high-quality historical analytics and AI-powered insights.

The platform supports:

- Real-time vessel monitoring
- AI-powered anomaly detection
- Vessel position prediction
- Collision risk alerts
- Maritime traffic congestion analysis
- Historical analytics
- Interactive dashboards
- RESTful APIs

---

# 🏗 System Architecture

MARIS consists of six major layers.

## 📦 Dataset & Project Environment

### Dataset

The project uses **Automatic Identification System (AIS)** vessel traffic data provided by the **NOAA MarineCadastre** program.

| Item | Details |
|------|---------|
| Dataset | NOAA MarineCadastre AIS Vessel Traffic |
| Source | https://hub.marinecadastre.gov/pages/vesseltraffic |
| Coverage | U.S. Coastal Waters |
| Time Scope | 7 consecutive daily files |
| Dataset Size | ~64 million AIS records |
| Format | CSV (converted to partitioned Parquet for processing) |

> **Dataset Source:** NOAA MarineCadastre – AIS Vessel Traffic  
> https://hub.marinecadastre.gov/pages/vesseltraffic

---

### Core Infrastructure

| Component | Version |
|----------|---------|
| PostgreSQL | 15 |
| Apache Kafka | 7.4 |
| Python | 3.x |

---

### Frameworks & Libraries

| Framework | Version |
|-----------|---------|
| Apache Spark | 3.4 |
| Delta Lake | 2.4 |
| FastAPI | Latest Stable |

---

### Processing Workflow

```
NOAA MarineCadastre AIS Dataset
            │
            ▼
      Raw CSV Files
            │
            ▼
 CSV Validation & Cleaning
            │
            ▼
  Partitioned Parquet Files
            │
            ├───────────────┐
            ▼               ▼
    Real-Time Pipeline   Batch Pipeline
```

The dataset is transformed from raw CSV files into partitioned Parquet files before being processed by the Lambda Architecture. This preprocessing improves storage efficiency, enables partition pruning, and accelerates Spark batch jobs while providing the streaming input used by the real-time pipeline.

---

## 2️⃣ Real-Time Speed Layer

The Speed Layer processes live vessel data with minimal latency.

```
Partitioned Parquet
        │
        ▼
kafka_producer.py
        │
        ▼
Apache Kafka
        │
        ▼
live_scorer.py
```

The `live_scorer.py` service performs:

- Live vessel scoring
- Near real-time analytics
- Anomaly detection
- Collision risk detection
- Latest vessel state updates

Real-time results are written directly into the PostgreSQL serving database.

---

## 3️⃣ Batch / Lakehouse Layer

Historical processing is performed independently using Apache Spark and Delta Lake.

```
Partitioned Parquet
        │
        ▼
Apache Spark
        │
        ▼
Delta Lake

Bronze
   │
Silver
   │
Gold
```

The Medallion Architecture organizes the data into three layers:

| Layer | Description |
|--------|-------------|
| Bronze | Raw validated data |
| Silver | Cleaned and feature-engineered data |
| Gold | Aggregated datasets for analytics and serving |

Batch jobs are orchestrated by **Apache Airflow**.

Airflow schedules:

- `bronze_job.py`
- `silver_job.py`
- `gold_job.py`
- Model training
- Model evaluation

> **Airflow orchestrates only the batch pipeline. The real-time speed layer runs independently.**

---

## 4️⃣ Machine Learning Layer

Machine learning models are trained using historical data from the Silver layer.

The platform includes:

| Model | Algorithm | Purpose |
|--------|-----------|----------|
| Anomaly Detection | Isolation Forest | Detect abnormal vessel behavior |
| Position Prediction | XGBoost | Predict vessel locations |
| Congestion Classification | Random Forest | Predict maritime congestion |

After training, the models are stored in the **Model Artifacts Registry** and loaded by the real-time scoring service for live inference.

---

## 5️⃣ Serving Layer

Both the real-time and batch pipelines publish their outputs to a centralized PostgreSQL serving database.

The serving schema stores:

- Live vessel states
- Alerts
- Historical vessel tracks
- Traffic density
- Daily statistics
- Analytical datasets

This layer acts as the single source of truth for all downstream applications.

---

## 6️⃣ API & Visualization Layer

The serving layer is exposed through **FastAPI** REST APIs.

Two visualization applications consume the APIs:

### React Dashboard

- Live vessel map
- Historical replay
- Alert monitoring
- Traffic congestion visualization

### Streamlit Dashboard

- Batch analytics
- Model insights
- Historical KPIs
- AI monitoring

---

# 🔄 End-to-End Pipeline

```
                 AIS CSV Files
                        │
                        ▼
              convert_csv.py
                        │
                        ▼
            Partitioned Parquet Files
                        │
         ┌──────────────┴──────────────┐
         │                             │
         ▼                             ▼
  Real-Time Speed Layer        Batch Lakehouse Layer
         │                             │
 kafka_producer.py              Apache Spark
         │                             │
         ▼                             ▼
   Apache Kafka                 Delta Lake
         │                Bronze → Silver → Gold
         ▼                             │
   live_scorer.py                      │
         │                             │
         ├──────────────┐              │
         ▼              ▼              ▼
 Live Scores       Alerts       Historical Analytics
               \       │       /
                \      │      /
                 ▼     ▼     ▼
             PostgreSQL Serving Layer
                      │
                      ▼
                 FastAPI REST API
                 /               \
                ▼                 ▼
      React Dashboard    Streamlit Dashboard
```

---

# 🛠 Technology Stack

### Data Engineering

- Apache Kafka
- Apache Spark
- Delta Lake
- Apache Airflow
- PostgreSQL

### Machine Learning

- Scikit-learn
- XGBoost

### Backend

- FastAPI
- SQLAlchemy

### Frontend

- React
- Streamlit

### Programming Language

- Python


---

# 🚀 Key Capabilities

- Lambda Architecture
- Delta Lake Medallion Architecture
- Real-time stream processing
- Batch analytics
- AI-powered maritime intelligence
- Feature engineering
- Machine learning inference
- RESTful APIs
- Interactive dashboards
- Scalable data pipelines

---

# 🔮 Future Enhancements

- Kubernetes deployment
- MLflow Model Registry
- Cloud Data Lake integration
- CI/CD pipeline
- Multi-node Spark cluster
- Multi-broker Kafka deployment
- Monitoring with Prometheus & Grafana

---

# 📸 Screenshots

```
Demo/
├── MARIS.mp4
├── architecture.png
├── react-dashboard.png
├── streamlit-dashboard.png
├── vessel-map.png
├── anomaly-dashboard.png
└── traffic-heatmap.png
```

---



# 👩‍💻 Author

**Fatema Samir**

**Hassnaa Saady**

**Lamiaa Nasser**

 Data Engineer | Big Data & AI Engineer

 Copyright (c) 2026 Fatema Samir

All Rights Reserved.

This source code is proprietary.
