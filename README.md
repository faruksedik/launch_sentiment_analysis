# LaunchSentiment – Wikipedia Pageviews Data Pipeline

## Project Overview

**LaunchSentiment** is a capstone data engineering project that demonstrates how to design, build, and operate a **production-style ETL data pipeline using Apache Airflow**.

The pipeline ingests hourly Wikipedia pageviews data, processes it, and stores curated records in a PostgreSQL database. The processed data can then be used by downstream analytics or machine learning systems to approximate **public sentiment around major technology companies**.

### Core Assumption

- 📈 **Increase in Wikipedia pageviews** → higher public interest → potentially positive market sentiment
- 📉 **Decrease in pageviews** → reduced interest → potentially negative market sentiment

This project focuses on **data engineering concerns only** (ingestion, orchestration, transformation, storage), not stock prediction itself.

---

## Business Scenario

Imagine you are hired as a **Data Engineer** at a data consulting firm. Your task is to build the data backbone for a sentiment-driven analytics product called **LaunchSentiment**.

Your responsibility is to:
- Ingest large-scale public datasets reliably
- Design a backfill-safe pipeline
- Ensure clean transformations
- Load data efficiently into a relational database

For validation and simplicity, the pipeline tracks pageviews for **five major technology companies**:

- Amazon
- Apple
- Facebook
- Google
- Microsoft

---

## Data Source

- **Provider:** Wikimedia Foundation
- **Dataset:** Wikipedia Pageviews (hourly aggregates)
- **Format:** Gzipped text files (`.gz`)
- **Availability:** Public (since 2015)

### Reference

- Pageviews index: https://dumps.wikimedia.org/other/pageviews

Each hourly file:
- ~50 MB compressed
- ~200–250 MB uncompressed

---

## Pipeline Features

- **Dynamic Download:**
  - Automatically computes execution timestamps
  - Applies a **2-month offset** to avoid incomplete or delayed data

- **Streaming Extract:**
  - Efficient `.gz` → `.txt` extraction
  - Minimal memory usage

- **Deterministic Transform:**
  - Filters only target companies
  - Generates SHA-256 surrogate keys
  - Produces clean CSV output

- **Efficient Load:**
  - Uses PostgreSQL `COPY FROM STDIN`
  - Optimized for large file ingestion

- **Airflow-Native Design:**
  - Modular tasks
  - Idempotent execution
  - XCom-driven data flow

---

## Project Structure

```
launch_sentiment_analysis/
├── include/
│   ├── data/
│   │   ├── raw/
│   │   │   └── pageviews_YYYYMMDD-HH.gz       # Downloaded hourly gzip file
│   │   ├── staging/
│   │   │   ├── pageviews_YYYYMMDD-HH.txt      # Extracted raw text file
│   │   │   └── pageviews_YYYYMMDD-HH.csv      # Transformed CSV output
│   ├── logs/
│   │   └── pipeline.log                       # Application-level logs
│   ├── scripts/
|   |   ├── sql/
│   │   │   └── load_to_postgress.sql          # Sql script for data loading
│   │   ├── download_pageviews.py              # Data ingestion logic
│   │   ├── extract_pageviews.py               # Gzip extraction logic
│   │   ├── transform_pageviews.py             # Transformation & filtering
│   │   ├── db.py                              # Database load & analysis
│   │   └── logger.py                          # Centralized logging utility
├── launch_sentiment_analysis_dag.py            # Airflow DAG definition
└── requirements.txt
```

This structure enforces a **clean separation of concerns** between ingestion, transformation, persistence, and orchestration.

---

## Pipeline Architecture

```
[Download] → [Extract] → [Transform] → [Load]
```

Each task passes file paths via **Airflow XCom**, keeping the pipeline declarative and traceable.

---

## Prerequisites

Before running this project, ensure you have:

- Docker & Docker Compose
- Python 3.10+
- A local PostgreSQL installation

---

## Step-by-Step Setup Guide

### 1. Clone the Repository

```bash
git clone https://github.com/faruksedik/launch_sentiment_analysis.git
cd launch_sentiment_analysis
```

---

### 2. Set Up PostgreSQL (Local)

This project assumes PostgreSQL is **running on your local machine**, not inside Docker.

#### Create Database and User

```sql
CREATE DATABASE launch_sentiment;
CREATE USER launch_user WITH PASSWORD 'strong_password';
GRANT ALL PRIVILEGES ON DATABASE launch_sentiment TO launch_user;
```

Ensure PostgreSQL is listening on port `5432`.

---

### 3. Configure Airflow Connection (PostgreSQL)

In the **Airflow UI**:

1. Go to **Admin → Connections**
2. Create a new connection:

| Field | Value |
|------|------|
| Conn Id | `postgres_connection` |
| Conn Type | `Postgres` |
| Host | `host.docker.internal` |
| Schema | `launch_sentiment` |
| Login | `launch_user` |
| Password | `strong_password` |
| Port | `5432` |

> `host.docker.internal` allows Docker containers to reach services running on the host machine.

---

### 4. Set Airflow Variables

In **Admin → Variables**, create the following:

| Key | Example Value |
|----|---------------|
| `PAGEVIEWS_SENTIMENT_ANALYSIS_RAW_DATA_DIR` | `/opt/airflow/include/data/raw` |
| `PAGEVIEWS_SENTIMENT_ANALYSIS_STAGING_DATA_DIR` | `/opt/airflow/include/data/staging` |
| `PAGEVIEWS_SENTIMENT_ANALYSIS_SQL_FILE_DIR` | `/opt/airflow/include/scripts/sql` |

---

### 5. Install Python Dependencies

```bash
pip install -r requirements.txt
```

---

### 6. Start Airflow

```bash
docker compose up --build
```

Access the Airflow UI:

```
http://localhost:8080
```

---

### 7. Run the Pipeline

- Enable the DAG: `launch_sentiment_analysis_dag`
- Trigger manually or allow scheduled execution
- Monitor logs for each task

---

## Logging & Observability

- All scripts use a centralized logger via `get_logger`.
- Logs include:
  - Download progress
  - Extraction completion
  - Transformation row counts
  - PostgreSQL load status
- Errors are fully traceable for debugging.

---

## Backfill & Idempotency

- Pipeline uses execution timestamps
- Applies a fixed **2-month offset**
- Safe for historical backfills
- File naming is deterministic across runs

---

## Author

**Faruk Sedik**  
Data Engineer | Backend Developer  
Focused on building scalable, production-grade data systems

---

## License

This project is for educational and demonstration purposes only.

