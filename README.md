# LaunchSentiment – Wikipedia Pageviews Data Pipeline

## Project Overview
This capstone project demonstrates the design and implementation of a **data pipeline orchestrated with Apache Airflow**. The pipeline ingests, processes, stores, and analyzes Wikipedia pageviews data to support a sentiment-analysis-driven stock market prediction tool called **LaunchSentiment**.

The underlying assumption is simple:
- **Increase in Wikipedia pageviews** → positive public sentiment → potential stock price increase
- **Decrease in pageviews** → loss of interest → potential stock price decrease

This project focuses on building the *first version* of the data pipeline that processes Wikipedia pageviews for a **single hour** and performs a simple analytical query.

---

## Business Scenario
For example, one is hired as a **Data Engineer** by a data consulting organization tasked with building LaunchSentiment. The system leverages publicly available Wikipedia pageviews data to approximate market sentiment for publicly traded companies.

For validation and simplicity, the pipeline tracks pageviews for **five major companies**:
- Amazon
- Apple
- Facebook
- Google
- Microsoft

---

## Data Source

- **Provider:** Wikimedia Foundation
- **Dataset:** Wikipedia Pageviews (hourly aggregates)
- **Format:** Gzipped text files
- **Availability:** Public (since 2015)

### Relevant Links
- Pageviews index: https://dumps.wikimedia.org/other/pageviews
- December 2025 data: https://dumps.wikimedia.org/other/pageviews/2025/2025-12/

Each hourly file is:
- ~50 MB (compressed)
- ~200–250 MB (uncompressed)

---

## Project Objective

Build an **Airflow DAG** that:
1. Downloads Wikipedia pageviews data for **one specific hour**
2. Extracts and processes the data
3. Filters records for the five selected companies
4. Loads transformed data into a database
5. Performs a simple analysis to identify the **most viewed company page** for that hour

---

## Project Structure

```
launch_sentiment_analysis/
├── include/
│   ├── data/
│   │   ├── raw/
│   │   │   └── pageviews-20251210-160000.gz   # Downloaded hourly gzip file
│   │   ├── staging/
│   │   │   ├── pageviews.txt                  # Extracted raw text file
│   │   │   └── pageviews.csv                  # Transformed CSV output
│   ├── logs/
│   │   ├── analysis_result.log                # Persistent analysis output
│   │   └── pipeline.log                       # Application-level logs
│   ├── scripts/
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

### High-Level Workflow

```
[Download] → [Extract] → [Transform] → [Load] → [Analyze]
```

### DAG Task Breakdown

1. **Download Pageviews**
   - Fetches a single hourly gzip file from Wikimedia
   - Uses streaming download for memory efficiency

2. **Extract File**
   - Decompresses the `.gz` file into raw text

3. **Transform Data**
   - Filters English (en) Wikipedia pages only
   - Selects the five target companies
   - Splits execution timestamp into:
     - year
     - month
     - day
     - hour

4. **Load to PostgreSQL**
   - Creates table if it does not exist
   - Inserts transformed records
   - Ensures idempotency with primary keys and conflict handling

5. **Analyze Results**
   - Aggregates pageviews
   - Determines the most viewed company page
   - Logs results to Airflow logs and a persistent log file

---

## Database Schema

```sql
CREATE TABLE wikipedia_pageviews (
    page_title_id INTEGER PRIMARY KEY,
    page_title TEXT NOT NULL,
    pageviews INTEGER NOT NULL,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    day INTEGER NOT NULL,
    hour INTEGER NOT NULL
);
```

---

## Analysis Output

The pipeline produces a clear, human-readable report such as:

```
Most viewed Wikipedia page: 'Amazon' | Total views: 152340 | Time window: 2025-12-10 at 16:00
```

Results are written to:
- Airflow task logs
- `/opt/airflow/dags/launch_sentiment_analysis/include/logs/analysis_result.log`

---

## Database Setup (Local PostgreSQL)

The PostgreSQL database used by this pipeline is expected to run **on the local machine**, outside Docker, while Airflow runs inside Docker containers.

To enable communication between Airflow (Docker) and the local PostgreSQL instance, the special hostname below is used.

### Database Connection Details

| Field | Value |
|------|------|
| Host | `host.docker.internal` |
| Database | `dbname` |
| User | `postgres_user` |
| Password | `postgres_password` |
| Port | `5432` |

> **Why `host.docker.internal`?**  
> This hostname allows Docker containers to securely access services running on the host machine without exposing ports unnecessarily.

### Credential Management

- Database credentials are **not hard-coded** in the DAG or scripts
- Credentials are managed using **Airflow Variables**
- This ensures:
  - Improved security
  - Easy environment configuration
  - Clean separation between code and secrets

---

## Configuration & Credentials Management

- Database credentials are **not hard-coded**
- PostgreSQL connection details are managed using **Airflow Variables**
- This ensures:
  - Better security
  - Environment flexibility (local, staging, production)
  - Cleaner DAG code


---

## Technologies Used

- **Apache Airflow** – workflow orchestration
- **Python 3.11** – pipeline logic
- **PostgreSQL** – data storage and analysis
- **apache-airflow-providers-postgres** – database integration via PostgresHook
- **Docker & Docker Compose** – environment setup
- **Wikimedia Pageviews Dataset** – data source

---

## Best Practices Implemented

- Idempotent data loads (`ON CONFLICT DO NOTHING`)
- Streaming downloads for large files
- Structured logging with function-level context
- Automatic retries and failure visibility via Airflow
- Transaction-safe database operations
- Clear separation of concerns (extract, transform, load, analyze)

---

## How to Run the Pipeline

1. Clone the repository
2. Start Airflow using Docker Compose:

```bash
docker compose up --build
```

3. Open Airflow UI:

```
http://localhost:8080
```

4. Trigger the DAG manually or wait for the scheduled run

---

## Deliverables Checklist

✔ Airflow DAG orchestrating the pipeline  
✔ End-to-end runnable data pipeline  
✔ Database-loaded transformed data  
✔ Analytical query for most viewed company  
✔ Architecture and design documentation  
✔ Best practices: retries, logging, idempotence  

---

## Project Duration

**1 Week**

---

## Author

**Faruk Sedik**  
Aspiring Data Engineer | Backend Developer  
Focused on building scalable, production-grade data systems

---

## License

This project is for educational and demonstration purposes.

