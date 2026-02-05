
# Airflow Flight Price Analysis (Bangladesh)

## Project Overview

This project implements an **end-to-end data engineering pipeline** using **Apache Airflow** to process and analyze flight price data for Bangladesh.

The pipeline ingests raw CSV data, validates and transforms it, computes analytical KPIs, and stores the results in a PostgreSQL analytics database. The entire system is containerized using Docker and follows production-style data engineering best practices.

Dataset used: **Flight Price Dataset of Bangladesh (Kaggle)**  
Total records processed: **57,000**

---

## Objectives

- Build an Airflow-orchestrated data pipeline
- Load raw CSV data into a MySQL staging database
- Perform data validation and transformation
- Compute required KPIs for flight price analysis
- Store analytical results in PostgreSQL
- Ensure reproducibility using Docker

---

## Technology Stack

- **Apache Airflow 2.8.1** – Workflow orchestration
- **MySQL 8.0** – Staging database
- **PostgreSQL 15** – Analytics database
- **Python (pandas, SQLAlchemy)** – Data processing
- **Docker & Docker Compose** – Containerized deployment

---

## Repository Structure

```

airflow-flight-price-analysis/
│
├── dags/
│   ├── flight_csv_to_mysql.py
│   └── flight_mysql_to_postgres.py
│
├── sql/
│   ├── mysql_setup.sql
    ├── postgres_analytics.sql
│   └── postgres_setup.sql
│
├── docker-compose.yaml
├── project_report.md
├── system_architecture.md
├── test_cases.md
└── README.md

```

---

## Pipeline Architecture

```

CSV File
↓
Airflow DAG (Ingestion)
↓
MySQL (Staging Layer)
↓
Airflow DAG (Validation & Transformation)
↓
PostgreSQL (Analytics Layer + KPIs)

````

---

## Airflow DAGs

### 1. `flight_csv_to_mysql`
**Purpose:**  
- Load raw CSV data into MySQL staging

**Key Actions:**
- Read CSV file
- Normalize column names
- Create staging table if not exists
- Idempotent load

**Output:**
- `flight_prices_raw` table with **57,000 records**

---

### 2. `flight_mysql_to_postgres`
**Purpose:**  
- Validate data
- Apply transformations
- Compute KPIs
- Load analytics data into PostgreSQL

**Validation Rules:**
- Required columns exist
- No negative fare values
- Correct numeric and categorical data types

**Transformations:**
- Recalculate total fare
- Normalize date and season fields
- Classify Peak vs Non-Peak seasons

---

## KPIs Generated (Real Results)

### Average Fare by Airline (Top 5)
| Airline | Avg Fare (BDT) |
|------|---------------|
| Turkish Airlines | 74,738.63 |
| AirAsia | 73,830.16 |
| Cathay Pacific | 72,594.04 |
| Malaysian Airlines | 72,247.17 |
| Thai Airways | 72,062.79 |

---

### Booking Count by Airline (Top 5)
| Airline | Booking Count |
|------|---------------|
| US-Bangla Airlines | 4,496 |
| Lufthansa | 2,368 |
| Vistara | 2,368 |
| FlyDubai | 2,346 |
| Biman Bangladesh Airlines | 2,344 |

---

### Most Popular Routes
| Route | Bookings |
|------|---------|
| RJH → SIN | 417 |
| DAC → DXB | 413 |
| BZL → YYZ | 410 |
| CGP → BKK | 408 |
| CXB → DEL | 408 |

---

### Seasonal Fare Comparison (Peak vs Non-Peak)
Peak seasons defined as **Eid, Hajj, Winter Holidays**

| Season Type | Avg Fare (BDT) |
|------------|---------------|
| Peak | 79,859.41 |
| Non-Peak | 67,935.11 |

---

## How to Run the Project

### Prerequisites
- Docker
- Docker Compose

### Steps

```bash
docker compose up -d
````

Access Airflow UI:

* URL: [http://localhost:8080](http://localhost:8080)
* Username: `admin`
* Password: `admin`

Trigger the DAGs in order:

1. `flight_csv_to_mysql`
2. `flight_mysql_to_postgres`

---

## Verification Queries

```sql
-- MySQL
SELECT COUNT(*) FROM flight_prices_raw;

-- PostgreSQL
SELECT COUNT(*) FROM flight_prices_clean;
SELECT * FROM kpi_peak_vs_non_peak_fares;
```

---

## Data Quality & Reliability

* Idempotent DAG executions
* Schema validation
* Explicit season classification
* No data loss between staging and analytics layers

---

## Challenges Addressed

* Docker port conflicts
* Airflow scheduler initialization
* Database connection misconfiguration
* Seasonal KPI definition correction

All issues were resolved and documented in `project_report.md`.

---

## Conclusion

This project demonstrates a complete, production-style data engineering pipeline using Apache Airflow. All KPIs were computed using real data and verified via SQL queries, fulfilling all requirements of **Module 6 Lab 1 : Airflow Project – Flight Price Analysis**.

---
