# SaaS Product Analytics Platform

## Project Overview

The **SaaS Product Analytics Platform** is an end-to-end AWS Data Engineering project that simulates user activity from a SaaS application. Product usage events are generated using AWS Lambda, stored in an Amazon S3 data lake, transformed into Parquet format using AWS Glue ETL, cataloged through the AWS Glue Data Catalog, orchestrated using Amazon MWAA (Apache Airflow), and analyzed using Amazon Athena.

The project demonstrates a production-style batch data pipeline following a modern data lake architecture.

---

## Architecture Diagram

```text
architecture/
└── architecture.png
```

---

## Tech Stack

* Python
* PySpark
* SQL
* AWS Lambda
* Amazon S3
* AWS Glue ETL
* AWS Glue Crawler
* AWS Glue Data Catalog
* Amazon Athena
* Amazon MWAA (Apache Airflow)
* IAM
* CloudWatch

---

## Project Architecture

```text
Airflow (MWAA)
        │
        ▼
AWS Lambda
(Random SaaS Event Generator)
        │
        ▼
Amazon S3 (Raw Layer)
        │
        ▼
AWS Glue ETL (PySpark)
        │
        ▼
Amazon S3 (Processed Layer - Parquet)
        │
        ▼
AWS Glue Crawler
        │
        ▼
Glue Data Catalog
        │
        ▼
Amazon Athena
```

---

## Local Project Structure

```text
saas-product-analytics-platform/

├── airflow/
│   └── dags/
│       └── saas_product_pipeline_dag.py
│
├── lambda/
│   └── lambda_function.py
│
├── glue/
│   └── saas_products_etl.py
│
├── sql/
│   ├── analytics_queries.sql
│   └── validation_queries.sql
│
├── architecture/
│   └── architecture.png
│
├── screenshots/
│
├── README.md
├── requirements.txt
├── pyproject.toml
├── uv.lock
└── .python-version
```

---

## AWS Resources

| Service          | Resource Name                 |
| ---------------- | ----------------------------- |
| S3 Bucket        | `de-saas-platform-san`        |
| Lambda Function  | `saas-event-generator`        |
| Glue Job         | `saas-events-etl`             |
| Glue Crawler     | `saas-events-crawler`         |
| Glue Database    | `saas_events_db`              |
| Athena Table     | `saas_events`                 |
| MWAA Environment | `saas-product-analytics-mwaa` |
| Lambda IAM Role  | `saas-lambda-role`            |
| Glue IAM Role    | `saas-glue-role`              |
| MWAA IAM Role    | `saas-mwaa-role`              |

---

## S3 Bucket Structure

```text
de-saas-platform-san/

├── raw/
│   └── year=YYYY/
│       └── month=MM/
│           └── day=DD/
│               └── events_YYYYMMDD_HHMMSS.json
│
├── processed/
│   └── year=YYYY/
│       └── month=MM/
│           └── day=DD/
│               └── part-00000.parquet
│
├── athena-results/
├── dags/
└── logs/
```

---

## Raw JSON Schema

Each Lambda execution generates one JSON file.

```json
{
  "events": [
    {
      "event_id": "EVT100001",
      "user_id": 105,
      "workspace_id": "WS15",
      "organization_id": "ORG10",
      "event_type": "login",
      "device": "Windows",
      "browser": "Chrome",
      "country": "India",
      "subscription_plan": "Pro",
      "timestamp": "2026-07-10T09:15:12Z"
    }
  ]
}
```

---

## Data Flow

```text
Lambda
    │
    ▼
Amazon S3 (Raw)
    │
    ▼
AWS Glue ETL
    │
    ▼
Amazon S3 (Processed)
    │
    ▼
Glue Crawler
    │
    ▼
Glue Data Catalog
    │
    ▼
Amazon Athena
```

---

## Glue ETL Responsibilities

The Glue ETL job performs the following operations:

* Reads JSON files from the Raw layer.
* Explodes the `events` array into individual records.
* Removes records with NULL values in mandatory fields.
* Removes duplicate records using `event_id`.
* Converts the event timestamp to Spark Timestamp.
* Adds an ingestion timestamp.
* Writes partitioned Parquet files to the Processed layer.

---

## Airflow Workflow

```text
Start
   │
   ▼
Generate SaaS Events (Lambda)
   │
   ▼
Run Glue ETL
   │
   ▼
Run Glue Crawler
   │
   ▼
End
```

---

## Validation Rules

### Mandatory Fields

The following fields cannot be NULL:

* `event_id`
* `user_id`
* `organization_id`
* `event_type`
* `country`
* `subscription_plan`
* `timestamp`

### Optional Fields

The following fields may contain NULL values:

* `device`
* `browser`

### Duplicate Handling

Duplicates are removed only using:

* `event_id`

Duplicates are **not** removed using:

* `user_id`
* `organization_id`
* `workspace_id`

---

## Athena Analytics Queries

The project includes SQL queries for analytics such as:

* Event count by event type
* Event count by subscription plan
* Event count by country
* Daily event volume
* Top 10 active users

Validation queries include:

* Total record count
* NULL mandatory field validation
* Duplicate `event_id` validation

---

## How to Run the Project

1. Trigger the Airflow DAG.
2. Airflow invokes the Lambda function.
3. Lambda generates SaaS event data and stores it in Amazon S3 (Raw layer).
4. Airflow starts the AWS Glue ETL job.
5. Glue transforms JSON into partitioned Parquet files.
6. Airflow runs the Glue Crawler.
7. The Glue Data Catalog is updated.
8. Execute SQL queries in Amazon Athena for analytics.

---

## Project Outcome

Successfully implemented an end-to-end AWS Data Engineering pipeline that:

* Simulates SaaS product activity.
* Stores immutable raw event data in Amazon S3.
* Cleans and transforms event data into Parquet format.
* Builds a searchable metadata catalog using AWS Glue.
* Automates the complete workflow using Amazon MWAA.
* Enables analytical reporting through Amazon Athena.

---

## Screenshots

Store project screenshots inside:

```text
screenshots/
```

Suggested screenshots:

* S3 Bucket Structure
* Lambda Function
* Lambda Successful Execution
* Glue ETL Job Success
* Glue Crawler Success
* Glue Data Catalog Table
* Athena Query Results
* MWAA Environment
* Airflow DAG Graph View
* Successful DAG Run
