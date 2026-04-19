# Stryker Take-Home Project — Senior Data Engineer

**Version 3.0 | Updated: 2026-01**

***Note, the use of AI is not permmitted***

Welcome to the Stryker Senior Data Engineer take-home exercise. This repository provides a lightweight, opinionated starting point intended to simulate a realistic data engineering workload at Stryker.

This challenge is **not** about completing a large volume of code. Instead, it is designed to surface how you think about data systems, tradeoffs, modeling decisions, scalability, and long-term maintainability.

---

## Problem Overview

A common responsibility for Senior Data Engineers at Stryker is designing and maintaining robust pipelines that ingest third-party data, model it appropriately, store it efficiently, and make it available for downstream consumers such as analytics, reporting, and data science teams.

This exercise represents a simplified snapshot of that responsibility.

To reduce setup overhead, we provide scaffolding for the execution environment (Docker, Airflow, and Postgres). You are encouraged to use, modify, or replace these components if you believe an alternative approach is more appropriate—provided you clearly document your rationale.

---

## What You’ll Build

For this project, you will ingest **current weather data** from the  
[OpenWeatherMap Current Weather API](https://openweathermap.org/current).  
The free API tier is sufficient; **no paid subscription is required**.

At a high level, the system consists of:

### Core Components (Provided)

- **Airflow**  
  Used to orchestrate ingestion and transformation workflows.

- **Postgres**  
  Used as the analytical storage layer for both raw and derived datasets.  
  (You may substitute another database if you prefer.)

- **Docker / Docker Compose**  
  Used to provide a reproducible local execution environment.

---

## Data Flow Expectations

The expected flow is intentionally flexible but should generally include the following stages:

1. **Ingestion (Fetcher DAG)**

   - `fetcher.py` retrieves data from the OpenWeatherMap API.
   - Data should be validated, normalized, and cleaned as appropriate.
   - Raw or lightly processed data should be persisted to Postgres.
   - Design with **schema evolution**, **data quality**, and **idempotency** in mind.

2. **Transformation (Transformer DAG)**

   - `transformer.py` produces one or more derived datasets.
   - Transformations may be implemented in **Python**, **SQL**, or a hybrid approach.
   - Derived tables should support historical analysis and downstream analytics use cases.

3. **Consumption**
   - Assume downstream users will query both raw and transformed datasets.
   - Queries may include time-series analysis, aggregations, or feature extraction.

---

## Design Philosophy

This exercise is intentionally open-ended.

We are far more interested in:

- **How you structure the problem**
- **Why you make certain design choices**
- **How you balance simplicity vs. scalability**
- **How you communicate assumptions and tradeoffs**

Perfection is not expected. Thoughtfulness is.

> **Note:**  
> If you are uncomfortable with Docker, Airflow, or Postgres, you may replace them with alternatives (e.g., local Python execution, SQLite, dbt, etc.). Just document your decision clearly—we will ask about it during follow-up discussions.

---

## Deliverables

Please submit a GitHub pull request containing:

- Source code for:
  - Data ingestion (fetcher)
  - Data transformation logic
- SQL (or equivalent) defining your data model
- Updates to `README.md` that include:
  - Design notes
  - Assumptions
  - Tradeoffs
  - Potential next steps

---

## Evaluation Criteria

We will evaluate this submission as part of a **Senior Data Engineer** interview loop. Review will focus on:

- **Code quality**
  - Readability, structure, naming, and maintainability
- **Data modeling**
  - Schema design, normalization vs. denormalization, relationships
- **Data engineering fundamentals**
  - Idempotency, error handling, observability, performance considerations
- **Technical judgment**
  - Tooling choices and architectural tradeoffs
- **Communication**
  - Clarity of documentation and reasoning

---

## Time Expectations

We recognize that senior candidates have limited availability.

- Expected effort: **~2 hours**
- Spending more or less time is entirely your choice.
- Please note your actual time spent in the section below so we can evaluate your work fairly and in context.

If you choose to go beyond the basics (e.g., testing, schema evolution, incremental loads), that’s welcome—but not required.

---

## Use of Public Resources

We encourage you to attempt this challenge independently.

That said, real-world engineering often involves referencing documentation, blog posts, or existing solutions. If you build upon external work, please:

- Clearly note it in your comments or README
- Provide links to the original sources
- Explain what you adapted or changed

Transparency matters more than originality.

---

## Getting Started

### Environment Setup

1. Fork and clone this repository.
2. Ensure Docker Desktop is installed and running.
3. Initialize and start the environment:

```bash
# Initialize folders and Airflow user
mkdir -p ./logs ./plugins
echo -e "AIRFLOW_UID=$(id -u)" > .env

# Initialize Airflow metadata DB
docker-compose up airflow-init

# Start services
docker-compose up
```

- Airflow UI: [http://localhost:8080](http://localhost:8080)
- Username / Password: `airflow / airflow`

If you encounter issues, refer to the official Airflow Docker docs:
[https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html](https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html)

---

### Implementation Notes

- There are several `TODO` markers in the repository—feel free to go beyond them.
- DAG examples are adapted from the official Airflow tutorial.
- For database interactions, you may reference:

  - Airflow Postgres Operator documentation
  - Native Python DB libraries
  - Any abstraction you deem appropriate

For simplicity, you may store all datasets in the Airflow-managed Postgres instance.

---

### Apple Silicon Note

If you are using Apple hardware (M1/M2), Docker image compatibility may require additional configuration.
Reference:
[https://javascript.plainenglish.io/which-docker-images-can-you-use-on-the-mac-m1-daba6bbc2dc5](https://javascript.plainenglish.io/which-docker-images-can-you-use-on-the-mac-m1-daba6bbc2dc5)

---

## Your Notes (README.md)

Use the sections below to document your work.

### Time Spent

- Setting up Docker-compose and fixing bug with airflow-triggerer: 1h
- Implementing fetcher DAG and getting familiar with OpenWeather API, and storage of JSON objects in Postgres : 1h
- Defining data architecture and modeling decisions: 30m
- Defining database to use (relational database vs OLAP/columnar database): 30m
- Transformation logic (SQL) and defining derived tables (AI was used to accelerate development time): 2h
- Testing and debugging: 1h

Total: ~6 hours

---

### Assumptions

- The API returns data for a single city, so the silver grain is defined as one row per source observation timestamp.
- Raw ingestion is append-only, even if duplicate observations are retrieved.
- The dataset volume is small, so no physical partitioning was implemented.
---

### Tradeoffs & Design Decisions

#### Architecture:
Implemented a layered Medallion architecture (raw → silver → gold) due to the following reasons:
- Raw/Bronze: Ingestion and storage of raw data "as it is" to handle schema evolution and data lineage/auditability.
- Silver: Intermediate transformation (Data cleaning, de-duplication, typed columns).
- Gold: Final curated datasets for analytics and downstream consumption.

#### Tradeoffs:
- Database/storage: Chose a simple Postgres database for ease of setup and demonstration, 
but this may not scale well for larger datasets or more complex transformations. 
A columnar OLAP database could be more performant for analytical queries.
- Computing Transformations: Implemented transformations in SQL for simplicity due to the low volume of data, but with larger datasets i would implement Spark based tools for distributed computing.
- Orchestartion: Pipelines are implemented as separate DAGs (fetcher, silver, gold), did not implement a master orchestration DAG due to time constraints. 

---

### Next Steps / Improvements
- True incremental processing: Currently the pipeline scans the full raw table and uses deduplication + NOT EXISTS
- Schema evolution handling: Add logic to handle changes in the API response structure, such as new fields or changes in data types.
- Data quality/ testing: Implement checks to key metrics. Evaluate implementation of unit tests for transformation logic.
- Observability / monitoring: Add exception handling to facilitate monitoring and debugging.
- Performance optimizations: Evaluate using partition strategies and an OLAP/columnar database solution. Also evaluate using Spark-based tools for distributed computing if dataset volume increases.
- Orchestration: Implement a master DAG to orchestrate the entire workflow and manage dependencies between fetcher, silver, and gold DAGs.
- Use of serverless services in cloud environments: Evaluate using serverless compute and managed  services to reduce operational overhead and reduce costs.
---

### Instructions to the Evaluator

This projects uses environment variables stored in a `.env` file.
To run the project:
1. Create a `.env` file in the root directory by copying the provided example:
```bash
cp .env.example .env
```
2. Fill the required environment variables in the `.env` file.
3. Start the project using Docker Compose:
```bash 
docker-compose up
``` 
---
