# NASA APOD Airflow ETL Pipeline

## Project Overview

This project extracts data from NASA Astronomy Picture of the Day (APOD) API,
transforms the response, performs data quality validation, and loads the data
into PostgreSQL using Apache Airflow.

## Architecture

```text
NASA APOD API
      |
      v
Extract Task
      |
      v
Transform Task
      |
      v
Data Quality Check
      |
      v
PostgreSQL
```

## Technologies

- Python
- Apache Airflow
- PostgreSQL
- Docker
- Astro CLI

## Features

- API ingestion from NASA APOD API
- Data transformation
- Data quality validation
- Logging and monitoring
- Duplicate prevention using PostgreSQL ON CONFLICT
- Scheduled execution with Apache Airflow

## Project Structure

```text
etl_pipeline/
├── dags/
│   └── etl.py
├── tests/
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── README.md
```

## Future Improvements

- Add retry and alert mechanisms
- Store images in object storage
- Build a data warehouse layer
- Integrate with dbt
- Add CI/CD using GitHub Actions
