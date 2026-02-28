# Medallion ETL Pipeline — End-to-End Data Engineering Project
Imagine you need a data platform that ingests events, cleans them, transforms them into analytical tables, archives the old data, and keeps everything observable.
But you don’t want random scripts, cron jobs, or manual cleanup.
You want a real pipeline — structured, automated, and production-like.


This project demonstrates exactly that.
It implements a full Medallion Architecture (Bronze → Silver → Gold) with Airflow, Kafka, MinIO, FastAPI, and PostgreSQL.
All parts work together to simulate a modern data engineering platform, with clear data lineage, lifecycle management, and observability.

### It is designed to demonstrate a complete modern data engineering stack, including:

* Near-real-time ingestion (Kafka → Bronze)
* Deterministic batch transformation (Silver)
* Normalization into analytical tables (Gold)
* Outbox tracking for observability & lineage
* Automatic archival DAG (moving raw files)
* Cleanup DAG (lifecycle management)
* HTMX-driven monitoring UI
* Fully structured JSON logging system
  
&nbsp;

## 📌 Architecture Overview

![screenshot](readme_assets/data_flow_arch.png)


## What This Project Is and Why It Exists

Many ETL examples are oversimplified: one script, one table, no lifecycle, no monitoring.
This project intentionally models a real production-grade system:

Events flow into Kafka.
* Airflow orchestrates multi-stage Medallion processing.
* MinIO stores Bronze, Silver, Gold, and Archive layers.
* FastAPI provides an operational UI and structured logging.
* PostgreSQL stores normalized analytical tables + metadata (outbox pattern).
* Separate DAGs handle ingestion, transformation, archiving, and cleanup.

It’s built to be touched, extended, and understood — not to hide complexity.

&nbsp;
# 🔶 Medallion Layers Overview
### 🔵 Bronze — Raw Files

Events arrive from Kafka and are saved into partitioned Parquet files in events-bronze.
Nothing is mutated. Data is immutable, append-only.

### 🟠 Silver — Cleaned & Standardized

Airflow transforms Bronze files:

* cleans fields
* standardizes schemas
* normalizes nested structures
* outputs Silver parquet files

### 🟡 Gold — Analytical Tables

Two analytical tables are produced:

* gold_page_views
* gold_product_events

Each transformation produces an outbox entry with:

* file name
* bucket
* row count
* processing status

This metadata enables deterministic lifecycle management.


## 📊 ETL Workflow Diagrams

---

### Full ETL DAG (Bronze → Silver → Gold)
Responsible for the entire transformation chain.

1. Fetch all new Bronze parquet files
2. Clean & transform to Silver format
3. Normalize into 2 Gold analytical tables:
      • gold_page_views
      • gold_product_events
4. Write metadata into outbox table:
      - file_name
      - bucket
      - row_count
      - status ("archived" after writing gold)
5. Finish

![screenshot](readme_assets/dag1.png)

---

### Archive DAG
Moves already-processed files to archive storage.

1. Read outbox where status="archived"
2. Find matching files in Bronze/Silver/Gold buckets
3. Move files to *archive* buckets
4. Update outbox status → "done"
5. Finish

![screenshot](readme_assets/dag2.png)

---

### Clean DAGs
Deletes files permanently after successful archival.

1. Read outbox where status="done"
2. Find matching files in archive buckets
3. Remove them permanently using MinIO API
4. Finish

![screenshot](readme_assets/dag3.png)

&nbsp;
---
### 🛡️ Run Tests
```bash
docker compose -f docker-compose.tests.yml up --build --exit-code-from api_tests
```
&nbsp;

## 🚀 Running Locally

```bash
docker compose \
  -f docker-compose.infra.yml \
  -f docker-compose.app.yml \
  -f docker-compose.airflow.yml \
  up -d --build
```
&nbsp;


## 🖥️ UI Preview

This project also includes an operational interface that makes it easy to explore the pipeline, inspect metrics, and interact with the API.


### 📘 Interactive API Documentation (Swagger UI) (/docs)

FastAPI automatically generates interactive API documentation for all endpoints.
This allows you to inspect schemas, execute requests, and understand the available API surface.


![screenshot](readme_assets/dataflow_swagger.png)

&nbsp;

### 📊 Live Dashboard with HTMX (/)

The dashboard provides live Medallion pipeline metrics:
Bronze / Silver / Gold row counts
number of Parquet files per layer
analytical counters (page views, product events)
data refresh every 10 seconds via HTMX
All metrics update automatically without reloading the page.

![screenshot](readme_assets/dataflow_dashboard.png)
&nbsp;

# 🧩 Key Design Principles

This project follows a set of principles commonly used in production-grade data platforms:

## 1. Immutable Raw Data (Bronze stays untouched)

* Raw events are never mutated or overwritten.
* All transformations produce new files.
* This ensures auditability, reproducibility, and safe re-processing.

## 2. Deterministic, Idempotent Transformations

Every step — Bronze → Silver → Gold — is designed so that:

* running the same transformation twice gives the same result
* ingestion failure does not corrupt data
* retries are safe

This mirrors real enterprise ETL reliability patterns.

## 3. Clear Layer Separation (Medallion Architecture)

Each layer has a single responsibility:

* Bronze → raw
* Silver → cleaned, normalized
* Gold → analytics-ready

This keeps the pipeline clean, predictable, and easy to debug.

## 4. Explicit Metadata & Lineage (Outbox Pattern)

Every processed file writes a metadata entry:
* file name
* row count
* bucket
* status

This allows Airflow to manage lifecycle DAGs deterministically and provides full transparency into data movement.

## 5. Storage-Based Workflow (Object Storage First)

All intermediate results live in MinIO (S3-like storage).
This avoids tight coupling with the database and improves scalability.
Airflow moves files between buckets instead of relying solely on SQL.

## 6. Orchestration via Airflow, Not Scripts

All workflows — ETL, archiving, cleanup — are explicitly defined in DAGs:
* retry policy
* dependencies
* scheduling
* monitoring

This makes the project closer to real-world production systems.

## 7. Strong Observability (Grafana + Prometheus + Loki)

The system emits structured JSON logs, Prometheus metrics, and Kafka metrics.
Everything can be traced:
* ingestion
* transformation
* errors
* file movements
* DAG runs

This level of observability is expected in modern data engineering.

## 8. Modular, Service-Oriented Architecture

Each responsibility is isolated:

* Airflow (orchestration)
* FastAPI (operational UI + metadata)
* MinIO (storage)
* Kafka (events)
* PostgreSQL (analytics storage)

This separation makes the project easy to extend and maintain.

## 9. Dockerized for Full Reproducibility

Every component — Airflow, Kafka, Connect, API, MinIO, DB — runs inside Docker.
This guarantees a consistent environment across machines.
