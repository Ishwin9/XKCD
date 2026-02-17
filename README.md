XKCD Comics Data Pipeline
========
This project implements a complete ELT (Extract, Load, Transform) pipeline for XKCD webcomic data:

- Extract & Load: Fetches comics from the XKCD API and loads into BigQuery Bronze layer

- Transform: Uses dbt to create dimensional models and calculate business metrics in the Gold layer

- Orchestration: Automated using Apache Airflow with scheduled runs on Monday, Wednesday, and Friday

- Data Quality: Comprehensive checks implemented using dbt tests

<img width="1009" height="170" alt="Screenshot 2026-02-16 at 6 57 22 PM" src="https://github.com/user-attachments/assets/8996faa2-81bf-4b52-942e-1ee94c6c6073" />

Prerequisites
================
- Docker Desktop (v20.10+)
- Astronomer CLI (astro-cli)
- Google Cloud Platform Account BigQuery API enabled
- Python 3.9+
- Git

Deploy Your Project Locally
===========================

Start Airflow on your local machine by running 'astro dev start'.

Airflow UI at http://localhost:8080/

Default Login:

Username: admin

Password: admin
