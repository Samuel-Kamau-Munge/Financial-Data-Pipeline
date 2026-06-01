# financial-data-pipeline

Minimal scaffold for a Python-based financial data pipeline using Kafka, PostgreSQL, Airflow, dbt and Kubernetes.

Overview
- Ingest financial files (Excel/CSV) via a Kafka producer.
- Consume Kafka messages and persist raw data into PostgreSQL.
- Orchestrate steps with Airflow DAGs.
- Transform data using dbt (Postgres adapter).
- Deploy services using Docker Compose locally and Kubernetes for production.

Quickstart (local Windows dev)
1. Install Docker Desktop and enable WSL2.
2. Open a PowerShell/Bash terminal in the project root.
3. Start local services:
   - docker-compose up -d
4. Initialize DB schema:
   - python scripts/init_db.py
5. Run Airflow webserver/scheduler via docker-compose (see `docker-compose.yml`).
6. Run producer locally to publish sample messages:
   - python services/producer/producer.py "C:\\Finance Data\\Accounts-Receivable.xlsx"
7. Run consumer locally to write to Postgres (or run via Docker):
   - python services/consumer/consumer.py

Publish to GitHub (create repo under user Samuel-Kamau-Munge)
1. Create a new repository on GitHub named `financial-data-pipeline` under the account Samuel-Kamau-Munge.
2. In project root:
   git init
   git add .
   git commit -m "Initial scaffold"
   git remote add origin https://github.com/Samuel-Kamau-Munge/financial-data-pipeline.git
   git push -u origin main

Files created
- docker-compose.yml, services/producer, services/consumer, airflow/dags, dbt/, k8s/, sql/, scripts/, .github/workflows/ci.yml, Makefile, LICENSE

Next steps
- Fill in credentials in `.env` or Docker Compose.
- Test end-to-end locally, then containerize and deploy to Kubernetes.
