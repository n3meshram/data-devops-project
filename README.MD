# Customer ETL Pipeline using PySpark, Docker & Kubernetes

A production-style **Customer ETL Pipeline** built using **PySpark**, **Docker**, and **Kubernetes**. The project demonstrates how raw customer data is processed through multiple data engineering layers (Bronze, Hub, SCD Type 2, and Mart) using Spark running on Kubernetes.

---

# Project Overview

This project simulates a real-world Data Engineering pipeline where customer data is ingested, validated, transformed, and prepared for business reporting.

The pipeline is containerized using Docker and deployed on Kubernetes using the Spark Operator.

---

# Architecture

```
                  Customer CSV
                       │
                       ▼
                Bronze Layer
          (Raw Data Validation)
                       │
                       ▼
                  Hub Layer
        (Unique Business Keys)
                       │
                       ▼
              SCD Type 2 Layer
         (Historical Tracking)
                       │
                       ▼
                 Mart Layer
        (Business Reporting Data)
                       │
                       ▼
          Power BI / Tableau / BI
```

---

# Technology Stack

| Technology | Purpose |
|------------|---------|
| Python | Programming Language |
| PySpark | Distributed Data Processing |
| Apache Spark 4.0 | ETL Engine |
| Docker | Containerization |
| Kubernetes | Container Orchestration |
| Spark Operator | Running Spark on Kubernetes |
| Git & GitHub | Version Control |
| Parquet | Storage Format |
| CSV | Source Data |

---

# Project Features

- Customer ETL Pipeline
- Bronze Layer
- Hub Layer
- SCD Type 2
- Mart Layer
- Dockerized Spark Application
- Kubernetes Deployment
- Spark Operator
- Persistent Volume Claim (PVC)
- RBAC Configuration
- Batch Logging
- Modular Project Structure

---

# Project Structure

```
customer-etl/
│
├── common/
│   ├── logger.py
│   └── utils.py
│
├── config/
│   └── customer_config.py
│
├── data/
│   ├── bronze/
│   ├── hub/
│   ├── mart/
│   ├── reject/
│   └── source/
│
├── kubernetes/
│   ├── spark-application.yaml
│   ├── spark-role.yaml
│   ├── spark-rbac.yaml
│   └── pvc.yaml
│
├── logs/
│   └── job_log.csv
│
├── scripts/
│   ├── bronze/
│   ├── hub/
│   └── mart/
│
├── Dockerfile
├── requirements.txt
├── run_pipeline.py
└── README.md
```

---

# Pipeline Flow

```
Customer CSV

↓

Bronze Layer

↓

Hub Layer

↓

SCD Type 2

↓

Mart Layer

↓

Business Reports
```

---

# Running the Project

## Clone Repository

```bash
git clone <YOUR_GITHUB_REPOSITORY_URL>

cd customer-etl
```

---

## Build Docker Image

```bash
docker build -t customer-etl:v1.0 .
```

---

## Verify Docker Image

```bash
docker images
```

---

## Deploy to Kubernetes

```bash
kubectl apply -f kubernetes/pvc.yaml

kubectl apply -f kubernetes/spark-role.yaml

kubectl apply -f kubernetes/spark-rbac.yaml

kubectl apply -f kubernetes/spark-application.yaml
```

---

## Monitor Spark Job

```bash
kubectl get sparkapplication -n spark-jobs

kubectl get pods -n spark-jobs

kubectl logs customer-etl-driver -n spark-jobs
```

---

# Output

The pipeline generates the following output folders.

```
data/

├── bronze/

├── hub/

├── mart/

└── reject/
```

---

# Logging

Each pipeline stage records execution details including:

- Job Name
- Batch ID
- Input Count
- Accepted Count
- Rejected Count
- Status
- Start Time
- End Time

---

# Kubernetes Components

The project uses:

- SparkApplication
- Driver Pod
- Executor Pods
- Persistent Volume Claim (PVC)
- ServiceAccount
- Role
- RoleBinding

---

# Troubleshooting

Common commands:

```bash
kubectl get sparkapplication -n spark-jobs

kubectl describe sparkapplication customer-etl -n spark-jobs

kubectl get pods -n spark-jobs

kubectl logs customer-etl-driver -n spark-jobs

kubectl get svc -n spark-jobs
```

---

# Future Improvements

- CI/CD using GitHub Actions
- Unit Testing with Pytest
- Airflow Orchestration
- Amazon S3 Integration
- Delta Lake
- Prometheus Monitoring
- Grafana Dashboards
- Spark History Server

---

# Author

**Nitin Meshram**

Data DevOps Engineer

GitHub: https://github.com/n3meshram

---

# License

This project is licensed under the MIT License.