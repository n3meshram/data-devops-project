# 🚀 Customer ETL Pipeline using PySpark, Docker & Kubernetes

A production-style ETL pipeline built with **PySpark**, **Docker**, **Kubernetes**, and the **Spark Operator**.

This project demonstrates how to execute a distributed Spark ETL pipeline on Kubernetes using Spark Operator. The pipeline processes customer data through multiple layers following modern Data Engineering practices.

---

# 📌 Project Overview

This project reads customer data from a CSV file, performs multiple ETL transformations, and stores the output in Parquet format.

Current Pipeline:

```
Customer CSV
      │
      ▼
 Bronze Layer
      │
      ▼
 Hub Layer
      │
      ▼
 SCD Type 2
      │
      ▼
 Mart Layer
```

The project is containerized using Docker and executed inside Kubernetes using Spark Operator.

---

# 🚀 Technologies Used

- Python 3
- PySpark 4.0
- Apache Spark
- Docker
- Kubernetes
- Spark Operator
- YAML
- Git
- GitHub

---

# 📂 Project Structure

```
project1/

│
├── common/
│     ├── logger.py
│     └── utils.py
│
├── config/
│     └── customer_config.py
│
├── data/
│     ├── input/
│     ├── bronze/
│     ├── hub/
│     ├── mart/
│     └── logs/
│
├── kubernetes/
│     ├── namespace.yaml
│     ├── spark-application.yaml
│     ├── spark-role.yaml
│     ├── spark-rbac.yaml
│     ├── spark-pi.yaml
│     └── pvc.yaml
│
├── scripts/
│     ├── bronze/
│     ├── hub/
│     └── mart/
│
├── tests/
│
├── Dockerfile
├── requirements.txt
├── run_pipeline.py
├── Jenkinsfile
└── README.md
```

---

# 🛠 Prerequisites

Install the following before running the project.

- Docker Desktop
- Kubernetes (Docker Desktop)
- kubectl
- Helm
- Python 3.10+
- Git

Verify installation

```
docker --version

kubectl version

helm version

python --version
```

---

# Clone Repository

```
git clone https://github.com/n3meshram/data-devops-project.git

cd project1
```

---

# Build Docker Image

```
docker build -t customer-etl .
```

Verify image

```
docker images
```

---

# Install Spark Operator

```
helm repo add spark-operator https://kubeflow.github.io/spark-operator

helm repo update
```

Install

```
helm install spark-operator spark-operator/spark-operator \
--namespace spark-operator \
--create-namespace
```

Verify

```
kubectl get pods -n spark-operator
```

---

# Create Kubernetes Resources

```
kubectl apply -f kubernetes/namespace.yaml

kubectl apply -f kubernetes/spark-rbac.yaml

kubectl apply -f kubernetes/spark-role.yaml
```

Verify

```
kubectl get namespace

kubectl get role -n spark-jobs

kubectl get serviceaccount -n spark-jobs
```

---

# Run Spark Application

Deploy

```
kubectl apply -f kubernetes/spark-application.yaml
```

Check status

```
kubectl get sparkapplication -n spark-jobs
```

Watch

```
kubectl get sparkapplication -w -n spark-jobs
```

---

# View Driver Logs

```
kubectl logs customer-etl-driver -n spark-jobs
```

---

# View Executor Logs

```
kubectl get pods -n spark-jobs

kubectl logs <executor-pod-name> -n spark-jobs
```

---

# Verify Output

Example

```
data/

bronze/

hub/

mart/
```

Verify generated Parquet files.

---

# Pipeline Flow

```
Customer CSV

        │

        ▼

Customer Bronze

        │

        ▼

Customer Hub

        │

        ▼

Customer SCD2

        │

        ▼

Customer Mart
```

---

# Features

✔ Spark on Kubernetes

✔ Dockerized ETL

✔ Spark Operator

✔ RBAC Configuration

✔ Bronze Layer

✔ Hub Layer

✔ SCD Type 2

✔ Mart Layer

✔ Logging

✔ Modular Code Structure

✔ Production Folder Structure

---

# Useful Commands

Delete application

```
kubectl delete sparkapplication customer-etl -n spark-jobs
```

Delete pods

```
kubectl delete pods --all -n spark-jobs
```

Watch pods

```
kubectl get pods -w -n spark-jobs
```

Driver logs

```
kubectl logs customer-etl-driver -n spark-jobs
```

---

# Troubleshooting

### Driver Pod Failed

```
kubectl logs customer-etl-driver -n spark-jobs
```

---

### Executor Failed

```
kubectl get pods -n spark-jobs

kubectl logs <executor-name> -n spark-jobs
```

---

### SparkApplication Status

```
kubectl describe sparkapplication customer-etl -n spark-jobs
```

---

### Spark Operator Logs

```
kubectl logs deployment/spark-operator-controller -n spark-operator
```

---

# Future Improvements

- Helm Chart
- CI/CD using GitHub Actions
- Jenkins Pipeline
- Delta Lake
- AWS S3 Storage
- Airflow Scheduling
- Monitoring using Prometheus & Grafana
- Unit Testing
- Integration Testing

---

# Author

**Nitin Meshram**

Data DevOps Engineer

GitHub: https://github.com/n3meshram

---

# License

This project is created for learning and demonstration purposes.