# ✈️ Flight Delay Prediction System on Kubernetes

This repository contains the deployment manifest and instructions to set
up a Big Data pipeline for Flight Delay Prediction on Kubernetes
(Minikube). The architecture includes Kafka, Zookeeper, MinIO, Spark
(ETL), PostgreSQL, and Apache Airflow.

## 📋 Prerequisites

Before proceeding, ensure you have the following installed and running: \* **Minikube** (Started with adequate resources, e.g.,
`minikube start --memory=7200 --cpus=4`) \* **Docker** \* **Kubectl** \*
**Helm**

---

## 🚀 Deployment Guide

### 1. Environment Setup

Create the `bigdata` namespace and apply global configurations.

```bash
kubectl apply -f k8s/00-namespace.yaml
kubectl apply -f k8s/01-configs.yaml
```

### 2. Infrastructure Deployment

Deploy the core infrastructure components: Zookeeper, Kafka, Kafka UI,
and MinIO.

```bash
kubectl apply -f k8s/kafka/02-infra-zookeeper.yaml
kubectl apply -f k8s/kafka/03-infra-kafka.yaml
kubectl apply -f k8s/kafka/04-infra-kafka-ui.yaml
kubectl apply -f k8s/05-infra-minio.yaml
```

### 3. MinIO Configuration (Manual Step)

You must create the storage bucket before running the jobs.

Expose MinIO:

```bash
kubectl port-forward -n bigdata service/minio-service 9000:9000 9001:9001
```

Access the Console: - URL: http://localhost:9001\

- Username: `admin`\
- Password: `admin`

Create a new bucket named **warehouse**.

### 4. Build Image & Configure Secrets

```bash
# Point shell to Minikube Docker daemon
eval $(minikube docker-env)

# Build image
docker build -t flight-prediction:v2 .

# Create Database Secret (NeonDB/Postgres)
kubectl create secret generic my-db-secret   --from-literal=db-url='postgresql://neondb_owner:npg_LK04qASEgCyc@ep-still-shadow-ahdpmoze-pooler.c-3.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require'   -n bigdata
```

### 5. Deploy Jobs & Streaming Services

```bash
# Batch Jobs
kubectl apply -f k8s/jobs/06-producer-job.yaml
kubectl apply -f k8s/jobs/07-ingestion-job.yaml
kubectl apply -f k8s/jobs/08-etl-job.yaml

# Streaming Consumer
kubectl apply -f k8s/streaming/09-consumer.yaml
```

### 6. Install Apache Airflow

```bash
helm repo add apache-airflow https://airflow.apache.org
helm repo update

helm upgrade --install airflow apache-airflow/airflow   --namespace bigdata   -f airflow/custom-values.yaml   --set webserver.enabled=true   --set webserver.replicas=1   --wait=false   --debug
```

## 🌐 Accessing Services

---

Service Port-Forward Command URL Credentials

---

Kafka UI kubectl port-forward -n bigdata localhost:8000 N/A
service/kafka-ui-service  
 8000:8000

MinIO kubectl port-forward -n bigdata localhost:9001 admin / admin
service/minio-service 9000:9000  
 9001:9001

Airflow UI kubectl port-forward localhost:8080 admin / admin
svc/airflow-api-server 8080:8080  
 -n bigdata

---

## 🕹️ Operations

Once Airflow is running, access the Airflow UI at
**http://localhost:8080**.

Available DAGs: - **ingestion_job**: Hourly raw data ingestion -
**etl_job**: Hourly data cleaning and processing

You can manually trigger these DAGs using the ▶️ button to test the
pipeline immediately.
