# ✈️ Implement Flight Delay Prediction System on K8S

## 📋 Prerequisites

```bash
minikube start --memory=7200 --cpus=4
```

- Docker\
- kubectl\
- Helm

---

## 🚀 Deployment Guide

### 1. Namespace & Global Configurations

```bash
kubectl apply -f k8s/00-namespace.yaml
kubectl apply -f k8s/01-configs.yaml
```

### 2. Infrastructure Deployment

```bash
kubectl apply -f k8s/kafka/02-infra-zookeeper.yaml
kubectl apply -f k8s/kafka/03-infra-kafka.yaml
kubectl apply -f k8s/kafka/04-infra-kafka-ui.yaml
kubectl apply -f k8s/05-infra-minio.yaml
```

### 3. MinIO Setup

```bash
kubectl port-forward -n bigdata service/minio-service 9000:9000 9001:9001
```

- URL: http://localhost:9001\
- Username: admin\
- Password: admin

Create bucket: **warehouse**

### 4. Build Image & Secrets

```bash
eval $(minikube docker-env)
docker build -t flight-prediction:v2 .

kubectl create secret generic my-db-secret   --from-literal=db-url='postgresql://<user>:<password>@<host>/<db>?sslmode=require'   -n bigdata
```

### 5. Deploy Jobs & Streaming

```bash
kubectl apply -f k8s/jobs/06-producer-job.yaml
kubectl apply -f k8s/jobs/07-ingestion-job.yaml
kubectl apply -f k8s/jobs/08-etl-job.yaml
kubectl apply -f k8s/streaming/09-consumer.yaml
```

### 6. Install Apache Airflow

```bash
helm repo add apache-airflow https://airflow.apache.org
helm repo update

helm upgrade --install airflow apache-airflow/airflow   --namespace bigdata   -f airflow/custom-values.yaml
```

---

## 🌐 Accessing Services

Service URL Credentials

---

Kafka UI http://localhost:8000 N/A
MinIO http://localhost:9001 admin / admin
Airflow UI http://localhost:8080 admin / admin

---

## 🕹️ Airflow DAGs

- **ingestion_job** -- hourly raw data ingestion\
- **etl_job** -- hourly data processing

Trigger manually via ▶️ in Airflow UI.

---
