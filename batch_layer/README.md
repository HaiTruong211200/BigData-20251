## Prerequisites

Before running the project, ensure you have the following installed:

- Docker Desktop (Running Linux containers)
- Python 3.8+
- Java (JDK 8 or 11) (Required for Spark)
- **(Windows Users)**: winutils.exe setup:
  - Create directory: `C:\hadoop\bin`
  - Download `winutils.exe` and place it inside the bin folder.
  - Note: The project code is configured to look for this path automatically.

## How to Run

### Step 1: Setup Infrastructure

Start Kafka and MinIO containers using Docker:

```bash
docker-compose up -d
```

Wait 1-2 minutes for services to initialize.

### Step 2: Configure MinIO (One-time Setup)

Open your browser and go to `http://localhost:9001`.

- Login with: `admin / password123`
- Go to Buckets > Create Bucket
- Name the bucket: `warehouse`
- Click Create

### Step 3: Install Dependencies

```bash
pip install -r requirements.txt
```

### Step 4: Start Data Generator (Producer)

Open a terminal and run the producer to start sending flight data to Kafka:

```bash
python data_generator/producer.py
```

You should see logs indicating messages are being sent: `-> Sent: AA | ...`

### Step 5: Run Ingestion Layer

This job reads from Kafka and saves Parquet files to MinIO.

```bash
python -m  src.jobs.ingestion_job
```

### Step 6: Run Batch ETL Layer

Once you have enough data in MinIO (check the MinIO browser), run the ETL job to process data and upload KPIs to the Database.

```bash
python -m  src.jobs.etl_job
```

## Verification

### Check Data Lake (MinIO)

Visit `http://localhost:9001`.
Navigate to bucket `warehouse`.
You should see the path: `data/raw/flights/year=.../month=.../`

### Check Database (Neon/PostgreSQL)

Connect to your database using DBeaver or any SQL client and run:

```sql
SELECT * FROM kpi_airline_daily_stats ORDER BY total_flights DESC;
```

## Troubleshooting

- **No such host is known (minio):** Spark cannot find the MinIO container name. Ensure your config uses `http://localhost:9000` when running scripts from the host machine.
- **FileNotFoundException: Hadoop home...** Missing `winutils.exe`. Verify that you created `C:\hadoop\bin\winutils.exe`.
- **ConnectionRefused (Kafka):** Kafka might not be ready. Check `docker ps` to ensure port 9092 is mapped.
