## Prerequisites

Before running the project, ensure you have the following installed:

- Docker Desktop (Running Linux containers)
- Python 3.10
- Java (JDK 8 or 11) (Required for Spark)
- **(Windows Users)**: winutils.exe setup:
  - Create directory: `C:\hadoop\bin`
  - Download `winutils.exe` and place it inside the bin folder.
  - Note: The project code is configured to look for this path automatically.

## How to Run

### Step 1: Setup Infrastructure

Start Kafka and HBase containers using Docker:

```bash
cd docker
docker compose -f docker_compose.yml up -d
```

Wait 1-2 minutes for services to initialize.

### Step 2: Install Dependencies

```bash
pip install -r requirements.txt
```

### Step 3: Add .env

Create .env file in folder consumer and config python path (Only for Window) and token Astra for Cassandra\
For example:
```
PYSPARK_PYTHON=C:\Users\Lenovo\miniconda3\envs\pyspark_3.10\python.exe (Only for Windows)
PYSPARK_DRIVER_PYTHON=C:\Users\Lenovo\miniconda3\envs\pyspark_3.10\python.exe (Only for Windows)
ASTRA_PASSWORD=AstraCS:xxxxxxxxxx
```

### Step 4: Start producing data to Kafka (Producer)

Open a terminal and run the producer to start sending flight data to Kafka:

```bash
python src/producer/produce_streaming_event.py
```

### Step 5: Run Spark Consumer

This job reads from Kafka, parse and use ML model to predict and save to Cassandra

```bash
python src/consumer/spark_consumer_cassandra.py
```

