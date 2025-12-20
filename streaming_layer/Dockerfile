# Python 3.10 + Debian Bullseye (ổn định cho Java 11)
FROM python:3.10-slim-bullseye

# Install Java 11 (JRE headless)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-11-jre-headless \
        curl \
        procps && \
    rm -rf /var/lib/apt/lists/*

# Environment
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Workdir
WORKDIR /app

# Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Application source
COPY src ./src
COPY Model ./Model

# Default entrypoint
ENTRYPOINT ["python"]
