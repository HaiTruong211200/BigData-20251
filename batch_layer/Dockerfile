# 1. Dùng base image rõ ràng là "bookworm" (Debian 12) để chắc chắn có Java mới
FROM python:3.11-slim-bookworm

# 2. Cài Java bằng gói 'default-jre' (An toàn hơn gọi tên cụ thể)
# Thêm --fix-missing để cố gắng sửa lỗi nếu mạng rớt gói tin
RUN apt-get update && \
    apt-get install -y --no-install-recommends --fix-missing default-jre procps && \
    rm -rf /var/lib/apt/lists/*

# Kiểm tra version Java ngay khi build để chắc chắn nó đã được cài
RUN java -version

# Thiết lập biến môi trường (Mặc định cho Debian/Ubuntu)
ENV JAVA_HOME=/usr/lib/jvm/default-java

# 3. Thiết lập thư mục
WORKDIR /app

# 4. Copy file PySpark (Phương án 3 cũ)
COPY pyspark-3.5.1.tar.gz .

# 5. Copy requirements
COPY requirements.txt .

# 6. Cài đặt
RUN pip install --no-cache-dir pyspark-3.5.1.tar.gz

# Cài các thư viện khác (bỏ qua pyspark nếu có trong file txt)
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt


# 7. Copy code
COPY . .

# 8. Env vars
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

CMD ["python", "src/etl_job.py"]