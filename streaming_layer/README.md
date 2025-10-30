# Streaming layer

## 1. Khởi tạo

```bash
cd docker
docker compose -f docker_compose.yml up -d
```

## 2. Triển khai
Lần lượt chạy file:
- ```produce_streaming_event.py```: gửi data vào Kafka)
- ```spark_consumer.py```: Spark nhận event từ Kafka và xử lý