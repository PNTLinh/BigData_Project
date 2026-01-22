FROM python:3.9-slim

WORKDIR /app

# Cài dependency
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy toàn bộ thư mục kafka vào container
COPY kafka/ /app/kafka/

# Chạy producer đúng path
CMD ["python", "/app/kafka/producer.py"]
