FROM python:3.12-slim

WORKDIR /app

# curl нужен для Docker HEALTHCHECK
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/

# Директории для данных и логов
RUN mkdir -p /app/data /app/logs

# Docker HEALTHCHECK — проверяет /health каждые 30 секунд.
# При 3 подряд неудачах контейнер помечается unhealthy.
# start_period даёт 60 секунд на инициализацию перед первой проверкой.
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

CMD ["python", "src/main.py"]
