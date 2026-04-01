# MAX Poster Bot v3.4 — Руководство по установке

## Требования
- Docker + Docker Compose
- Ubuntu 22.04+ / Debian 12+
- 1 vCPU, 512 MB RAM
- Токен бота MAX (business.max.ru → Чат-боты → Интеграция)

## Структура проекта

```
max_poster/
├── .env                    # Переменные окружения
├── Dockerfile              # Сборка контейнера
├── docker-compose.yml      # Оркестрация + autoheal
├── requirements.txt        # Python-зависимости
├── README.md               # Документация
├── INSTALL.md              # Это руководство
├── src/
│   ├── bot.py              # Диспетчер бота (3761 строк)
│   ├── api.py              # REST API сервер
│   ├── database.py         # SQLite
│   ├── max_client.py       # HTTP-клиент MAX API
│   ├── config.py           # Конфигурация
│   ├── main.py             # Точка входа
│   ├── admin.py            # Администраторы
│   ├── rate_limiter.py     # Ограничитель скорости
│   ├── watchdog.py         # Мониторинг
│   └── test_preview.png    # Превью кнопок
├── data/
│   └── db.sqlite           # БД (создаётся автоматически)
└── logs/
    └── app_YYYY-MM-DD.log  # Логи (ротация по дням)
```

## Быстрый старт

### 1. Файлы на сервер

```bash
mkdir -p /root/max_poster
cd /root/max_poster
# Распаковать архив или клонировать репозиторий
```

### 2. Переменные окружения

```bash
cp .env.example .env
nano .env
```

Обязательные:
```env
MAX_BOT_TOKEN=ваш_токен_бота
ADMIN_IDS=123456789
```

Опциональные:
```env
API_PORT=8080
DB_PATH=/app/data/db.sqlite
LOG_DIR=/app/logs
API_BASE_URL=https://your-domain.com
WEBHOOK_URL=https://your-backend.com/webhook
TARIFF_API_URL=http://localhost:8080/mgw_acc_status/
```

### 3. Dockerfile

```dockerfile
FROM python:3.12-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY src/ ./src/
RUN mkdir -p /app/data /app/logs
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1
CMD ["python", "src/main.py"]
```

### 4. docker-compose.yml

```yaml
services:
  max_poster:
    build: .
    container_name: max_poster
    restart: unless-stopped
    env_file: .env
    ports:
      - "8080:8080"
    volumes:
      - ./data:/app/data
      - ./logs:/app/logs
    environment:
      - PYTHONUNBUFFERED=1
      - TZ=Europe/Moscow
    ulimits:
      nofile:
        soft: 65536
        hard: 65536
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8080/health"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 60s
    labels:
      - "autoheal=true"
    logging:
      driver: "json-file"
      options:
        max-size: "10m"
        max-file: "5"

  autoheal:
    image: willfarrell/autoheal
    container_name: autoheal
    restart: unless-stopped
    environment:
      - AUTOHEAL_CONTAINER_LABEL=autoheal
      - AUTOHEAL_INTERVAL=30
      - AUTOHEAL_START_PERIOD=120
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
```

### 5. Запуск

```bash
mkdir -p data logs
docker-compose up -d --build
docker logs -f max_poster
```

Должно появиться:
```
Запуск MAX Poster...
Таблицы БД созданы/проверены
Long Polling started
Scheduler loop started: interval 30s
REST API v3.0 started on port 8080
```

## HTTPS (nginx)

```bash
# Сертификат
sudo certbot certonly --standalone -d your-domain.com

# Конфиг nginx
nano /etc/nginx/sites-available/your-domain.com
```

```nginx
server {
    listen 443 ssl;
    server_name your-domain.com;
    ssl_certificate /etc/letsencrypt/live/your-domain.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/your-domain.com/privkey.pem;
    location / {
        proxy_pass http://127.0.0.1:8080;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

```bash
ln -sf /etc/nginx/sites-available/your-domain.com /etc/nginx/sites-enabled/
nginx -t && systemctl restart nginx
```

## Обновление

```bash
cd /root/max_poster && docker-compose down
cd src
curl -sLO https://raw.githubusercontent.com/USER/REPO/main/src/bot.py
curl -sLO https://raw.githubusercontent.com/USER/REPO/main/src/database.py
curl -sLO https://raw.githubusercontent.com/USER/REPO/main/src/max_client.py
cd .. && docker-compose up -d --build
docker logs -f max_poster
```

## Проверка

```bash
curl -k https://your-domain.com/health
docker logs max_poster --tail 50
docker exec max_poster sqlite3 /app/data/db.sqlite "SELECT count(*) FROM users;"
```

## Решение проблем

**Бот не отвечает:** `docker logs max_poster --tail 100`

**401 от MAX API:** проверить `MAX_BOT_TOKEN` в .env

**400 при кнопках:** `grep BATCH_BUTTONS logs/app_$(date +%Y-%m-%d).log` — fallback без кнопок

**БД заблокирована:** `docker-compose restart`

**Autoheal перезапускает:** проверить healthcheck порт совпадает с API_PORT
