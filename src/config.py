"""
config.py — загрузка и валидация переменных окружения.

Читает .env файл, проверяет наличие обязательных переменных
(MAX_BOT_TOKEN, ADMIN_IDS) и предоставляет единый объект Config.

API_BASE_URL определяется автоматически:
1. Если задан в .env → используется как есть (домен, кастомный IP)
2. Если не задан → определяется внешний IP сервера через публичные сервисы
3. Если автодетект не сработал → fallback http://localhost:PORT
"""

import os
from dataclasses import dataclass
from urllib.request import urlopen

from dotenv import load_dotenv

load_dotenv()


def _detect_external_ip() -> str | None:
    """
    Определить внешний IP сервера через публичные сервисы.
    Пробуем несколько сервисов на случай если один недоступен.
    Возвращает IP-адрес строкой или None.
    """
    services = [
        "https://api.ipify.org",
        "https://ifconfig.me/ip",
        "https://icanhazip.com",
    ]
    for url in services:
        try:
            with urlopen(url, timeout=5) as resp:
                ip = resp.read().decode().strip()
                if ip and "." in ip:
                    return ip
        except Exception:
            continue
    return None


@dataclass
class Config:
    max_bot_token: str
    admin_ids: list[str]      # строки, т.к. MAX user_id приходит как строка
    api_base_url: str         # внешний URL API, показывается пользователям в боте
    api_port: int
    db_path: str
    log_dir: str
    webhook_url: str          # URL для отправки событий (добавление/удаление каналов)
    tariff_api_url: str       # URL API тарифов (пустая строка = отключено)
    bot_link: str             # Deeplink URL бота (https://max.ru/BOT_NAME) — для кнопки Комментарии


def load_config() -> Config:
    token = os.environ.get("MAX_BOT_TOKEN", "").strip()
    if not token:
        raise ValueError("MAX_BOT_TOKEN не задан в .env")

    raw_ids = os.environ.get("ADMIN_IDS", "").strip()
    if not raw_ids:
        raise ValueError("ADMIN_IDS не задан в .env")
    admin_ids = [x.strip() for x in raw_ids.split(",") if x.strip()]

    port = int(os.environ.get("API_PORT", "8080"))

    # API_BASE_URL: ручное значение → автодетект IP → fallback localhost
    raw_url = os.environ.get("API_BASE_URL", "").strip().rstrip("/")
    if raw_url and "YOUR_SERVER_IP" not in raw_url:
        api_base_url = raw_url
    else:
        ip = _detect_external_ip()
        if ip:
            api_base_url = "http://%s:%s" % (ip, port)
        else:
            api_base_url = "http://localhost:%s" % port

    return Config(
        max_bot_token=token,
        admin_ids=admin_ids,
        api_base_url=api_base_url,
        api_port=port,
        db_path=os.environ.get("DB_PATH", "/app/data/db.sqlite"),
        log_dir=os.environ.get("LOG_DIR", "/app/logs"),
        webhook_url=os.environ.get("WEBHOOK_URL", "").strip(),
        tariff_api_url=os.environ.get("TARIFF_API_URL", "").strip(),
        bot_link=os.environ.get("BOT_LINK", "").strip().rstrip("/"),
    )


config = load_config()
