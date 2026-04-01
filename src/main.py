"""
main.py — точка входа приложения MAX Poster.

Порядок инициализации:
1. Настроить логирование (stderr + файлы с ротацией)
2. Инициализировать БД (создать таблицы)
3. Заполнить начальных администраторов из ADMIN_IDS
4. Создать экземпляры: heartbeat, очередь, клиент MAX, диспетчер бота
5. Запустить четыре параллельные задачи:
   - bot polling loop (получение событий от MAX)
   - REST API server (aiohttp на порту из конфига)
   - queue worker (разгребает очередь отправки с rate limit)
   - watchdog (мониторинг heartbeat-ов, перезапуск при зависании)

Три уровня защиты от зависаний:
1. Watchdog — ловит зависшие asyncio-задачи → SIGTERM → Docker restart
2. Docker HEALTHCHECK → /health — ловит зависание всего процесса
3. Docker restart: unless-stopped — перезапускает контейнер

Graceful shutdown: по SIGTERM/SIGINT задачи корректно завершаются.
"""

import asyncio
import signal
import sys
from pathlib import Path

from loguru import logger

from config import config
from database import Database
from max_client import MaxClient
from rate_limiter import RateLimitedQueue
from bot import BotDispatcher
from api import create_api_server
from watchdog import Heartbeat, watchdog_loop


async def main():
    # --- Логирование ---
    Path(config.log_dir).mkdir(parents=True, exist_ok=True)
    logger.remove()
    logger.add(sys.stderr, level="INFO")
    logger.add(
        f"{config.log_dir}/app_{{time:YYYY-MM-DD}}.log",
        rotation="10 MB",
        retention="30 days",
        level="DEBUG",
        encoding="utf-8",
    )

    logger.info("Запуск MAX Poster...")

    # --- Инициализация ---
    db = Database(config.db_path)
    await db.init()
    await db.seed_admins(config.admin_ids)
    logger.info(f"БД инициализирована. Администраторы: {config.admin_ids}")
    logger.info(f"API URL для пользователей: {config.api_base_url}")

    # Heartbeat — общий объект для отслеживания здоровья компонентов
    heartbeat = Heartbeat()

    queue = RateLimitedQueue(rps=25, heartbeat=heartbeat)
    client = MaxClient(config.max_bot_token, queue)
    bot = BotDispatcher(db, client, heartbeat=heartbeat)

    # --- Graceful shutdown ---
    def handle_signal():
        logger.info("Получен сигнал остановки, завершаем...")
        # Отменяем все задачи — asyncio.gather получит CancelledError
        for task in asyncio.all_tasks():
            task.cancel()

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, handle_signal)

    # --- Запуск пяти параллельных задач ---
    try:
        await asyncio.gather(
            bot.polling_loop(),
            create_api_server(db, client, config.api_port, heartbeat=heartbeat),
            queue.worker(),
            watchdog_loop(
                heartbeat,
                check_interval=30,      # проверять каждые 30 секунд
                max_stale_seconds=120,   # alarm если heartbeat старше 2 минут
                grace_period=60,         # первую минуту после старта не проверять
            ),
            bot.admin_check_loop(
                interval=300,            # проверять права каждые 5 минут
                grace_period=120,        # первые 2 минуты после старта не проверять
            ),
            bot.tariff_check_loop(
                interval=10800,          # проверять тарифы каждые 3 часа
            ),
            bot.scheduler_loop(
                interval=30,             # проверять очередь каждые 30 секунд
            ),
        )
    except asyncio.CancelledError:
        pass
    finally:
        # Закрываем ресурсы корректно — HTTP-сессия и БД-соединение
        await client.close()
        await db.close()
        logger.info("MAX Poster остановлен")


if __name__ == "__main__":
    asyncio.run(main())
