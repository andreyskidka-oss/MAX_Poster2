"""
watchdog.py — внутренний мониторинг здоровья всех компонентов.

Каждый компонент (polling, queue worker, API) периодически обновляет
свой heartbeat timestamp. Watchdog проверяет свежесть этих heartbeat-ов
и убивает процесс если какой-то компонент перестал отвечать.

Три уровня защиты от зависаний:
1. Watchdog (этот модуль) — ловит зависшие asyncio-задачи
2. Docker HEALTHCHECK → /health — ловит зависание всего процесса
3. Docker restart: unless-stopped — перезапускает контейнер

Зачем убивать процесс, а не перезапускать задачу:
- asyncio-задачи могут делить состояние (pending_states, db connection)
- перезапуск одной задачи не гарантирует чистое состояние
- Docker перезапустит контейнер за секунды — это надёжнее
"""

import asyncio
import os
import signal
import time

from loguru import logger

# Максимальное время без heartbeat перед аварийным завершением (секунды)
# Long Polling таймаут = 30 сек, поэтому polling heartbeat приходит
# минимум раз в ~35 сек. Даём тройной запас.
DEFAULT_MAX_STALE_SECONDS = 120


class Heartbeat:
    """
    Хранилище heartbeat-ов от всех компонентов.

    Каждый компонент вызывает heartbeat.ping("component_name")
    при каждой итерации своего цикла. Watchdog проверяет свежесть.
    """

    def __init__(self):
        self._beats: dict[str, float] = {}
        self._started_at: float = time.monotonic()

    def ping(self, component: str) -> None:
        """Обновить heartbeat компонента. Вызывать при каждой итерации."""
        self._beats[component] = time.monotonic()

    def get_status(self) -> dict:
        """
        Получить статус всех компонентов.
        Возвращает dict с age (секунды с последнего ping) каждого компонента.
        """
        now = time.monotonic()
        status = {}
        for name, last_beat in self._beats.items():
            age = round(now - last_beat, 1)
            status[name] = {
                "last_ping_ago_sec": age,
                "alive": True,
            }
        return status

    def check_stale(self, max_stale_seconds: float = DEFAULT_MAX_STALE_SECONDS) -> list[str]:
        """
        Проверить какие компоненты зависли (не слали heartbeat дольше max_stale_seconds).
        Возвращает список имён зависших компонентов.
        """
        now = time.monotonic()
        stale = []
        for name, last_beat in self._beats.items():
            if (now - last_beat) > max_stale_seconds:
                stale.append(name)
        return stale

    @property
    def uptime_seconds(self) -> float:
        return round(time.monotonic() - self._started_at, 1)

    @property
    def components(self) -> list[str]:
        return list(self._beats.keys())


async def watchdog_loop(
    heartbeat: Heartbeat,
    check_interval: int = 30,
    max_stale_seconds: int = DEFAULT_MAX_STALE_SECONDS,
    grace_period: int = 60,
) -> None:
    """
    Периодически проверяет heartbeat-ы всех компонентов.

    При обнаружении зависшего компонента:
    1. Логирует CRITICAL
    2. Отправляет SIGTERM текущему процессу
    → Docker перезапустит контейнер благодаря restart: unless-stopped

    Args:
        heartbeat: экземпляр Heartbeat
        check_interval: секунды между проверками
        max_stale_seconds: максимальный возраст heartbeat до аварии
        grace_period: секунды после старта, в течение которых не проверяем
                      (даём время на инициализацию)
    """
    logger.info(
        f"Watchdog started: check every {check_interval}s, "
        f"stale threshold {max_stale_seconds}s, "
        f"grace period {grace_period}s"
    )

    # Даём компонентам время на инициализацию
    await asyncio.sleep(grace_period)

    while True:
        try:
            await asyncio.sleep(check_interval)

            stale = heartbeat.check_stale(max_stale_seconds)
            if stale:
                logger.critical(
                    f"Watchdog: компоненты зависли: {stale}. "
                    f"Нет heartbeat {max_stale_seconds}+ секунд. "
                    f"Завершаем процесс для перезапуска Docker."
                )
                # SIGTERM даёт asyncio время на graceful shutdown
                os.kill(os.getpid(), signal.SIGTERM)
                # Даём 5 секунд на graceful, потом SIGKILL
                await asyncio.sleep(5)
                os.kill(os.getpid(), signal.SIGKILL)
            else:
                components = heartbeat.get_status()
                ages = {
                    name: f"{info['last_ping_ago_sec']}s"
                    for name, info in components.items()
                }
                logger.debug(f"Watchdog OK: {ages}")

        except asyncio.CancelledError:
            logger.info("Watchdog stopped")
            break
        except Exception as e:
            logger.error("Watchdog error: %s" % e)
