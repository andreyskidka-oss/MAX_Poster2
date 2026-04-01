"""
rate_limiter.py — rate-limited очередь для отправки запросов в MAX Bot API.

Файл намеренно назван rate_limiter.py, а не queue.py —
чтобы не перекрывать стандартный модуль Python 'queue',
который используется внутри aiosqlite и других библиотек.

MAX ограничивает до 30 rps. Очередь обеспечивает равномерную нагрузку
и защищает от 429 ошибок при пиковых запросах (100+ пользователей одновременно).
"""

import asyncio

from loguru import logger


class RateLimitedQueue:
    """
    Asyncio-очередь с ограничением скорости отправки.

    Принцип: один воркер забирает задачи по одной с паузой 1/rps между ними.
    Это гарантирует ровно rps запросов в секунду вне зависимости от нагрузки.

    Все отправки (send_message) идут через enqueue().
    answer_callback — не через очередь, нужен немедленно.
    """

    def __init__(self, rps: int = 25, heartbeat=None):
        self._queue: asyncio.Queue = asyncio.Queue()
        self._rps = rps
        # Интервал между задачами в секундах (0.04 сек при 25 rps)
        self._interval = 1.0 / rps
        self._heartbeat = heartbeat

    async def worker(self) -> None:
        """Разгребает очередь с нужным интервалом. Запускать как asyncio task."""
        logger.info(f"Queue worker started, {self._rps} rps")
        while True:
            try:
                # Таймаут на ожидание задачи — чтобы heartbeat обновлялся
                # даже когда очередь пуста (иначе watchdog решит что воркер завис)
                try:
                    task_fn = await asyncio.wait_for(self._queue.get(), timeout=30)
                except asyncio.TimeoutError:
                    # Очередь пуста — просто пингуем heartbeat и ждём дальше
                    if self._heartbeat:
                        self._heartbeat.ping("queue")
                    continue

                try:
                    await task_fn()
                except Exception as e:
                    # Логируем ошибку задачи, но не останавливаем воркер
                    logger.error("Queue task error: %s" % e)
                finally:
                    self._queue.task_done()

                # Heartbeat после каждой обработанной задачи
                if self._heartbeat:
                    self._heartbeat.ping("queue")

                # Пауза между задачами для соблюдения лимита rps
                await asyncio.sleep(self._interval)
            except asyncio.CancelledError:
                logger.info("Queue worker stopped")
                break

    async def enqueue(self, coro_fn, *args, **kwargs):
        """
        Добавить coroutine в очередь и дождаться результата.

        Принимает функцию (не coroutine!) чтобы создать её внутри задачи.
        Coroutine нельзя запустить дважды, а задача может ждать в очереди.

        Пример:
            result = await queue.enqueue(client._post, "/messages", body)
        """
        loop = asyncio.get_event_loop()
        future: asyncio.Future = loop.create_future()

        async def task():
            try:
                result = await coro_fn(*args, **kwargs)
                # Проверяем done() на случай таймаута со стороны вызывающего
                if not future.done():
                    future.set_result(result)
            except Exception as e:
                if not future.done():
                    future.set_exception(e)

        await self._queue.put(task)
        return await future
