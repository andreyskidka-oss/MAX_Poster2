"""
max_client.py — HTTP-клиент для MAX Bot API.

Инкапсулирует всю работу с platform-api.max.ru:
- Long Polling (получение событий)
- Отправка сообщений через RateLimitedQueue
- Загрузка медиафайлов (изображения, видео)
- Ответы на callback (без очереди — немедленно)
- Получение информации о чате и участниках
- Удаление сообщений (для очистки чата)
- Проверка прав бота в канале

Используется ОДНА общая aiohttp.ClientSession на всё время жизни.
"""

import asyncio
from urllib.parse import urlparse
import os
import json

import aiohttp
from loguru import logger

from rate_limiter import RateLimitedQueue

MAX_API_BASE = "https://platform-api.max.ru"
LONG_POLL_TIMEOUT = 30

_VIDEO_EXTENSIONS = {".mp4", ".mov", ".avi", ".webm", ".mkv", ".3gp", ".flv", ".wmv"}


def _detect_media_type(url: str) -> str:
    path = urlparse(url).path.lower()
    _, ext = os.path.splitext(path)
    return "video" if ext in _VIDEO_EXTENSIONS else "image"


class MaxAPIError(Exception):
    pass


class MaxClient:
    _RETRYABLE_STATUSES = {500, 502, 503, 504}

    def __init__(self, token: str, queue: RateLimitedQueue):
        self._token = token
        self._queue = queue
        self._headers = {
            "Authorization": token,
            "Content-Type": "application/json",
        }
        self._session: aiohttp.ClientSession | None = None
        self._title_cache: dict[str, tuple[str, float]] = {}  # chat_id → (title, timestamp)
        self._TITLE_CACHE_TTL = 300  # 5 минут

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30),
            )
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    async def _post(self, path: str, body: dict) -> dict:
        session = await self._get_session()
        attempts = 3
        backoff = 1.0
        last_error = None
        for attempt in range(1, attempts + 1):
            async with session.post(
                "%s%s" % (MAX_API_BASE, path),
                headers=self._headers, json=body,
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                try:
                    data = await resp.json(content_type=None)
                except Exception:
                    data = {"raw": await resp.text()}

                if resp.status in (200, 201):
                    return data

                if resp.status == 429:
                    logger.warning("Rate limited on %s, retrying in 2s" % path)
                    last_error = MaxAPIError("HTTP %s on %s: %s" % (resp.status, path, data))
                    await asyncio.sleep(2)
                    continue

                if resp.status in self._RETRYABLE_STATUSES and attempt < attempts:
                    logger.warning(
                        "Retryable HTTP %s on %s (attempt %s/%s), retrying in %.1fs | body_sent=%s | response=%s"
                        % (resp.status, path, attempt, attempts, backoff,
                           json.dumps({k: v for k, v in body.items() if k != "text"}, ensure_ascii=False),
                           json.dumps(data, ensure_ascii=False))
                    )
                    last_error = MaxAPIError("HTTP %s on %s: %s" % (resp.status, path, data))
                    await asyncio.sleep(backoff)
                    backoff *= 2
                    continue

                # Полное логирование при 400/500 ошибках
                logger.error(
                    "API error HTTP %s on %s | response=%s | body_keys=%s | text_len=%s | att_types=%s"
                    % (resp.status, path,
                       json.dumps(data, ensure_ascii=False)[:500],
                       list(body.keys()),
                       len(str(body.get("text", ""))),
                       [a.get("type") for a in body.get("attachments", []) if isinstance(a, dict)])
                )
                raise MaxAPIError("HTTP %s on %s: %s" % (resp.status, path, data))

        if last_error:
            raise last_error
        raise MaxAPIError("Unknown POST error on %s" % path)

    async def _get(self, path: str, params: dict | None = None) -> dict:
        session = await self._get_session()
        async with session.get(
            "%s%s" % (MAX_API_BASE, path),
            headers=self._headers, params=params,
            timeout=aiohttp.ClientTimeout(total=LONG_POLL_TIMEOUT + 10),
        ) as resp:
            data = await resp.json()
            if resp.status not in (200, 201):
                raise MaxAPIError("HTTP %s on %s: %s" % (resp.status, path, data))
            return data

    async def _put(self, path: str, body: dict) -> dict:
        session = await self._get_session()
        async with session.put(
            "%s%s" % (MAX_API_BASE, path),
            headers=self._headers, json=body,
            timeout=aiohttp.ClientTimeout(total=30),
        ) as resp:
            data = await resp.json()
            if resp.status not in (200, 201):
                raise MaxAPIError("HTTP %s on %s: %s" % (resp.status, path, data))
            return data

    async def _delete(self, path: str) -> dict | None:
        session = await self._get_session()
        async with session.delete(
            "%s%s" % (MAX_API_BASE, path),
            headers=self._headers,
            timeout=aiohttp.ClientTimeout(total=30),
        ) as resp:
            if resp.status in (200, 201, 204):
                try:
                    return await resp.json()
                except Exception:
                    return {}
            data = await resp.json() if resp.content_length else {}
            raise MaxAPIError("HTTP %s on DELETE %s: %s" % (resp.status, path, data))

    # ─── Загрузка медиафайлов ────────────────────────────────────────────

    async def upload_media(self, url: str) -> dict:
        media_type = _detect_media_type(url)
        session = await self._get_session()

        async with session.get(url, timeout=aiohttp.ClientTimeout(total=60)) as resp:
            if resp.status != 200:
                raise MaxAPIError("Failed to download media from %s: HTTP %s" % (url, resp.status))
            file_data = await resp.read()
            content_type = resp.headers.get("Content-Type", "application/octet-stream")

        async with session.post(
            "%s/uploads" % MAX_API_BASE,
            headers=self._headers, params={"type": media_type},
            timeout=aiohttp.ClientTimeout(total=15),
        ) as resp:
            upload_info = await resp.json()
            if resp.status not in (200, 201):
                raise MaxAPIError("Failed to get upload URL: %s" % upload_info)

        upload_url = upload_info.get("url")
        if not upload_url:
            raise MaxAPIError("No upload URL in response: %s" % upload_info)

        pre_token = upload_info.get("token")

        form = aiohttp.FormData()
        filename = os.path.basename(urlparse(url).path) or "file"
        form.add_field("data", file_data, filename=filename, content_type=content_type)

        async with session.post(
            upload_url,
            headers={"Authorization": self._token},
            data=form,
            timeout=aiohttp.ClientTimeout(total=120),
        ) as resp:
            if resp.status not in (200, 201):
                body = await resp.text()
                raise MaxAPIError("Failed to upload media (HTTP %s): %s" % (resp.status, body[:200]))

            upload_result = {}
            if media_type not in ("video", "audio"):
                try:
                    upload_result = await resp.json(content_type=None)
                except Exception:
                    pass

        if media_type in ("video", "audio"):
            token = pre_token
            if not token:
                raise MaxAPIError("No token for %s in /uploads response" % media_type)
        else:
            token = upload_result.get("token")
            if not token:
                photos = upload_result.get("photos")
                if photos and isinstance(photos, dict):
                    first = list(photos.values())[0]
                    if isinstance(first, dict):
                        token = first.get("token")

        if not token:
            raise MaxAPIError("No token in upload response: %s" % upload_result)

        logger.debug("Uploaded %s from %s, token: %s" % (media_type, url, token[:20]))

        if media_type in ("video", "audio"):
            await asyncio.sleep(3)

        return {"type": media_type, "payload": {"token": token}}

    async def upload_local_image(self, file_path: str) -> dict:
        """
        Загрузить локальный файл изображения в MAX и получить токен.
        Возвращает {"type": "image", "payload": {"token": "..."}}.
        """
        import mimetypes
        session = await self._get_session()

        with open(file_path, "rb") as f:
            file_data = f.read()

        content_type = mimetypes.guess_type(file_path)[0] or "image/png"
        filename = os.path.basename(file_path)

        # Получаем upload URL
        async with session.post(
            "%s/uploads" % MAX_API_BASE,
            headers=self._headers, params={"type": "image"},
            timeout=aiohttp.ClientTimeout(total=15),
        ) as resp:
            upload_info = await resp.json()
            if resp.status not in (200, 201):
                raise MaxAPIError("Failed to get upload URL: %s" % upload_info)

        upload_url = upload_info.get("url")
        if not upload_url:
            raise MaxAPIError("No upload URL in response: %s" % upload_info)

        form = aiohttp.FormData()
        form.add_field("data", file_data, filename=filename, content_type=content_type)

        async with session.post(
            upload_url,
            headers={"Authorization": self._token},
            data=form,
            timeout=aiohttp.ClientTimeout(total=60),
        ) as resp:
            if resp.status not in (200, 201):
                body = await resp.text()
                raise MaxAPIError("Failed to upload local image (HTTP %s): %s" % (resp.status, body[:200]))
            try:
                upload_result = await resp.json(content_type=None)
            except Exception:
                upload_result = {}

        token = upload_result.get("token")
        if not token:
            photos = upload_result.get("photos")
            if photos and isinstance(photos, dict):
                first = list(photos.values())[0]
                if isinstance(first, dict):
                    token = first.get("token")

        if not token:
            raise MaxAPIError("No token in upload response: %s" % upload_result)

        logger.debug("Uploaded local image %s, token: %s" % (filename, token[:20]))
        return {"type": "image", "payload": {"token": token}}

    async def reupload_token(self, token: str, media_type: str) -> str | None:
        """
        Скачать медиафайл по токену MAX и перезалить его от имени бота.
        Нужно для видео из пересланных сообщений — чужой токен MAX не принимает.
        Возвращает новый токен или None при ошибке.

        MAX отдаёт файл по URL вида: https://platform-api.max.ru/media/{token}
        """
        session = await self._get_session()
        download_url = "%s/media/%s" % (MAX_API_BASE, token)
        try:
            async with session.get(
                download_url,
                headers={"Authorization": self._token},
                timeout=aiohttp.ClientTimeout(total=90),
                allow_redirects=True,
            ) as resp:
                if resp.status != 200:
                    logger.warning(
                        "reupload_token: cannot download media token %s… HTTP %s" % (token[:16], resp.status)
                    )
                    return None
                file_data = await resp.read()
                content_type = resp.headers.get("Content-Type", "application/octet-stream")
                # Определяем расширение из Content-Type
                ext_map = {
                    "video/mp4": ".mp4", "video/quicktime": ".mov",
                    "video/webm": ".webm", "video/x-matroska": ".mkv",
                    "image/jpeg": ".jpg", "image/png": ".png", "image/gif": ".gif",
                }
                ext = ext_map.get(content_type.split(";")[0].strip(), "")
                filename = "media%s" % ext or "media"
        except Exception as e:
            logger.warning("reupload_token: download failed for token %s: %s" % (token[:16], e))
            return None

        # Получаем upload URL от MAX
        try:
            async with session.post(
                "%s/uploads" % MAX_API_BASE,
                headers=self._headers,
                params={"type": media_type},
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                upload_info = await resp.json()
                if resp.status not in (200, 201):
                    logger.warning("reupload_token: /uploads failed: %s" % upload_info)
                    return None
        except Exception as e:
            logger.warning("reupload_token: /uploads request failed: %s" % e)
            return None

        upload_url = upload_info.get("url")
        pre_token = upload_info.get("token")
        if not upload_url:
            return None

        form = aiohttp.FormData()
        form.add_field("data", file_data, filename=filename, content_type=content_type)
        try:
            async with session.post(
                upload_url,
                headers={"Authorization": self._token},
                data=form,
                timeout=aiohttp.ClientTimeout(total=120),
            ) as resp:
                if resp.status not in (200, 201):
                    body_text = await resp.text()
                    logger.warning(
                        "reupload_token: upload failed HTTP %s: %s" % (resp.status, body_text[:200])
                    )
                    return None
                upload_result = {}
                if media_type not in ("video", "audio"):
                    try:
                        upload_result = await resp.json(content_type=None)
                    except Exception:
                        pass
        except Exception as e:
            logger.warning("reupload_token: upload POST failed: %s" % e)
            return None

        if media_type in ("video", "audio"):
            new_token = pre_token
        else:
            new_token = upload_result.get("token")
            if not new_token:
                photos = upload_result.get("photos")
                if photos and isinstance(photos, dict):
                    first = list(photos.values())[0]
                    if isinstance(first, dict):
                        new_token = first.get("token")

        if new_token:
            logger.debug(
                "reupload_token: OK %s → new token %s" % (token[:16], new_token[:16])
            )
            if media_type in ("video", "audio"):
                await asyncio.sleep(3)
        return new_token

    # ─── Отправка сообщений ──────────────────────────────────────────────

    def _build_message_body(self, text: str,
                            attachments: list | None = None,
                            format: str | None = "html",
                            markup: list | None = None) -> dict:
        body: dict = {"text": text}
        if format:
            body["format"] = format
        if markup:
            body["markup"] = markup
        if attachments:
            body["attachments"] = attachments
        return body

    @staticmethod
    def _payload_summary(body: dict) -> str:
        attachments = body.get("attachments") or []
        markup = body.get("markup") or []
        return json.dumps({
            "text_len": len(str(body.get("text", ""))),
            "format": body.get("format"),
            "markup_count": len(markup),
            "attachments": [att.get("type") for att in attachments if isinstance(att, dict)],
            "has_keyboard": any(att.get("type") == "inline_keyboard" for att in attachments if isinstance(att, dict)),
        }, ensure_ascii=False)

    @staticmethod
    def _strip_html(text: str) -> str:
        import re
        if not text:
            return " "
        plain = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)
        plain = re.sub(r"</p\s*>", "\n", plain, flags=re.IGNORECASE)
        plain = re.sub(r"<[^>]+>", "", plain)
        plain = (plain
                 .replace("&nbsp;", " ")
                 .replace("&lt;", "<")
                 .replace("&gt;", ">")
                 .replace("&amp;", "&")
                 .replace("&quot;", '"'))
        plain = re.sub(r"\n{3,}", "\n\n", plain).strip()
        return plain or " "

    @staticmethod
    def _dump_debug_payload(body: dict) -> None:
        try:
            debug_dir = os.environ.get("MAX_POSTER_DEBUG_DIR", "/app/logs").strip() or "/app/logs"
            os.makedirs(debug_dir, exist_ok=True)

            text = str(body.get("text", "") or "")
            json_path = os.path.join(debug_dir, "last_max_payload.json")
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(body, f, ensure_ascii=False, indent=2)

            if text:
                ext = "html" if body.get("format") == "html" else "txt"
                text_path = os.path.join(debug_dir, f"last_max_payload.{ext}")
                with open(text_path, "w", encoding="utf-8") as f:
                    f.write(text)
                logger.debug("Debug payload saved: %s, %s" % (json_path, text_path))
            else:
                logger.debug("Debug payload saved: %s" % json_path)
        except Exception as e:
            logger.warning("Failed to dump debug payload: %s" % e)

    async def _send_message_with_fallback(self, chat_id: str, text: str,
                                          attachments: list | None = None,
                                          format: str | None = "html",
                                          markup: list | None = None) -> dict:
        body = self._build_message_body(text, attachments=attachments, format=format, markup=markup)
        self._dump_debug_payload(body)
        logger.debug("MAX /messages payload chat_id=%s %s" % (chat_id, self._payload_summary(body)))

        path = "/messages?chat_id=%s" % chat_id
        try:
            return await self._post(path, body)
        except MaxAPIError as e:
            if format != "html":
                raise
            logger.warning("HTML send failed for chat %s, trying safe plain-text fallback: %s" % (chat_id, e))

        plain_text = self._strip_html(str(text or ""))

        fallback_body = self._build_message_body(plain_text, attachments=attachments, format=None, markup=None)
        self._dump_debug_payload(fallback_body)
        logger.debug("MAX /messages fallback payload chat_id=%s %s" % (chat_id, self._payload_summary(fallback_body)))
        try:
            return await self._post(path, fallback_body)
        except MaxAPIError as e_plain:
            if attachments:
                logger.warning("Plain-text with attachments failed for chat %s, trying split fallback: %s" % (chat_id, e_plain))
                media_only = [att for att in (attachments or []) if isinstance(att, dict) and att.get("type") != "inline_keyboard"]
                keyboard_only = [att for att in (attachments or []) if isinstance(att, dict) and att.get("type") == "inline_keyboard"]

                if media_only:
                    media_body = self._build_message_body(" ", attachments=media_only, format=None, markup=None)
                    self._dump_debug_payload(media_body)
                    await self._post(path, media_body)

                text_body = self._build_message_body(plain_text, attachments=keyboard_only or None, format=None, markup=None)
                self._dump_debug_payload(text_body)
                logger.debug("MAX /messages split-text payload chat_id=%s %s" % (chat_id, self._payload_summary(text_body)))
                return await self._post(path, text_body)
            raise e_plain

    async def send_message(self, chat_id: str, text: str,
                           attachments: list | None = None,
                           format: str | None = "html",
                           markup: list | None = None) -> dict:
        """Отправить сообщение через очередь (rate limited)."""
        return await self._queue.enqueue(
            self._send_message_with_fallback, chat_id, text, attachments, format, markup
        )

    async def send_message_direct(self, chat_id: str, text: str,
                                  attachments: list | None = None,
                                  format: str | None = "html",
                                  markup: list | None = None) -> dict:
        """Отправить сообщение напрямую БЕЗ очереди (для уведомлений)."""
        return await self._send_message_with_fallback(chat_id, text, attachments, format, markup)

    # ─── Удаление сообщений ──────────────────────────────────────────────

    async def delete_message(self, message_id: str) -> None:
        """Удалить сообщение бота по message_id."""
        try:
            await self._delete("/messages?message_id=%s" % message_id)
        except MaxAPIError as e:
            logger.debug("Delete message %s failed: %s" % (message_id, e))

    # ─── Закрепление сообщений ───────────────────────────────────────────

    async def pin_message(self, chat_id: str, message_id: str, notify: bool = False) -> bool:
        """Закрепить сообщение в канале. Возвращает True при успехе."""
        try:
            session = await self._get_session()
            async with session.put(
                "%s/chats/%s/pin" % (MAX_API_BASE, chat_id),
                headers=self._headers,
                json={"message_id": message_id, "notify": notify},
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                ok = resp.status in (200, 201)
                if not ok:
                    body = await resp.text()
                    logger.warning("Pin failed chat=%s mid=%s HTTP %s: %s" % (chat_id, message_id, resp.status, body[:200]))
                else:
                    logger.info("Pin OK chat=%s mid=%s" % (chat_id, message_id))
                return ok
        except Exception as e:
            logger.error("Pin message failed chat=%s mid=%s: %s" % (chat_id, message_id, e))
            return False

    async def unpin_message(self, chat_id: str) -> bool:
        """Открепить сообщение в канале."""
        try:
            session = await self._get_session()
            async with session.delete(
                "%s/chats/%s/pin" % (MAX_API_BASE, chat_id),
                headers=self._headers,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                return resp.status in (200, 201, 204)
        except Exception as e:
            logger.debug("Unpin failed chat=%s: %s" % (chat_id, e))
            return False

    async def get_pinned_message(self, chat_id: str) -> dict | None:
        """Получить закреплённое сообщение."""
        try:
            return await self._get("/chats/%s/pin" % chat_id)
        except MaxAPIError:
            return None

    # ─── Редактирование сообщений ────────────────────────────────────────

    async def edit_message(self, message_id: str, text: str = None,
                           attachments: list | None = None) -> dict:
        """Редактировать сообщение (лимит MAX: 24 часа)."""
        body = {}
        if text is not None:
            body["text"] = text
            body["format"] = "html"
        if attachments:
            body["attachments"] = attachments
        return await self._put("/messages?message_id=%s" % message_id, body)

    # ─── Callback ────────────────────────────────────────────────────────

    async def answer_callback(self, callback_id: str, notification: str = "") -> None:
        try:
            session = await self._get_session()
            async with session.post(
                "%s/answers/callbacks/%s" % (MAX_API_BASE, callback_id),
                headers=self._headers,
                json={"notification": notification},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status not in (200, 201):
                    body = await resp.text()
                    logger.debug("answer_callback HTTP %s: %s" % (resp.status, body[:100]))
        except Exception as e:
            logger.error("answer_callback failed: %s" % e)

    async def send_action(self, chat_id: str, action: str = "mark_seen") -> None:
        """Отправить действие в чат (mark_seen, typing_on, typing_off, sending_photo и др.)."""
        try:
            session = await self._get_session()
            async with session.post(
                "%s/chats/%s/actions" % (MAX_API_BASE, chat_id),
                headers=self._headers,
                json={"action": action},
                timeout=aiohttp.ClientTimeout(total=5),
            ) as resp:
                pass
        except Exception:
            pass

    # ─── Long Polling ────────────────────────────────────────────────────

    async def get_updates(self, marker: int | None = None) -> dict:
        params = {"timeout": LONG_POLL_TIMEOUT}
        if marker is not None:
            params["marker"] = marker
        return await self._get("/updates", params)

    # ─── Информация о чате ───────────────────────────────────────────────

    async def get_chat_title(self, chat_id: str) -> str:
        import time as _time
        # Кеш названий каналов (5 мин, макс 500 записей)
        cached = self._title_cache.get(chat_id)
        if cached and (_time.monotonic() - cached[1]) < self._TITLE_CACHE_TTL:
            return cached[0]
        try:
            data = await self._get("/chats/%s" % chat_id)
            title = data.get("title", "Канал %s" % chat_id)
            if len(self._title_cache) > 500:
                self._title_cache.clear()
            self._title_cache[chat_id] = (title, _time.monotonic())
            return title
        except MaxAPIError:
            return "Канал %s" % chat_id

    async def get_chat_info(self, chat_id: str) -> dict | None:
        """Получить полную информацию о чате/канале."""
        try:
            return await self._get("/chats/%s" % chat_id)
        except MaxAPIError:
            return None

    # ─── Участники канала ────────────────────────────────────────────────

    async def get_chat_members(self, chat_id: str) -> list[dict]:
        """Получить список участников чата/канала."""
        try:
            data = await self._get("/chats/%s/members" % chat_id)
            return data.get("members", [])
        except MaxAPIError as e:
            logger.error("get_chat_members %s failed: %s" % (chat_id, e))
            return []

    async def check_bot_is_admin(self, chat_id: str) -> bool:
        """
        Проверить, является ли бот администратором канала.
        Используем GET /chats/{chatId}/members/me — возвращает is_admin и permissions бота.
        """
        try:
            data = await self._get("/chats/%s/members/me" % chat_id)
            is_admin = data.get("is_admin", False)
            is_owner = data.get("is_owner", False)
            perms = data.get("permissions")
            has_write = isinstance(perms, list) and "write" in perms

            logger.info(
                "check_bot_is_admin %s: is_admin=%s, is_owner=%s, permissions=%s"
                % (chat_id, is_admin, is_owner, perms)
            )

            return bool(is_admin or is_owner or has_write)
        except Exception as e:
            logger.warning("check_bot_is_admin %s failed: %s" % (chat_id, e))
            return False

    async def get_bot_membership(self, chat_id: str) -> dict | None:
        """Получить полную информацию о членстве бота в чате."""
        try:
            return await self._get("/chats/%s/members/me" % chat_id)
        except MaxAPIError:
            return None

    async def get_messages(self, chat_id: str, count: int = 50) -> list[dict]:
        """Получить последние сообщения из чата."""
        try:
            data = await self._get("/messages", params={"chat_id": chat_id, "count": count})
            return data.get("messages", [])
        except MaxAPIError as e:
            logger.debug("get_messages %s failed: %s" % (chat_id, e))
            return []

    async def clear_bot_messages(self, chat_id: str) -> int:
        """
        Удалить все сообщения бота в чате.
        Возвращает количество удалённых сообщений.
        """
        messages = await self.get_messages(chat_id, count=50)
        deleted = 0
        for msg in messages:
            sender = msg.get("sender", {})
            if not sender.get("is_bot"):
                continue
            mid = msg.get("body", {}).get("mid")
            if not mid:
                continue
            try:
                await self.delete_message(str(mid))
                deleted += 1
                # Пауза каждые 10 удалений — не упираемся в rate limit
                if deleted % 10 == 0:
                    await asyncio.sleep(0.5)
            except Exception:
                pass
        return deleted

    # ─── Webhook ─────────────────────────────────────────────────────────

    async def send_webhook(self, url: str, payload: dict) -> int:
        """Отправить POST webhook. Возвращает HTTP status или 0 при ошибке."""
        try:
            session = await self._get_session()
            async with session.post(
                url, json=payload,
                timeout=aiohttp.ClientTimeout(total=10),
                allow_redirects=False,  # Не следуем за редиректами — тело теряется
            ) as resp:
                if 300 <= resp.status < 400:
                    location = resp.headers.get("Location", "?")
                    logger.warning(
                        "Webhook redirect %s → %s (body will be lost!). "
                        "Fix WEBHOOK_URL in .env to: %s" % (resp.status, url, location)
                    )
                elif resp.status >= 400:
                    body = await resp.text()
                    logger.warning(
                        "Webhook %s returned HTTP %s: %s" % (url, resp.status, body[:200])
                    )
                return resp.status
        except Exception as e:
            logger.error("Webhook to %s failed: %s" % (url, e))
            return 0
