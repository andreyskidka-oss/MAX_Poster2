"""
bot.py — диспетчер событий MAX Bot API v3.0.

Новое в v3:
- Очистка чата: бот удаляет предыдущее сообщение при отправке нового
- Уведомления с именем канала и определением прав бота
- Пакетный постинг: несколько постов → выбор кнопок → выбор каналов → отправка
- Поддержка пересланных сообщений
- Управление помощниками через канал
- Три набора кнопок на канал (анонсы/обычные/ручные)
"""

import asyncio
import html
import json
import re
import secrets
import time
from datetime import datetime, timezone, timedelta

from loguru import logger

from config import config
from database import Database
from max_client import MaxClient
from admin import AdminManager

# Максимальное время жизни pending_state (секунды).
# Если пользователь бросил batch и ушёл — через 30 мин состояние удалится.
_PENDING_TTL = 1800
# Максимальный размер _last_bot_msg (LRU-подобный лимит)
_MAX_TRACKED_USERS = 5000


class BotDispatcher:

    def __init__(self, db: Database, client: MaxClient, heartbeat=None):
        self._db = db
        self._client = client
        self._heartbeat = heartbeat
        self._pending_states: dict[str, dict] = {}
        self._admin_mgr = AdminManager(db, client, self._pending_states)
        # Трекинг последнего сообщения бота для очистки чата
        self._last_bot_msg: dict[str, str] = {}  # max_user_id -> message mid

    # ─── Housekeeping ────────────────────────────────────────────────────

    def _cleanup_stale_states(self) -> None:
        """Удалить зависшие pending_states старше _PENDING_TTL."""
        now = time.monotonic()
        stale = [uid for uid, st in self._pending_states.items()
                 if now - st.get("_ts", now) > _PENDING_TTL]
        for uid in stale:
            del self._pending_states[uid]
        if stale:
            logger.debug("Cleaned up %d stale pending states" % len(stale))

    def _set_pending(self, max_user_id: str, state: dict) -> None:
        """Установить pending state с timestamp."""
        state["_ts"] = time.monotonic()
        self._pending_states[max_user_id] = state

    # ─── Очистка чата: ответ с удалением предыдущего ─────────────────────

    async def _reply(self, chat_id: str, max_user_id: str, text: str,
                     attachments=None, keep_previous: bool = False) -> str:
        """
        Отправить сообщение и удалить предыдущее бот-сообщение.
        keep_previous=True оставляет предыдущее (для уведомлений о каналах).
        Возвращает mid нового сообщения.
        """
        if not keep_previous:
            prev_mid = self._last_bot_msg.get(max_user_id)
            if prev_mid:
                try:
                    await self._client.delete_message(prev_mid)
                except Exception:
                    pass

        result = await self._client.send_message(chat_id, text, attachments=attachments)
        mid = str(result.get("message", {}).get("body", {}).get("mid", ""))
        if mid:
            # LRU-подобная очистка: если слишком много записей, удалить старые
            if len(self._last_bot_msg) > _MAX_TRACKED_USERS:
                # Удаляем первую половину (самые старые по порядку вставки, dict сохраняет порядок)
                keys = list(self._last_bot_msg.keys())
                for k in keys[:len(keys) // 2]:
                    del self._last_bot_msg[k]
            self._last_bot_msg[max_user_id] = mid
        return mid

    # ─── Извлечение контента из сообщения ────────────────────────────────

    @staticmethod
    def _extract_content(msg: dict) -> tuple[str, list[dict], list[dict]]:
        """
        Извлечь текст, медиа-токены и markup из сообщения (обычного или пересланного).
        Поддерживает: обычные сообщения, пересылки (link), share-вложения,
        пересылки со скрытым отправителем.
        Возвращает (text, media_tokens, markup).
        """
        body = msg.get("body", {})
        text = str(body.get("text", "") or "")
        markup = body.get("markup") if isinstance(body.get("markup"), list) else []
        raw_attachments = body.get("attachments", [])

        media_tokens = []
        for att in raw_attachments:
            att_type = att.get("type")
            if att_type in ("image", "video"):
                token = att.get("payload", {}).get("token")
                if token:
                    media_tokens.append({"type": att_type, "payload": {"token": token}})
            elif att_type == "share":
                payload = att.get("payload", {})
                share_text = str(payload.get("text", "") or "")
                share_markup = payload.get("markup") if isinstance(payload.get("markup"), list) else []
                if share_text and not text:
                    text = share_text
                    markup = share_markup
                for sa in payload.get("attachments", []):
                    sa_type = sa.get("type")
                    if sa_type in ("image", "video"):
                        sa_token = sa.get("payload", {}).get("token")
                        if sa_token:
                            media_tokens.append({"type": sa_type, "payload": {"token": sa_token}})

        # Пересланное сообщение (link) — обычная пересылка
        # По схеме MAX API: link.message = MessageBody (text, attachments, markup напрямую)
        # Но на практике может быть и link.message.body.text — проверяем оба варианта
        link = msg.get("link")
        if link:
            link_msg = link.get("message", {})

            # Вариант 1: link.message.text (по официальной схеме API)
            link_text = str(link_msg.get("text", "") or "")
            link_markup_v1 = link_msg.get("markup") if isinstance(link_msg.get("markup"), list) else []

            # Вариант 2: link.message.body.text (fallback если API обернул в body)
            link_body = link_msg.get("body", {})
            link_markup_v2 = []
            if isinstance(link_body, dict):
                link_body_text = str(link_body.get("text", "") or "")
                link_markup_v2 = link_body.get("markup") if isinstance(link_body.get("markup"), list) else []
                if not link_text:
                    link_text = link_body_text

            # Берём markup: приоритет — тот что с текстом, потом мерж если разные
            if link_markup_v1:
                link_markup = link_markup_v1
            elif link_markup_v2:
                link_markup = link_markup_v2
            else:
                link_markup = []

            if link_text and not text:
                text = link_text
                markup = link_markup

            # Диагностика: логируем типы markup при пересылке
            if link_markup:
                markup_types = [m.get("type") for m in link_markup if isinstance(m, dict)]
                logger.debug("LINK_MARKUP_TYPES: %s (count=%d)" % (markup_types, len(link_markup)))

            # Диагностика: полный дамп структуры link для анализа цитат
            import json as _json
            try:
                link_dump = {
                    "link_keys": list(link.keys()),
                    "link_type": link.get("type"),
                    "link_chat_id": link.get("chat_id"),
                    "link_msg_keys": list(link_msg.keys()) if isinstance(link_msg, dict) else "not_dict",
                    "link_msg_text_len": len(link_text),
                    "link_msg_has_body": "body" in link_msg if isinstance(link_msg, dict) else False,
                }
                if isinstance(link_body, dict) and link_body:
                    link_dump["link_body_keys"] = list(link_body.keys())
                logger.debug("LINK_FULL_DUMP: %s" % _json.dumps(link_dump, ensure_ascii=False))
            except Exception:
                pass

            # Диагностика: проверяем есть ли HTML-теги в сыром тексте
            if link_text and ("<" in link_text):
                import re as _re
                html_tags = _re.findall(r"</?[a-zA-Z][^>]*>", link_text)
                if html_tags:
                    logger.warning("LINK_RAW_HTML_TAGS: %s text_preview=%s" % (html_tags[:20], repr(link_text[:200])))

            # Медиа: проверяем оба пути
            for source in (link_msg, link_msg.get("body", {})):
                if not isinstance(source, dict):
                    continue
                for att in source.get("attachments", []):
                    att_type = att.get("type")
                    if att_type in ("image", "video"):
                        token = att.get("payload", {}).get("token")
                        entry = {"type": att_type, "payload": {"token": token}}
                        if token and entry not in media_tokens:
                            media_tokens.append(entry)

        # Пересылка со скрытым отправителем — дополнительная проверка поля forward
        if not text:
            forward = msg.get("forward")
            if forward:
                fwd_msg = forward.get("message", {})
                fwd_text = str(fwd_msg.get("text", "") or "")
                fwd_markup_v1 = fwd_msg.get("markup") if isinstance(fwd_msg.get("markup"), list) else []
                fwd_body = fwd_msg.get("body", {})
                fwd_markup_v2 = []
                if isinstance(fwd_body, dict):
                    fwd_body_text = str(fwd_body.get("text", "") or "")
                    fwd_markup_v2 = fwd_body.get("markup") if isinstance(fwd_body.get("markup"), list) else []
                    if not fwd_text:
                        fwd_text = fwd_body_text
                fwd_markup = fwd_markup_v1 if fwd_markup_v1 else fwd_markup_v2
                if fwd_text:
                    text = fwd_text
                    markup = fwd_markup

        # Диагностика: если есть медиа но нет текста — логируем структуру сообщения
        if media_tokens and not text:
            import json as _json
            try:
                link_info = {}
                if link:
                    link_type = link.get("type", "?")
                    link_msg = link.get("message", {})
                    link_body = link_msg.get("body", {}) or {}
                    link_info = {
                        "link_type": link_type,
                        "link_msg_keys": list(link_msg.keys()) if isinstance(link_msg, dict) else str(type(link_msg)),
                        "link_body_keys": list(link_body.keys()) if isinstance(link_body, dict) else str(type(link_body)),
                        "link_body_text": repr(link_body.get("text", ""))[:100] if isinstance(link_body, dict) else "N/A",
                        "link_body_att_types": [a.get("type") for a in (link_body.get("attachments", []) if isinstance(link_body, dict) else [])],
                        "link_msg_att_types": [a.get("type") for a in link_msg.get("attachments", [])],
                    }
                    # Если есть sender в link — запишем (это поможет понять скрытие)
                    link_sender = link_msg.get("sender")
                    if link_sender:
                        link_info["link_sender_id"] = link_sender.get("user_id")
                        link_info["link_sender_is_bot"] = link_sender.get("is_bot")
                    else:
                        link_info["link_sender"] = None
                keys_info = {
                    "body_keys": list(body.keys()),
                    "body_text": repr(body.get("text", "")),
                    "has_link": "link" in msg,
                    "has_forward": "forward" in msg,
                    "att_types": [a.get("type") for a in raw_attachments],
                    "msg_top_keys": list(msg.keys()),
                    "link_info": link_info,
                }
                logger.warning("EXTRACT_CONTENT: media found but text empty: %s" % _json.dumps(keys_info, ensure_ascii=False))
            except Exception:
                logger.warning("EXTRACT_CONTENT: media found but text empty (logging failed)")

        return text, media_tokens, markup

    # ─── Long Polling ─────────────────────────────────────────────────────

    async def polling_loop(self) -> None:
        marker = None
        logger.info("Long Polling started")
        _cleanup_counter = 0
        _retry_delay = 1  # exponential backoff: 1, 2, 4, 8, 16, 30 (max)
        _consecutive_errors = 0
        while True:
            try:
                data = await self._client.get_updates(marker=marker)
                marker = data.get("marker", marker)
                for update in data.get("updates", []):
                    asyncio.create_task(self._dispatch(update))
                if self._heartbeat:
                    self._heartbeat.ping("polling")
                # Сброс backoff при успехе
                _retry_delay = 1
                _consecutive_errors = 0
                # Периодическая очистка (каждые ~30 итераций ≈ 15 мин)
                _cleanup_counter += 1
                if _cleanup_counter >= 30:
                    _cleanup_counter = 0
                    self._cleanup_stale_states()
            except asyncio.CancelledError:
                break
            except Exception as e:
                _consecutive_errors += 1
                if _consecutive_errors <= 3:
                    logger.warning("Long Polling error (%d): %s" % (_consecutive_errors, e))
                elif _consecutive_errors % 10 == 0:
                    # Логируем каждую 10-ю ошибку чтобы не забивать лог
                    logger.error("Long Polling: %d consecutive errors, last: %s" % (_consecutive_errors, e))
                if self._heartbeat:
                    self._heartbeat.ping("polling")
                await asyncio.sleep(_retry_delay)
                _retry_delay = min(_retry_delay * 2, 30)  # max 30 сек

    # ─── Периодическая проверка прав бота в каналах ──────────────────────

    async def admin_check_loop(self, interval: int = 300, grace_period: int = 120) -> None:
        """
        Фоновая задача: каждые interval секунд проверяет права бота
        во всех активных каналах. При изменении статуса:
        - admin → не admin: уведомление «⚠️ назначьте бота администратором»
        - не admin → admin: уведомление «👌 бот получил права, постинг подключён»
        Обновляет БД и шлёт webhook.
        """
        logger.info(
            "Admin check loop started: interval %ds, grace period %ds"
            % (interval, grace_period)
        )
        await asyncio.sleep(grace_period)

        while True:
            try:
                await asyncio.sleep(interval)
                channels = await self._db.get_all_active_channels()
                if not channels:
                    continue

                for ch in channels:
                    channel_id = ch["channel_id"]
                    was_admin = bool(ch.get("bot_is_admin"))
                    title = ch.get("title") or channel_id

                    try:
                        is_admin = await self._client.check_bot_is_admin(channel_id)
                    except Exception as e:
                        logger.debug(
                            "Admin check failed for %s: %s" % (channel_id, e)
                        )
                        continue

                    if bool(is_admin) == was_admin:
                        continue

                    # Статус изменился — обновляем БД
                    await self._db.update_bot_admin_status(channel_id, is_admin)
                    logger.info(
                        "Admin status changed: channel=%s title=%s was=%s now=%s"
                        % (channel_id, title, was_admin, is_admin)
                    )

                    # Уведомляем всех владельцев канала
                    affected_users = await self._db.get_users_by_channel(channel_id)
                    for user in affected_users:
                        chat_id = user.get("chat_id")
                        max_uid = user.get("max_user_id", "")
                        if not chat_id:
                            continue

                        if is_admin:
                            # Получил права — постинг восстановлен
                            msg = (
                                "👌 <b>Готово!</b> Бот получил нужные права.\n"
                                "Постинг в канал «%s» подключён ✅"
                            ) % html.escape(title)
                        else:
                            # Потерял права — предупреждение
                            msg = (
                                "⚠️ <b>Канал «%s»</b>\n\n"
                                "Бот потерял права администратора.\n"
                                "Назначьте бота администратором\n"
                                "и включите права публикации,\n"
                                "чтобы восстановить постинг."
                            ) % html.escape(title)

                        try:
                            await self._reply(
                                chat_id, max_uid, msg,
                                attachments=self._kb([
                                    [{"type": "callback", "text": "📡 Настройка каналов",
                                      "payload": "menu_channels"}],
                                    self._back(),
                                ]),
                                keep_previous=True,
                            )
                        except Exception as e:
                            logger.warning(
                                "Admin check notification failed user=%s: %s"
                                % (max_uid, e)
                            )

                        # Webhook — актуальный список каналов
                        await self._send_channel_webhook(user)

                    # Небольшая пауза между каналами — не перегружаем API
                    await asyncio.sleep(0.5)

                logger.debug(
                    "Admin check completed: %d channels checked" % len(channels)
                )

            except asyncio.CancelledError:
                logger.info("Admin check loop stopped")
                break
            except Exception as e:
                logger.error("Admin check loop error: %s" % e)

    async def tariff_check_loop(self, interval: int = 10800) -> None:
        """
        Фоновая задача: каждые interval секунд (по умолчанию 3 часа)
        опрашивает API тарифов и обновляет флаг активности пользователей.

        Также проверяет неподключённых к Telegram пользователей:
        - 1-е напоминание через 3 часа
        - 2-е напоминание через 6 часов
        - Удаление через 9 часов
        """
        if not config.tariff_api_url:
            logger.info("Tariff API URL not set, tariff check disabled")
            return

        logger.info("Tariff check loop started: interval %ds, url %s" % (interval, config.tariff_api_url))
        await asyncio.sleep(120)  # grace period

        while True:
            try:
                await asyncio.sleep(interval)

                # --- 1. Опрос API тарифов ---
                try:
                    session = await self._client._get_session()
                    async with session.get(config.tariff_api_url) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            # Формат: {"status": "OK", "accs": [{"token": "...", "active": true}]}
                            if not isinstance(data, dict) or data.get("status") != "OK":
                                logger.warning("Tariff API: unexpected response, skipping: %s" % str(data)[:200])
                            else:
                                accs = data.get("accs", [])
                                if not isinstance(accs, list):
                                    logger.warning("Tariff API: 'accs' is not a list, skipping")
                                else:
                                    updated = 0
                                    for item in accs:
                                        token = item.get("token", "")
                                        is_active = bool(item.get("active", True))
                                        user = await self._db.get_user_by_token(token)
                                        if user:
                                            was_active = bool(user.get("is_active", 1))
                                            if was_active != is_active:
                                                await self._db.set_user_active(user["max_user_id"], is_active)
                                                updated += 1
                                                if not is_active:
                                                    chat_id = user.get("chat_id")
                                                    if chat_id:
                                                        try:
                                                            await self._client.send_message_direct(
                                                                chat_id,
                                                                "⚠️ <b>Доступ ограничен</b>\n\n"
                                                                "Ваш тариф не активен.\n"
                                                                "Обратитесь в поддержку: "
                                                                "<a href=\"https://t.me/best_anons_tp\">@best_anons_tp</a>",
                                                            )
                                                        except Exception:
                                                            pass
                                    logger.info("Tariff check: %d users updated (total accs: %d)" % (updated, len(accs)))
                        else:
                            # Нет ответа / ошибка — логируем, НЕ блокируем
                            logger.warning("Tariff API returned HTTP %s — no action taken" % resp.status)
                except Exception as e:
                    # API недоступен — логируем, НЕ блокируем
                    logger.error("Tariff API unreachable — no action taken: %s" % e)

                # --- 2. Проверка неподключённых пользователей ---
                try:
                    unlinked = await self._db.get_unlinked_users(hours=3)
                    for u in unlinked:
                        max_uid = u["max_user_id"]
                        chat_id = u.get("chat_id")
                        reminder_count = u.get("reminder_count", 0)

                        if not chat_id:
                            continue

                        if reminder_count >= 3:
                            # Третье напоминание прошло — удаляем
                            await self._db.delete_user_full(max_uid)
                            logger.info("Unlinked user deleted: %s" % max_uid)
                            continue

                        # Отправляем напоминание (3 разных текста)
                        count = await self._db.increment_reminder(max_uid)
                        reminder_messages = {
                            1: (
                                "👋 <b>Добро пожаловать в MAX от BestAnons</b>\n\n"
                                "👉 Для подключения необходимо выполнить связку с Telegram\n\n"
                                "Перейдите в <a href=\"https://t.me/best_anons_bot\">@best_anons_bot</a>\n"
                                "Настройки → Автопостинг → MAX → ссылка\n"
                                "После этого вы сможете полноценно пользоваться ботом"
                            ),
                            2: (
                                "Пожалуйста, подключите связку с Telegram\n\n"
                                "Перейдите в <a href=\"https://t.me/best_anons_bot\">@best_anons_bot</a>\n"
                                "Настройки → Автопостинг → MAX → ссылка\n\n"
                                "<blockquote>💡 Это поможет вам упростить ведение каналов 👍</blockquote>"
                            ),
                            3: (
                                "✨ <b>Начните подключение</b>\n\n"
                                "Сделайте первый шаг — подключите связку с Telegram\n"
                                "Перейдите в <a href=\"https://t.me/best_anons_bot\">@best_anons_bot</a>\n\n"
                                "Настройки → Автопостинг → MAX → ссылка\n\n"
                                "<blockquote>💡 После подключения вы сможете:\n"
                                "— удобно работать с контентом\n"
                                "— автоматизировать публикации\n"
                                "— добавлять кнопки к анонсам и не только\n"
                                "— формировать свои посты\n"
                                "— использовать анонсы нашего бота или свои\n"
                                "— делать понятный список анонсов за день\n"
                                "— планировать отправку постов</blockquote>\n"
                                "👉 Этот бот создан, чтобы упростить ведение канала "
                                "и сэкономить ваше время\n"
                                "Попробуйте — вам точно понравится 👍"
                            ),
                        }
                        msg_text = reminder_messages.get(count, reminder_messages[3])
                        try:
                            await self._client.send_message_direct(chat_id, msg_text)
                        except Exception:
                            pass

                except Exception as e:
                    logger.error("Unlinked users check error: %s" % e)

            except asyncio.CancelledError:
                logger.info("Tariff check loop stopped")
                break
            except Exception as e:
                logger.error("Tariff check loop error: %s" % e)

    async def scheduler_loop(self, interval: int = 30) -> None:
        """
        Фоновая задача: каждые 30 секунд проверяет запланированные посты.
        Если время наступило — публикует, закрепляет, обновляет ссылки.
        """
        logger.info("Scheduler loop started: interval %ds" % interval)
        await asyncio.sleep(30)  # grace period

        tz = timezone(timedelta(hours=3))  # Moscow

        while True:
            try:
                await asyncio.sleep(interval)
                now = datetime.now(tz)
                now_iso = now.strftime("%Y-%m-%d %H:%M:%S")

                due_posts = await self._db.get_due_posts(now_iso)
                if not due_posts:
                    continue

                logger.info("Scheduler: %d posts due" % len(due_posts))

                for sp in due_posts:
                    post_id = sp["id"]
                    try:
                        channels = json.loads(sp.get("channels_json", "[]"))
                        text = sp.get("text") or ""
                        markup = json.loads(sp.get("markup_json") or "[]") or None
                        media = json.loads(sp.get("media_json") or "[]")
                        buttons_data = json.loads(sp.get("buttons_json") or "null")
                        post_type = sp.get("post_type", "post")
                        user_id = sp.get("user_id")
                        chat_id = sp.get("chat_id")
                        max_user_id = sp.get("max_user_id", "")

                        rendered_text, rendered_format, rendered_markup = self._render_post_text(text, markup)

                        ch_sent = 0
                        ch_errors = 0
                        ch_names = []

                        for channel_id in channels:
                            try:
                                # Deep copy media для каждого канала
                                attachments = [dict(m) for m in media]
                                # Кнопки
                                if buttons_data and isinstance(buttons_data, list):
                                    kb = self._build_keyboard_with_layout(buttons_data, 1)
                                    if kb:
                                        attachments.append({"type": "inline_keyboard", "payload": {"buttons": kb}})

                                # Кнопка комментариев
                                sched_comment_key = ""
                                if config.bot_link:
                                    sched_comment_key = self._generate_comment_key()
                                    text_preview = (text or "")[:200]
                                    await self._db.create_comment_link(
                                        sched_comment_key, channel_id, user_id, text_preview)
                                    self._append_comment_button_to_attachments(
                                        attachments, sched_comment_key)

                                result = await self._client.send_message(
                                    channel_id, rendered_text,
                                    attachments=attachments or None,
                                    format=rendered_format, markup=rendered_markup,
                                )

                                msg_url = result.get("message", {}).get("url")
                                msg_mid = str(result.get("message", {}).get("body", {}).get("mid", ""))

                                # Обновляем comment_link с msg_id
                                if sched_comment_key and msg_mid:
                                    await self._db.update_comment_link_msg_id(
                                        sched_comment_key, msg_mid)

                                # Закрепляем если тип pin
                                if post_type == "pin" and msg_mid:
                                    pinned = await self._client.pin_message(channel_id, msg_mid)
                                    if pinned:
                                        if msg_url:
                                            updated = await self._db.update_pin_url(user_id, channel_id, msg_url)
                                            if updated:
                                                logger.info("Scheduler: pin URL updated for %d buttons in %s" % (updated, channel_id))
                                        else:
                                            logger.warning("Scheduler: pin OK but no msg_url for %s" % channel_id)
                                    else:
                                        logger.error("Scheduler: PIN FAILED for channel %s mid=%s" % (channel_id, msg_mid))

                                title = await self._client.get_chat_title(channel_id)
                                ch_names.append(title)
                                ch_sent += 1
                                pin_status = ""
                                if post_type == "pin":
                                    pin_status = " 📌" if (msg_mid and await self._client.get_pinned_message(channel_id)) else " ⚠️pin?"
                                logger.info("Scheduler: sent %s to %s (mid=%s)%s" % (post_type, channel_id, msg_mid[:20], pin_status))

                            except Exception as ch_err:
                                ch_errors += 1
                                logger.error("Scheduler: channel %s error: %s" % (channel_id, ch_err))

                        if ch_sent > 0:
                            await self._db.mark_scheduled_sent(post_id)
                        else:
                            await self._db.mark_scheduled_error(post_id, "All channels failed")

                        # Уведомляем пользователя (НЕ через _reply чтобы не сломать текущий интерфейс)
                        if chat_id and max_user_id:
                            icon = "📌" if post_type == "pin" else "⏰"
                            try:
                                notification_text = (
                                    "%s <b>Запланированный %s опубликован</b>\n\n"
                                    "→ %s%s"
                                    % (icon,
                                       "закреп" if post_type == "pin" else "пост",
                                       ", ".join(ch_names) if ch_names else "каналы",
                                       "\n⚠️ Ошибки: %d" % ch_errors if ch_errors else "")
                                )
                                await self._client.send_message_direct(
                                    chat_id, notification_text,
                                    attachments=self._kb([
                                        [{"type": "callback", "text": "✏️ Создать пост", "payload": "menu_create_post"}],
                                        [{"type": "callback", "text": "📋 Очередь", "payload": "menu_queue"}],
                                        self._back(),
                                    ]),
                                )
                            except Exception:
                                pass

                    except Exception as e:
                        logger.error("Scheduler: failed post_id=%s: %s" % (post_id, e))
                        await self._db.mark_scheduled_error(post_id, str(e))

            except asyncio.CancelledError:
                logger.info("Scheduler loop stopped")
                break
            except Exception as e:
                logger.error("Scheduler loop error: %s" % e)

    async def _dispatch(self, update: dict) -> None:
        update_type = update.get("update_type")
        try:
            if update_type == "bot_started":
                await self._handle_bot_started(update)
            elif update_type == "message_created":
                await self._handle_message(update)
            elif update_type == "message_edited":
                await self._handle_message_edited(update)
            elif update_type == "message_callback":
                await self._handle_callback(update)
            elif update_type == "bot_added":
                await self._handle_bot_added(update)
            elif update_type == "bot_removed":
                await self._handle_bot_removed(update)
            elif update_type == "chat_title_changed":
                await self._handle_title_changed(update)
            elif update_type in {"message_removed", "user_added"}:
                logger.debug("Ignored update_type: %s" % update_type)
            else:
                logger.debug("Unknown update_type: %s" % update_type)
        except Exception as e:
            logger.opt(exception=True).error("Error handling %s: %s" % (update_type, e))

    # ─── Клавиатуры ──────────────────────────────────────────────────────

    async def _build_main_menu(self, max_user_id: str) -> list:
        buttons = [
            [{"type": "callback", "text": "📡 Настройка каналов", "payload": "menu_channels"}],
            [{"type": "callback", "text": "✏️ Создать пост", "payload": "menu_create_post"}],
            [{"type": "callback", "text": "📋 Очередь отправки", "payload": "menu_queue"}],
            [{"type": "callback", "text": "📚 Мои кнопки", "payload": "menu_saved_btns"}],
            [{"type": "callback", "text": "ℹ️ Инструкция", "payload": "menu_help"}],
        ]
        if await self._db.is_admin(max_user_id):
            buttons.append([{"type": "callback", "text": "🔑 API", "payload": "menu_api"}])
            buttons.append([{"type": "callback", "text": "👥 Администраторы", "payload": "menu_admins"}])
        buttons.append([
            {"type": "callback", "text": "🆔 Мой ID", "payload": "show_user_id"},
            {"type": "callback", "text": "🔄 Обновить бот", "payload": "restart_bot"},
        ])
        return buttons

    @staticmethod
    def _back() -> list:
        return [{"type": "callback", "text": "📁 Главное меню", "payload": "menu_main"}]

    @staticmethod
    def _kb(buttons: list) -> list:
        return [{"type": "inline_keyboard", "payload": {"buttons": buttons}}]

    @staticmethod
    def _button_scope_meta(scope: str) -> tuple[str, str]:
        mapping = {
            "announcement": ("🔖 Анонсы бота", "announcement"),
            "regular": ("🆓 Свои анонсы", "regular"),
            "manual": ("✍️ Ручной пост", "manual"),
            "default": ("🧰 Набор кнопок канала", "default"),
        }
        return mapping.get(scope, (scope, scope))

    async def _resolve_button_owner_id(self, acting_user_id: int, channel_id: str) -> int | None:
        return await self._db.resolve_button_owner_id(channel_id, acting_user_id, require_manage_buttons=True)

    async def _get_scope_buttons(self, owner_user_id: int, channel_id: str, scope: str) -> list[dict]:
        if scope == "default":
            return await self._db.get_channel_buttons(owner_user_id, channel_id)
        return await self._db.get_button_set(owner_user_id, channel_id, scope)

    # ─── Хелперы ─────────────────────────────────────────────────────────

    async def _get_chat_id(self, max_user_id: str) -> str | None:
        user = await self._db.get_user_by_max_id(max_user_id)
        if user and user.get("chat_id"):
            return user["chat_id"]
        return None

    async def _ensure_user(self, max_user_id: str, chat_id: str) -> dict:
        """Получить или создать пользователя."""
        user = await self._db.get_user_by_max_id(max_user_id)
        if not user:
            token = secrets.token_urlsafe(32)
            user = await self._db.create_user(max_user_id, chat_id, token)
        else:
            await self._db.update_chat_id(max_user_id, chat_id)
        return user

    async def _send_channel_webhook(self, user: dict) -> None:
        webhook_url = config.webhook_url
        if not webhook_url:
            return
        channels = await self._db.get_channels(user["id"])
        payload = {
            "event": "channels_updated",
            "user_token": user["token"],
            "channels": [
                {
                    "id": ch["channel_id"],
                    "title": ch["title"],
                    "bot_is_admin": bool(ch.get("bot_is_admin")),
                }
                for ch in channels
            ],
        }
        status = await self._client.send_webhook(webhook_url, payload)
        logger.info("Webhook → %s: HTTP %s" % (webhook_url, status))

    # ─── Regular кнопки: перехват постов в каналах ─────────────────────

    @staticmethod
    def _has_price_in_text(text: str) -> bool:
        """Определить наличие цены в тексте (валюты, числа с символами)."""
        if not text:
            return False
        # Паттерны: $123, 123$, 123₽, ₽123, 123 руб, 123 USD, цена: 123 и т.д.
        patterns = [
            r"[\$€£¥₽₴]\s*\d",       # $123, €50
            r"\d\s*[\$€£¥₽₴]",       # 123$, 50€
            r"\d[\s.,]*\d*\s*(?:рублей|руб\.|руб(?:\s|$)|р\.|usd|eur|rub)",  # 123 руб, 50 USD, 100 рублей
            r"(?:цена|price|стоимость|cost)\s*[:\-]?\s*\d",  # цена: 123
        ]
        for pat in patterns:
            if re.search(pat, text, re.IGNORECASE):
                return True
        return False

    async def _try_regular_button_intercept(self, channel_id: str, msg: dict) -> None:
        """
        Логика «своих анонсов» (regular-кнопки):
        Кнопки добавляются ТОЛЬКО если пост содержит:
        - изображение(я) + цена в тексте
        Если пост — только текст с ценой (без картинки), кнопки НЕ крепятся.
        """
        body = msg.get("body", {})
        mid = body.get("mid")
        if not mid:
            return

        text = str(body.get("text", "") or "")
        if not self._has_price_in_text(text):
            return

        # Проверяем наличие изображений в посте
        raw_attachments = body.get("attachments", [])
        has_image = any(
            att.get("type") == "image" for att in raw_attachments
        )
        # Также проверяем share-вложения
        if not has_image:
            for att in raw_attachments:
                if att.get("type") == "share":
                    share_atts = att.get("payload", {}).get("attachments", [])
                    has_image = any(sa.get("type") == "image" for sa in share_atts)
                    if has_image:
                        break

        if not has_image:
            # Есть цена, но нет картинки — кнопки не крепим
            return

        # Ищем владельца канала и его regular-кнопки
        owner_user_id = await self._db.get_channel_owner_id(channel_id)
        if owner_user_id is None:
            return

        regular_buttons = await self._db.get_button_set(owner_user_id, channel_id, "regular")
        if not regular_buttons:
            return

        # Получаем layout для regular-кнопок
        layout = await self._db.get_button_set_layout(owner_user_id, channel_id, "regular")

        logger.info(
            "Regular intercept: channel=%s mid=%s buttons=%d"
            % (channel_id, str(mid)[:20], len(regular_buttons))
        )

        # Извлекаем контент из оригинального сообщения
        text_content, media_tokens, markup = self._extract_content(msg)

        # Удаляем оригинальный пост
        try:
            await self._client.delete_message(str(mid))
        except Exception as e:
            logger.warning("Regular intercept: delete failed mid=%s: %s" % (str(mid)[:20], e))
            return

        # Собираем кнопки с учётом layout
        kb = self._build_keyboard_with_layout(regular_buttons, layout)
        attachments = list(media_tokens)
        if kb:
            attachments.append({"type": "inline_keyboard", "payload": {"buttons": kb}})

        # Кнопка комментариев
        intercept_comment_key = ""
        if config.bot_link:
            intercept_comment_key = self._generate_comment_key()
            text_preview = (text_content or "")[:200]
            await self._db.create_comment_link(
                intercept_comment_key, channel_id, owner_user_id, text_preview)
            self._append_comment_button_to_attachments(attachments, intercept_comment_key)

        # Рендерим текст
        rendered_text, rendered_format, rendered_markup = self._render_post_text(text_content, markup)

        try:
            result = await self._client.send_message(
                channel_id, rendered_text,
                attachments=attachments or None,
                format=rendered_format, markup=rendered_markup,
            )
            logger.info("Regular intercept: reposted to %s with %d buttons" % (channel_id, len(regular_buttons)))
            # Обновляем comment_link с msg_id
            if intercept_comment_key:
                msg_mid = str(result.get("message", {}).get("body", {}).get("mid", ""))
                if msg_mid:
                    await self._db.update_comment_link_msg_id(intercept_comment_key, msg_mid)
        except Exception as e:
            logger.error("Regular intercept: repost failed channel=%s: %s" % (channel_id, e))

    # ─── Хелпер: клавиатура с layout ────────────────────────────────────

    @staticmethod
    def _build_keyboard_with_layout(buttons: list[dict], layout: int = 1) -> list[list[dict]]:
        """
        Построить inline_keyboard с группировкой кнопок по layout.
        layout=1 → каждая кнопка в отдельной строке
        layout=2 → по 2 в ряд
        layout=3 → по 3 в ряд
        """
        layout = max(1, min(3, layout))  # Ограничиваем 1-3
        kb = []
        row = []
        for btn in buttons:
            url = str(btn.get("url", "") or "").strip()
            text = str(btn.get("text", "") or "").strip()
            if not url or not text:
                continue
            if not url.startswith("http://") and not url.startswith("https://"):
                continue  # MAX API требует http/https
            # Убираем пробелы и спецсимволы из URL
            url = url.replace(" ", "%20").replace("\n", "").replace("\r", "").replace("\t", "")
            if len(url) > 2048:
                continue  # MAX лимит
            row.append({"type": "link", "text": text[:128], "url": url})
            if len(row) >= layout:
                kb.append(row)
                row = []
        if row:
            kb.append(row)
        return kb

    # ─── Хелпер: кнопка комментариев ────────────────────────────────────

    @staticmethod
    def _generate_comment_key() -> str:
        """Генерировать уникальный ключ для привязки комментариев к посту."""
        return secrets.token_urlsafe(8)  # ~11 символов, URL-safe

    @staticmethod
    def _make_comment_button(comment_key: str) -> list[dict]:
        """
        Создать кнопку [💬 Комментарии] для поста.
        Возвращает строку клавиатуры (список из одной кнопки).
        Если BOT_LINK не настроен — возвращает пустой список.
        """
        if not config.bot_link:
            return []
        url = "%s?start=c_%s" % (config.bot_link, comment_key)
        return [{"type": "link", "text": "💬 Комментарии", "url": url}]

    def _append_comment_button_to_attachments(
        self, attachments: list, comment_key: str
    ) -> list:
        """
        Добавить кнопку комментариев в attachments поста.
        Если inline_keyboard уже есть — добавляет строку в конец.
        Если нет — создаёт новый inline_keyboard.
        """
        btn_row = self._make_comment_button(comment_key)
        if not btn_row:
            return attachments

        # Ищем существующий inline_keyboard
        for att in attachments:
            if isinstance(att, dict) and att.get("type") == "inline_keyboard":
                att["payload"]["buttons"].append(btn_row)
                return attachments

        # Нет клавиатуры — создаём новую
        attachments.append({
            "type": "inline_keyboard",
            "payload": {"buttons": [btn_row]},
        })
        return attachments

    # ─── bot_started ─────────────────────────────────────────────────────

    async def _handle_bot_started(self, update: dict) -> None:
        max_user_id = str(update["user"]["user_id"])
        chat_id = str(update["chat_id"])
        payload = str(update.get("payload", "") or "").strip()

        # ── Deeplink: комментарии (c_КЛЮЧ) ──
        if payload.startswith("c_"):
            comment_key = payload[2:]
            # Гарантируем что пользователь есть в базе (даже без /start)
            user = await self._db.get_user_by_max_id(max_user_id)
            if not user:
                token = secrets.token_urlsafe(32)
                user = await self._db.create_user(max_user_id, chat_id, token)
            else:
                await self._db.update_chat_id(max_user_id, chat_id)
            # Имя комментатора из MAX API
            commenter_name = update.get("user", {}).get("name", "")
            if not commenter_name:
                commenter_name = update.get("user", {}).get("username", "")
            await self._show_comments(max_user_id, chat_id, comment_key, commenter_name)
            return

        # ── Обычный /start — главное меню ──
        # Удаляем все прошлые сообщения бота — чат начинается с чистого листа
        deleted = await self._client.clear_bot_messages(chat_id)
        if deleted:
            logger.info("Cleared %d old bot messages for user %s" % (deleted, max_user_id))

        user = await self._db.get_user_by_max_id(max_user_id)
        if not user:
            token = secrets.token_urlsafe(32)
            user = await self._db.create_user(max_user_id, chat_id, token)
            msg = (
                "✅ <b>Добро пожаловать!</b>\n\n"
                "Здесь вы можете:\n"
                "• управлять своими каналами\n"
                "• создавать посты с кнопками\n"
                "• настраивать и редактировать кнопки"
            )
        else:
            await self._db.update_chat_id(max_user_id, chat_id)
            msg = (
                "✅ <b>Добро пожаловать!</b>\n\n"
                "Здесь вы можете:\n"
                "• управлять своими каналами\n"
                "• создавать посты с кнопками\n"
                "• настраивать и редактировать кнопки"
            )

        menu = await self._build_main_menu(max_user_id)
        # Отправляем без удаления предыдущего — мы уже всё почистили
        result = await self._client.send_message(chat_id, msg, attachments=self._kb(menu))
        mid = str(result.get("message", {}).get("body", {}).get("mid", ""))
        if mid:
            self._last_bot_msg[max_user_id] = mid

        # Отдельное сообщение только с ID — удобно копировать для привязки в Telegram
        await self._client.send_message_direct(chat_id, max_user_id)

    # ─── Комментарии ─────────────────────────────────────────────────────

    async def _show_comments(self, max_user_id: str, chat_id: str,
                              comment_key: str, commenter_name: str = "",
                              page: int = 0) -> None:
        """Показать комментарии к посту с пагинацией."""
        link = await self._db.get_comment_link(comment_key)
        if not link:
            await self._reply(
                chat_id, max_user_id,
                "❌ Пост не найден или ссылка устарела.",
                attachments=self._kb([self._back()]))
            return

        per_page = 20
        total = await self._db.count_comments(comment_key)
        comments = await self._db.get_comments(comment_key, limit=per_page,
                                                offset=page * per_page)

        # Заголовок
        channel_id = link.get("channel_id", "")
        preview = link.get("post_text_preview", "")
        if preview:
            short_preview = preview[:100] + "…" if len(preview) > 100 else preview
            header = "💬 <b>Комментарии</b> (%d)\n\n📝 <i>%s</i>\n" % (
                total, html.escape(short_preview))
        else:
            header = "💬 <b>Комментарии</b> (%d)\n" % total

        if not comments and total == 0:
            header += "\nКомментариев пока нет. Будьте первым!"
        else:
            lines = []
            for c in comments:
                name = html.escape(c.get("commenter_name") or "Пользователь")
                dt = c.get("created_at", "")[:16].replace("T", " ")
                text = html.escape(c.get("text", ""))
                # Кнопка удаления доступна только автору
                lines.append(
                    "\n<b>%s</b> <i>(%s)</i>\n%s" % (name, dt, text)
                )
            header += "\n".join(lines)

        # Кнопки навигации
        buttons = []

        # Кнопка «Написать комментарий»
        buttons.append([{
            "type": "callback",
            "text": "✏️ Написать комментарий",
            "payload": "cmt_write:%s" % comment_key,
        }])

        # Пагинация
        nav_row = []
        if page > 0:
            nav_row.append({
                "type": "callback",
                "text": "⬅️ Назад",
                "payload": "cmt_page:%s:%d" % (comment_key, page - 1),
            })
        if (page + 1) * per_page < total:
            nav_row.append({
                "type": "callback",
                "text": "➡️ Далее",
                "payload": "cmt_page:%s:%d" % (comment_key, page + 1),
            })
        if nav_row:
            buttons.append(nav_row)

        # Удаление своих комментариев (показываем только если есть свои)
        own_comments = [c for c in comments if str(c.get("commenter_max_user_id")) == max_user_id]
        if own_comments:
            buttons.append([{
                "type": "callback",
                "text": "🗑 Удалить мой комментарий",
                "payload": "cmt_delmenu:%s" % comment_key,
            }])

        # Обновить
        buttons.append([{
            "type": "callback",
            "text": "🔄 Обновить",
            "payload": "cmt_page:%s:%d" % (comment_key, page),
        }])

        buttons.append(self._back())

        # Сохраняем контекст для commenter_name
        self._set_pending(max_user_id, {
            "action": "in_comments",
            "comment_key": comment_key,
            "commenter_name": commenter_name,
        })

        await self._reply(chat_id, max_user_id, header,
                           attachments=self._kb(buttons))

    async def _start_write_comment(self, max_user_id: str, chat_id: str,
                                    comment_key: str) -> None:
        """Перевести пользователя в режим ввода комментария."""
        state = self._pending_states.get(max_user_id, {})
        commenter_name = state.get("commenter_name", "")

        self._set_pending(max_user_id, {
            "action": "awaiting_comment",
            "comment_key": comment_key,
            "commenter_name": commenter_name,
        })

        buttons = [
            [{"type": "callback", "text": "↩️ Отмена",
              "payload": "cmt_page:%s:0" % comment_key}],
        ]

        await self._reply(
            chat_id, max_user_id,
            "✏️ <b>Напишите ваш комментарий:</b>\n\n"
            "Отправьте текст сообщения.",
            attachments=self._kb(buttons))

    async def _handle_comment_text(self, max_user_id: str, chat_id: str,
                                    text: str) -> None:
        """Обработать введённый текст комментария."""
        state = self._pending_states.get(max_user_id, {})
        comment_key = state.get("comment_key", "")
        commenter_name = state.get("commenter_name", "")

        if not comment_key:
            del self._pending_states[max_user_id]
            await self._show_main_menu(max_user_id, chat_id)
            return

        # Проверяем что comment_link существует
        link = await self._db.get_comment_link(comment_key)
        if not link:
            del self._pending_states[max_user_id]
            await self._reply(
                chat_id, max_user_id,
                "❌ Пост не найден.",
                attachments=self._kb([self._back()]))
            return

        # Ограничение длины
        if len(text) > 2000:
            await self._reply(
                chat_id, max_user_id,
                "❌ Комментарий слишком длинный (макс. 2000 символов). Попробуйте короче:",
                attachments=self._kb([
                    [{"type": "callback", "text": "↩️ Отмена",
                      "payload": "cmt_page:%s:0" % comment_key}],
                ]))
            return

        # Если имя не получено ранее — пробуем получить
        if not commenter_name:
            commenter_name = "Пользователь %s" % max_user_id[:8]

        await self._db.add_comment(comment_key, max_user_id, commenter_name, text)
        logger.info("Comment added: key=%s user=%s name=%s" % (
            comment_key, max_user_id, commenter_name))

        # Показываем обновлённые комментарии
        await self._show_comments(max_user_id, chat_id, comment_key, commenter_name)

    async def _show_delete_own_comments(self, max_user_id: str, chat_id: str,
                                         comment_key: str) -> None:
        """Показать список своих комментариев для удаления."""
        comments = await self._db.get_comments(comment_key, limit=100)
        own = [c for c in comments if str(c.get("commenter_max_user_id")) == max_user_id]

        if not own:
            await self._reply(
                chat_id, max_user_id,
                "У вас нет комментариев к этому посту.",
                attachments=self._kb([
                    [{"type": "callback", "text": "💬 К комментариям",
                      "payload": "cmt_page:%s:0" % comment_key}],
                    self._back(),
                ]))
            return

        lines = ["🗑 <b>Выберите комментарий для удаления:</b>\n"]
        buttons = []
        for c in own:
            short_text = c["text"][:60] + "…" if len(c["text"]) > 60 else c["text"]
            dt = c.get("created_at", "")[:16].replace("T", " ")
            lines.append("• <i>%s</i> (%s)" % (html.escape(short_text), dt))
            buttons.append([{
                "type": "callback",
                "text": "❌ %s" % (short_text[:40]),
                "payload": "cmt_del:%s:%d" % (comment_key, c["id"]),
            }])

        buttons.append([{
            "type": "callback", "text": "💬 К комментариям",
            "payload": "cmt_page:%s:0" % comment_key,
        }])
        buttons.append(self._back())

        await self._reply(chat_id, max_user_id, "\n".join(lines),
                           attachments=self._kb(buttons))

    async def _delete_own_comment(self, max_user_id: str, chat_id: str,
                                   comment_key: str, comment_id: int) -> None:
        """Удалить свой комментарий."""
        deleted = await self._db.delete_comment(comment_id, max_user_id)
        if deleted:
            logger.info("Comment %d deleted by %s" % (comment_id, max_user_id))
        state = self._pending_states.get(max_user_id, {})
        commenter_name = state.get("commenter_name", "")
        await self._show_comments(max_user_id, chat_id, comment_key, commenter_name)

    # ─── message_created ─────────────────────────────────────────────────

    async def _handle_message(self, update: dict) -> None:
        msg = update["message"]
        chat_id = str(msg.get("recipient", {}).get("chat_id", ""))

        # Сообщения из каналов могут не иметь sender (системные, от ботов).
        sender = msg.get("sender")
        if not sender:
            # Канальное сообщение без отправителя — проверяем regular-кнопки
            if chat_id.startswith("-"):
                await self._try_regular_button_intercept(chat_id, msg)
            return

        max_user_id = str(sender.get("user_id", ""))
        if not max_user_id:
            return

        # Игнорируем собственные сообщения бота (в любых чатах)
        if sender.get("is_bot"):
            return

        # Сообщения из каналов/групп — перехват обычных кнопок
        if chat_id.startswith("-"):
            await self._try_regular_button_intercept(chat_id, msg)
            return

        # Сбрасываем индикатор «бот печатает» (часы)
        await self._client.send_action(chat_id, "mark_seen")

        user = await self._ensure_user(max_user_id, chat_id)

        # Комментарии доступны всем — обрабатываем ДО тарифной проверки
        state_pre = self._pending_states.get(max_user_id)
        if state_pre and state_pre.get("action") == "awaiting_comment":
            comment_text = msg.get("body", {}).get("text", "").strip()
            if comment_text:
                await self._handle_comment_text(max_user_id, chat_id, comment_text)
            else:
                await self._reply(
                    chat_id, max_user_id,
                    "❌ Отправьте текстовое сообщение.",
                    attachments=self._kb([
                        [{"type": "callback", "text": "↩️ Отмена",
                          "payload": "cmt_page:%s:0" % state_pre.get("comment_key", "")}],
                    ]))
            return

        # Проверка тарифной блокировки
        if not await self._db.is_user_active(max_user_id):
            await self._reply(
                chat_id, max_user_id,
                "⚠️ <b>Доступ ограничен</b>\n\n"
                "Ваш тариф не активен.\n"
                "Обратитесь в поддержку: "
                "<a href=\"https://t.me/best_anons_tp\">@best_anons_tp</a>",
            )
            return

        # Pending states
        state = self._pending_states.get(max_user_id)
        if state:
            action = state.get("action")
            if action == "awaiting_admin_id":
                await self._admin_mgr.handle_awaiting_admin_id(max_user_id, chat_id,
                                                                msg.get("body", {}).get("text", "").strip())
                return
            elif action == "batch_collecting":
                await self._handle_batch_content(max_user_id, chat_id, msg)
                return
            elif action == "awaiting_helper_alias":
                text = msg.get("body", {}).get("text", "").strip()
                await self._handle_helper_alias(max_user_id, chat_id, text)
                return
            elif action == "awaiting_channel_button_text":
                btn_text = msg.get("body", {}).get("text", "").strip()
                await self._handle_channel_button_text(max_user_id, chat_id, btn_text)
                return
            elif action == "awaiting_channel_button_url":
                btn_url = msg.get("body", {}).get("text", "").strip()
                await self._handle_channel_button_url(max_user_id, chat_id, btn_url)
                return
            elif action == "awaiting_saved_btn_text":
                btn_text = msg.get("body", {}).get("text", "").strip()
                await self._handle_saved_btn_text(max_user_id, chat_id, btn_text)
                return
            elif action == "awaiting_saved_btn_url":
                btn_url = msg.get("body", {}).get("text", "").strip()
                await self._handle_saved_btn_url(max_user_id, chat_id, btn_url)
                return
            elif action == "awaiting_saved_btn_edit_text":
                new_text = msg.get("body", {}).get("text", "").strip()
                await self._handle_saved_btn_edit_text(max_user_id, chat_id, new_text)
                return
            elif action == "awaiting_saved_btn_edit_url":
                new_url = msg.get("body", {}).get("text", "").strip()
                await self._handle_saved_btn_edit_url(max_user_id, chat_id, new_url)
                return
            elif action == "awaiting_schedule_date":
                text_input = msg.get("body", {}).get("text", "").strip()
                await self._handle_schedule_date_input(max_user_id, chat_id, text_input)
                return
            elif action == "awaiting_schedule_time":
                text_input = msg.get("body", {}).get("text", "").strip()
                await self._handle_schedule_time_input(max_user_id, chat_id, text_input)
                return

        # Извлечь контент
        text, media_tokens, markup = self._extract_content(msg)

        # Если пришёл контент (медиа или пересланный пост) без активного состояния —
        # автоматически начинаем пакетный постинг и сразу добавляем первый пост,
        # чтобы не потерять пересланный контент.
        if media_tokens or (text and msg.get("link")):
            user_for_batch = await self._db.get_user_by_max_id(max_user_id)
            if user_for_batch:
                channels = await self._db.get_accessible_channels(user_for_batch["id"])
                if channels and (text or media_tokens):
                    self._set_pending(max_user_id, {
                        "action": "batch_collecting",
                        "posts": [{
                            "text": text,
                            "media_tokens": media_tokens,
                            "markup": markup,
                            "source_mid": str(msg.get("body", {}).get("mid", "")),
                        }],
                    })
                    count = 1
                    preview_parts = []
                    if text:
                        short = text[:80] + "..." if len(text) > 80 else text
                        preview_parts.append(short)
                    if media_tokens:
                        img = sum(1 for m in media_tokens if m["type"] == "image")
                        vid = sum(1 for m in media_tokens if m["type"] == "video")
                        parts = []
                        if img:
                            parts.append("%d фото" % img)
                        if vid:
                            parts.append("%d видео" % vid)
                        preview_parts.append("📎 " + ", ".join(parts))
                    preview = "\n".join(preview_parts) or "(контент)"
                    await self._reply(
                        chat_id, max_user_id,
                        "✅ <b>Пост #1 добавлен</b>\n\n"
                        "%s\n\n"
                        "Отправьте ещё контент или нажмите <b>✅ Готово</b>" % preview,
                        attachments=self._kb([
                            [{"type": "callback", "text": "✅ Готово (1 пост)", "payload": "batch_done"}],
                            [{"type": "callback", "text": "➕ Ещё пост", "payload": "batch_add_more"}],
                            self._back(),
                        ]))
                    return
            # Нет каналов или пользователя — показать подсказку
            await self._reply(
                chat_id, max_user_id,
                "📸 Вижу контент! Нажмите <b>[✏️ Создать пост]</b>, затем отправьте его.",
                attachments=self._kb([
                    [{"type": "callback", "text": "✏️ Создать пост", "payload": "menu_create_post"}],
                    self._back(),
                ]))
            return

        # Любой текст → главное меню
        await self._show_main_menu(max_user_id, chat_id)

    # ─── Callback ────────────────────────────────────────────────────────

    async def _handle_callback(self, update: dict) -> None:
        cb = update["callback"]
        callback_id = cb["callback_id"]
        max_user_id = str(cb["user"]["user_id"])
        payload = cb.get("payload", "")

        chat_id = await self._get_chat_id(max_user_id)
        if not chat_id:
            await self._client.answer_callback(callback_id, notification="Нажмите /start")
            return

        # Проверка тарифной блокировки (комментарии доступны всем)
        if not payload.startswith("cmt_") and not await self._db.is_user_active(max_user_id):
            await self._client.answer_callback(callback_id, notification="Доступ ограничен — тариф не активен")
            return

        await self._client.answer_callback(callback_id)

        # Сброс pending при навигации в меню
        nav_prefixes = ("menu_", "back_")
        if any(payload.startswith(p) for p in nav_prefixes):
            if max_user_id in self._pending_states:
                action = self._pending_states[max_user_id].get("action", "")
                # Не сбрасываем если в процессе batch и нажали не главное меню
                if payload == "menu_main" or not action.startswith("batch"):
                    del self._pending_states[max_user_id]

        # ── Навигация ──
        if payload == "menu_main":
            if max_user_id in self._pending_states:
                del self._pending_states[max_user_id]
            await self._show_main_menu(max_user_id, chat_id)
        elif payload == "restart_bot":
            # Поведение как /start — очистка чата + главное меню
            if max_user_id in self._pending_states:
                del self._pending_states[max_user_id]
            deleted = await self._client.clear_bot_messages(chat_id)
            if deleted:
                logger.info("Restart: cleared %d messages for %s" % (deleted, max_user_id))
            await self._db.update_chat_id(max_user_id, chat_id)
            menu = await self._build_main_menu(max_user_id)
            result = await self._client.send_message(
                chat_id, "🔄 <b>Бот обновлён!</b>\n\n👇 Выберите действие:",
                attachments=self._kb(menu))
            mid = str(result.get("message", {}).get("body", {}).get("mid", ""))
            if mid:
                self._last_bot_msg[max_user_id] = mid
        elif payload == "show_user_id":
            await self._show_user_id(max_user_id, chat_id)
        elif payload == "menu_channels":
            await self._show_channels(max_user_id, chat_id)
        elif payload == "menu_create_post":
            await self._start_batch_post(max_user_id, chat_id)
        elif payload == "menu_help":
            await self._show_help(max_user_id, chat_id)
        elif payload == "menu_api":
            if await self._db.is_admin(max_user_id):
                await self._show_api_info(max_user_id, chat_id)
        elif payload == "menu_admins":
            if await self._db.is_admin(max_user_id):
                await self._admin_mgr.cmd_admins(max_user_id, chat_id)

        # ── Токен ──
        elif payload == "reset_token":
            await self._cb_reset_token(max_user_id, chat_id)
        elif payload == "confirm_reset_token":
            await self._cb_confirm_reset_token(max_user_id, chat_id)
        elif payload == "cancel_reset_token":
            await self._show_api_info(max_user_id, chat_id)

        # ── Каналы ──
        elif payload.startswith("ch_info:"):
            channel_id = payload.split(":", 1)[1]
            await self._show_channel_info(max_user_id, chat_id, channel_id)
        elif payload == "back_channels":
            await self._show_channels(max_user_id, chat_id)

        # ── Пакетный постинг ──
        elif payload == "batch_add_more":
            await self._batch_add_more(max_user_id, chat_id)
        elif payload == "batch_done":
            await self._batch_select_channels(max_user_id, chat_id)
        elif payload.startswith("batch_toggle:"):
            channel_id = payload.split(":", 1)[1]
            await self._batch_toggle_channel(max_user_id, chat_id, channel_id)
        elif payload == "batch_all":
            await self._batch_select_all(max_user_id, chat_id)
        elif payload == "batch_send":
            await self._batch_publish(max_user_id, chat_id)
        elif payload.startswith("batch_schedule:"):
            schedule_type = payload.split(":", 1)[1]  # post or pin
            await self._batch_schedule_ask_date(max_user_id, chat_id, schedule_type)
        elif payload == "sched_confirm":
            await self._batch_schedule_confirm(max_user_id, chat_id)
        elif payload == "sched_change_datetime":
            state = self._pending_states.get(max_user_id)
            if state:
                await self._batch_schedule_ask_date(max_user_id, chat_id, state.get("schedule_type", "post"))
        elif payload == "menu_queue":
            await self._show_queue(max_user_id, chat_id)
        elif payload.startswith("sched_detail:"):
            await self._show_sched_detail(max_user_id, chat_id, int(payload.split(":", 1)[1]))
        elif payload.startswith("sched_cancel:"):
            await self._cancel_scheduled(max_user_id, chat_id, int(payload.split(":", 1)[1]))

        # ── Помощники ──
        elif payload.startswith("ch_helpers:"):
            channel_id = payload.split(":", 1)[1]
            await self._show_helpers(max_user_id, chat_id, channel_id)
        elif payload.startswith("add_helper:"):
            channel_id = payload.split(":", 1)[1]
            await self._add_helper_start(max_user_id, chat_id, channel_id)
        elif payload.startswith("sel_helper:"):
            # sel_helper:CH_ID:HELPER_MAX_ID
            parts = payload.split(":", 2)
            if len(parts) == 3:
                await self._select_helper_perms(max_user_id, chat_id, parts[1], parts[2])
        elif payload.startswith("helper_perm:"):
            # helper_perm:CH_ID:HELPER_MAX_ID:post:buttons
            parts = payload.split(":", 4)
            if len(parts) == 5:
                await self._confirm_add_helper(max_user_id, chat_id,
                                               parts[1], parts[2],
                                               parts[3] == "1", parts[4] == "1")
        elif payload.startswith("rm_helper:"):
            # rm_helper:CH_ID:HELPER_USER_ID
            parts = payload.split(":", 2)
            if len(parts) == 3:
                await self._remove_helper(max_user_id, chat_id, parts[1], int(parts[2]))
        elif payload.startswith("alias_helper:"):
            # alias_helper:CH_ID:HELPER_USER_ID
            parts = payload.split(":", 2)
            if len(parts) == 3:
                await self._start_helper_alias(max_user_id, chat_id, parts[1], int(parts[2]))

        # ── Кнопки канала ──
        elif payload.startswith("ch_buttons:"):
            channel_id = payload.split(":", 1)[1]
            await self._show_button_sets(max_user_id, chat_id, channel_id)
        elif payload.startswith("ch_btn_scope:"):
            parts = payload.split(":", 2)
            if len(parts) == 3:
                await self._show_button_group(max_user_id, chat_id, parts[1], parts[2])
        elif payload.startswith("ch_btn_add:"):
            parts = payload.split(":", 2)
            if len(parts) == 3:
                await self._start_add_channel_button(max_user_id, chat_id, parts[1], parts[2])
        elif payload.startswith("ch_btn_del_menu:"):
            parts = payload.split(":", 2)
            if len(parts) == 3:
                await self._show_delete_channel_buttons(max_user_id, chat_id, parts[1], parts[2])
        elif payload.startswith("ch_btn_del:"):
            parts = payload.split(":", 3)
            if len(parts) == 4:
                await self._delete_channel_button(max_user_id, chat_id, parts[1], parts[2], int(parts[3]))
        elif payload.startswith("ch_btn_clear:"):
            parts = payload.split(":", 2)
            if len(parts) == 3:
                await self._clear_button_group(max_user_id, chat_id, parts[1], parts[2])
        elif payload.startswith("ch_btn_layout:"):
            parts = payload.split(":", 2)
            if len(parts) == 3:
                await self._show_layout_picker(max_user_id, chat_id, parts[1], parts[2])
        elif payload.startswith("ch_btn_layout_set:"):
            parts = payload.split(":", 3)
            if len(parts) == 4:
                await self._set_button_layout(max_user_id, chat_id, parts[1], parts[2], int(parts[3]))
        elif payload.startswith("ch_btn_lib:"):
            parts = payload.split(":", 2)
            if len(parts) == 3:
                await self._show_saved_buttons_for_scope(max_user_id, chat_id, parts[1], parts[2])
        elif payload.startswith("ch_btn_pick:"):
            parts = payload.split(":", 3)
            if len(parts) == 4:
                await self._pick_saved_button(max_user_id, chat_id, parts[1], parts[2], int(parts[3]))

        # ── Библиотека кнопок ──
        elif payload == "menu_saved_btns":
            await self._show_saved_buttons(max_user_id, chat_id)
        elif payload == "saved_btn_create":
            await self._start_create_saved_button(max_user_id, chat_id)
        elif payload.startswith("saved_btn_view:"):
            btn_id = int(payload.split(":", 1)[1])
            await self._show_saved_button_detail(max_user_id, chat_id, btn_id)
        elif payload.startswith("saved_btn_edit_text:"):
            btn_id = int(payload.split(":", 1)[1])
            await self._start_edit_saved_btn_text(max_user_id, chat_id, btn_id)
        elif payload.startswith("saved_btn_edit_url:"):
            btn_id = int(payload.split(":", 1)[1])
            await self._start_edit_saved_btn_url(max_user_id, chat_id, btn_id)
        elif payload.startswith("saved_btn_del:"):
            btn_id = int(payload.split(":", 1)[1])
            await self._delete_saved_button_action(max_user_id, chat_id, btn_id)

        # ── Тест кнопок ──
        elif payload.startswith("ch_btn_test:"):
            parts = payload.split(":", 2)
            if len(parts) == 3:
                await self._show_test_buttons(max_user_id, chat_id, parts[1], parts[2])

        # ── Toggle кнопки из библиотеки ──
        elif payload.startswith("ch_btn_toggle:"):
            parts = payload.split(":", 3)
            if len(parts) == 4:
                await self._toggle_saved_button(max_user_id, chat_id, parts[1], parts[2], int(parts[3]))

        # ── Изменить порядок кнопок ──
        elif payload.startswith("ch_btn_reorder:"):
            parts = payload.split(":", 2)
            if len(parts) == 3:
                await self._show_reorder_buttons(max_user_id, chat_id, parts[1], parts[2])
        elif payload.startswith("ch_btn_swap:"):
            # ch_btn_swap:CH_ID:SCOPE:FROM:TO
            parts = payload.split(":", 4)
            if len(parts) == 5:
                await self._swap_buttons(max_user_id, chat_id, parts[1], parts[2], int(parts[3]), int(parts[4]))

        # ── Подтверждение удаления всех кнопок ──
        elif payload.startswith("ch_btn_clear_yes:"):
            parts = payload.split(":", 2)
            if len(parts) == 3:
                await self._clear_button_group_confirmed(max_user_id, chat_id, parts[1], parts[2])

        # ── Комментарии ──
        elif payload.startswith("cmt_write:"):
            comment_key = payload.split(":", 1)[1]
            await self._start_write_comment(max_user_id, chat_id, comment_key)
        elif payload.startswith("cmt_page:"):
            parts = payload.split(":", 2)
            if len(parts) == 3:
                comment_key = parts[1]
                page = int(parts[2]) if parts[2].isdigit() else 0
                state = self._pending_states.get(max_user_id, {})
                commenter_name = state.get("commenter_name", "")
                await self._show_comments(max_user_id, chat_id, comment_key,
                                           commenter_name, page)
        elif payload.startswith("cmt_delmenu:"):
            comment_key = payload.split(":", 1)[1]
            await self._show_delete_own_comments(max_user_id, chat_id, comment_key)
        elif payload.startswith("cmt_del:"):
            parts = payload.split(":", 2)
            if len(parts) == 3:
                comment_key = parts[1]
                comment_id = int(parts[2]) if parts[2].isdigit() else 0
                await self._delete_own_comment(max_user_id, chat_id, comment_key, comment_id)

        # ── Админы ──
        elif payload.startswith("del_admin:"):
            target_id = payload.split(":", 1)[1]
            await self._admin_mgr.cb_delete_admin(max_user_id, target_id, chat_id)
        elif payload == "add_admin":
            await self._admin_mgr.cb_add_admin_start(max_user_id, chat_id)

    # ─── bot_added — с определением прав ─────────────────────────────────

    async def _handle_bot_added(self, update: dict) -> None:
        channel_id = str(update["chat_id"])
        max_user_id = str(update["user"]["user_id"])
        title = await self._client.get_chat_title(channel_id)

        user = await self._db.get_user_by_max_id(max_user_id)
        if not user:
            logger.info("bot_added: user %s not registered" % max_user_id)
            return

        # Проверяем права бота в канале
        is_admin = await self._client.check_bot_is_admin(channel_id)

        await self._db.add_channel(user["id"], channel_id, title, bot_is_admin=is_admin)
        await self._send_channel_webhook(user)

        personal_chat_id = user.get("chat_id")
        if not personal_chat_id:
            return

        if is_admin:
            msg = (
                "✅ <b>Канал «%s» подключён!</b>\n\n"
                "👌 Бот получил нужные права.\n"
                "Постинг в канал подключён ✅\n\n"
                "Вы можете добавить кнопки к постам 👇"
            ) % title
            buttons = [
                [{"type": "callback", "text": "➕ Кнопки", "payload": "ch_buttons:%s" % channel_id}],
                [{"type": "callback", "text": "📡 Настройка каналов", "payload": "menu_channels"}],
                self._back(),
            ]
        else:
            msg = (
                "📺 <b>Канал «%s» добавлен</b>\n\n"
                "⚠️ Назначьте бота <b>администратором</b>\n"
                "и включите права публикации, чтобы подключить постинг."
            ) % title
            buttons = [
                [{"type": "callback", "text": "📡 Настройка каналов", "payload": "menu_channels"}],
                self._back(),
            ]

        await self._reply(personal_chat_id, max_user_id, msg,
                          attachments=self._kb(buttons), keep_previous=True)

    # ─── bot_removed — с именем канала ───────────────────────────────────

    async def _handle_bot_removed(self, update: dict) -> None:
        channel_id = str(update["chat_id"])

        # Получаем название ДО деактивации
        affected_users = await self._db.get_users_by_channel(channel_id)
        # Пробуем получить title из БД
        title = "Неизвестный канал"
        for u in affected_users:
            channels = await self._db.get_channels(u["id"])
            for ch in channels:
                if ch["channel_id"] == channel_id:
                    title = ch.get("title", title)
                    break
            break

        await self._db.deactivate_channel(channel_id)
        logger.info("Bot removed from channel %s (%s)" % (channel_id, title))

        for user in affected_users:
            await self._send_channel_webhook(user)
            cid = user.get("chat_id")
            if cid:
                max_uid = user.get("max_user_id", "")
                await self._reply(
                    cid, max_uid,
                    "⚠️ <b>Вы отключили канал «%s»</b>\n\n"
                    "Бот удалён из канала или потерял права.\n"
                    "Чтобы восстановить — добавьте бота обратно." % title,
                    attachments=self._kb([
                        [{"type": "callback", "text": "📡 Настройка каналов", "payload": "menu_channels"}],
                        self._back(),
                    ]),
                    keep_previous=True,
                )

    # ─── Смена заголовка канала ───────────────────────────────────────────

    async def _handle_title_changed(self, update: dict) -> None:
        """Обновить название канала в БД при смене."""
        chat_id = str(update.get("chat_id", ""))
        new_title = update.get("title", "")
        if not chat_id or not new_title:
            return
        users = await self._db.get_users_by_channel(chat_id)
        for user in users:
            await self._db.add_channel(user["id"], chat_id, new_title)
        logger.info("Channel %s renamed to: %s" % (chat_id, new_title))

    # ─── Экраны ──────────────────────────────────────────────────────────

    @staticmethod
    def _utf16_to_py(text: str, utf16_offset: int) -> int:
        """Конвертировать UTF-16 позицию в Python string index."""
        u16 = 0
        for i, ch in enumerate(text):
            if u16 >= utf16_offset:
                return i
            u16 += 2 if ord(ch) > 0xFFFF else 1
        return len(text)

    @staticmethod
    def _utf16_len(text: str, start: int, length: int) -> int:
        """Конвертировать UTF-16 length в Python length начиная с позиции start."""
        u16 = 0
        count = 0
        for ch in text[start:]:
            if u16 >= length:
                break
            u16 += 2 if ord(ch) > 0xFFFF else 1
            count += 1
        return count


    @classmethod
    def _normalize_markup(cls, text: str, markup: list[dict] | None) -> list[dict]:
        if not text or not markup:
            return []

        supported_types = {
            "strong", "emphasized", "emphasis", "italic", "underline",
            "strikethrough", "monospace", "code", "link",
            "quote", "blockquote", "quoted_text",   # цитаты — все возможные варианты
            "heading",                               # заголовок — на случай если MAX присылает
        }
        text_len = len(text)
        cleaned: list[dict] = []
        for item in markup:
            if not isinstance(item, dict):
                continue
            mtype = str(item.get("type", "") or "").strip()
            if mtype not in supported_types:
                if mtype:
                    logger.warning(
                        "MARKUP_DROPPED: unsupported type=%s from=%s length=%s text_preview=%s"
                        % (mtype, item.get("from"), item.get("length"),
                           repr(text[int(item.get("from", 0)):int(item.get("from", 0)) + 50]) if text else "")
                    )
                continue
            try:
                raw_start = int(item.get("from", -1))
                raw_length = int(item.get("length", 0))
            except Exception:
                continue
            if raw_start < 0 or raw_length <= 0:
                continue
            start = cls._utf16_to_py(text, raw_start)
            length = cls._utf16_len(text, start, raw_length)
            if start >= text_len or length <= 0:
                continue
            end = min(text_len, start + length)
            if end <= start:
                continue
            normalized = {"from": start, "length": end - start, "type": mtype}
            if mtype == "link":
                url = str(item.get("url", "") or "").strip()
                if not url:
                    continue
                normalized["url"] = url
            cleaned.append(normalized)

        cleaned.sort(key=lambda x: (x["from"], -(x["length"]), x.get("type", "")))
        deduped: list[dict] = []
        seen = set()
        for item in cleaned:
            key = (item["from"], item["length"], item.get("type"), item.get("url"))
            if key in seen:
                continue
            seen.add(key)
            deduped.append(item)

        safe: list[dict] = []
        stack: list[tuple[int, int]] = []
        for item in deduped:
            start = item["from"]
            end = start + item["length"]
            while stack and start >= stack[-1][1]:
                stack.pop()
            if stack and end > stack[-1][1]:
                continue
            safe.append(item)
            stack.append((start, end))
        return safe

    @staticmethod
    def _html_tags_for_markup(item: dict) -> tuple[str, str] | None:
        mtype = item.get("type")
        if mtype == "strong":
            return ("<b>", "</b>")
        if mtype in {"emphasized", "emphasis", "italic"}:
            return ("<i>", "</i>")
        if mtype == "underline":
            return ("<u>", "</u>")
        if mtype == "strikethrough":
            return ("<s>", "</s>")
        if mtype == "quote":
            return ("<blockquote>", "</blockquote>")
        if mtype in {"blockquote", "quoted_text"}:
            return ("<blockquote>", "</blockquote>")
        if mtype == "heading":
            return ("<b>", "</b>")
        if mtype in {"monospace", "code"}:
            return ("<code>", "</code>")
        if mtype == "link":
            url = html.escape(str(item.get("url", "") or ""), quote=True)
            if not url:
                return None
            return (f'<a href="{url}">', "</a>")
        return None

    @classmethod
    def _markup_to_html(cls, text: str, markup: list[dict] | None) -> str:
        safe_text = text or ""
        normalized = cls._normalize_markup(safe_text, markup)
        if not normalized:
            return html.escape(safe_text or " ")

        opens: dict[int, list[tuple[int, int, str, str]]] = {}
        closes: dict[int, list[tuple[int, int, str, str]]] = {}
        for item in normalized:
            tags = cls._html_tags_for_markup(item)
            if not tags:
                continue
            start = item["from"]
            end = start + item["length"]
            open_tag, close_tag = tags
            entry = (start, end, open_tag, close_tag)
            opens.setdefault(start, []).append(entry)
            closes.setdefault(end, []).append(entry)

        parts: list[str] = []
        for idx in range(len(safe_text) + 1):
            if idx in closes:
                for _, _, _, close_tag in sorted(closes[idx], key=lambda x: (x[0], x[1] - x[0]))[::-1]:
                    parts.append(close_tag)
            if idx in opens:
                for _, _, open_tag, _ in sorted(opens[idx], key=lambda x: (-(x[1] - x[0]), x[0])):
                    parts.append(open_tag)
            if idx < len(safe_text):
                parts.append(html.escape(safe_text[idx]))

        rendered = "".join(parts)
        return rendered or " "

    @staticmethod
    def _format_post_number(text: str, is_html: bool) -> tuple[str, bool]:
        """
        Проверить последние 15 символов на наличие 6-значного номера.
        Поддерживает: №111111, 111111 (просто 6 цифр в конце).
        Если найден — обернуть в <code><u>...</u></code> (машинописный + подчёркнутый).
        """
        tail = text[-15:] if len(text) > 15 else text
        match = re.search(r"(№?\d{6})\s*$", tail)
        if not match:
            return text, is_html
        number = match.group(1)
        if is_html:
            # Машинописный + подчёркнутый (подчёркивание снаружи для совместимости)
            formatted = "<u><code>%s</code></u>" % number
            # Заменяем последнее вхождение
            idx = text.rfind(number)
            if idx >= 0:
                text = text[:idx] + formatted + text[idx + len(number):]
        else:
            # Простой текст → делаем HTML
            escaped = html.escape(text)
            formatted = "<u><code>%s</code></u>" % number
            idx = escaped.rfind(number)
            if idx >= 0:
                text = escaped[:idx] + formatted + escaped[idx + len(number):]
            is_html = True
        return text, is_html

    @classmethod
    def _render_post_text(cls, text: str, markup: list[dict] | None) -> tuple[str, str | None, list[dict] | None]:
        safe_text = text or ""
        if not safe_text and not markup:
            return " ", None, None
        normalized_markup = cls._normalize_markup(safe_text, markup)
        if normalized_markup:
            rendered = cls._markup_to_html(safe_text, markup)
            rendered, _ = cls._format_post_number(rendered, True)
            return rendered, "html", None
        # Нет markup — проверяем номер поста
        rendered, is_html = cls._format_post_number(safe_text, False)
        if is_html:
            return rendered, "html", None
        return safe_text or " ", None, None

    async def _show_user_id(self, max_user_id: str, chat_id: str) -> None:
        user = await self._db.get_user_by_max_id(max_user_id)
        if not user:
            await self._reply(
                chat_id, max_user_id,
                "❌ <b>Токен не найден</b>\n\nПопробуйте перезапустить бота.",
                attachments=self._kb([self._back()]),
            )
            return

        token = str(user.get("token", "")).strip()
        if not token:
            await self._reply(
                chat_id, max_user_id,
                "❌ <b>Токен не найден</b>\n\nПопробуйте перезапустить бота.",
                attachments=self._kb([self._back()]),
            )
            return

        await self._reply(
            chat_id, max_user_id,
            "<code>%s</code>" % html.escape(token),
            attachments=self._kb([self._back()]),
        )

    async def _show_main_menu(self, max_user_id: str, chat_id: str) -> None:
        menu = await self._build_main_menu(max_user_id)
        await self._reply(
            chat_id, max_user_id,
            "📁 <b>Главное меню</b>\n\n"
            "Здесь вы можете:\n"
            "• управлять своими каналами\n"
            "• создавать посты с кнопками\n"
            "• настраивать и редактировать кнопки",
            attachments=self._kb(menu))

    async def _show_channels(self, max_user_id: str, chat_id: str) -> None:
        user = await self._db.get_user_by_max_id(max_user_id)
        if not user:
            return

        channels = await self._db.get_accessible_channels(user["id"])
        if not channels:
            await self._reply(
                chat_id, max_user_id,
                "📭 <b>Нет подключённых каналов</b>\n\n"
                "Чтобы подключить:\n"
                "1. Добавьте бота как подписчика канала\n"
                "2. Назначьте <b>администратором</b> с правом публикации\n"
                "3. Бот зафиксирует канал автоматически",
                attachments=self._kb([self._back()]))
            return

        # Перепроверяем права бота в каждом канале и обновляем БД
        for ch in channels:
            is_admin = await self._client.check_bot_is_admin(ch["channel_id"])
            if bool(is_admin) != bool(ch.get("bot_is_admin")):
                await self._db.update_bot_admin_status(ch["channel_id"], is_admin)
                ch["bot_is_admin"] = 1 if is_admin else 0

        buttons = []
        for ch in channels:
            status = "✅" if ch.get("bot_is_admin") else "⚠️"
            buttons.append([{
                "type": "callback",
                "text": "%s %s" % (status, ch["title"]),
                "payload": "ch_info:%s" % ch["channel_id"],
            }])
        buttons.append(self._back())

        await self._reply(
            chat_id, max_user_id,
            "📡 <b>Ваши каналы (%d):</b>\n"
            "<blockquote>кнопки • помощники • параметры</blockquote>\n\n"
            "✅ — постинг активен\n"
            "⚠️ — бот не админ\n\n"
            "Выберите канал для настройки👇\n"
            "<blockquote>💡 Чтобы добавить новый канал, назначьте бота администратором канала</blockquote>"
            % len(channels),
            attachments=self._kb(buttons))

    async def _show_channel_info(self, max_user_id: str, chat_id: str, channel_id: str) -> None:
        user = await self._db.get_user_by_max_id(max_user_id)
        if not user:
            return

        channels = await self._db.get_accessible_channels(user["id"])
        channel = next((ch for ch in channels if ch["channel_id"] == channel_id), None)
        if not channel:
            await self._reply(
                chat_id, max_user_id, "❌ Канал не найден",
                attachments=self._kb([
                    [{"type": "callback", "text": "📡 К списку каналов", "payload": "back_channels"}],
                    self._back(),
                ]))
            return

        is_admin = await self._client.check_bot_is_admin(channel_id)
        if bool(is_admin) != bool(channel.get("bot_is_admin")):
            await self._db.update_bot_admin_status(channel_id, is_admin)

        owner_user_id = await self._db.resolve_button_owner_id(channel_id, user["id"], require_manage_buttons=False)
        if owner_user_id is None:
            owner_user_id = user["id"]

        helpers = await self._db.get_helpers(channel_id)

        # Собираем кнопки по группам
        all_sets = await self._db.get_all_button_sets(owner_user_id, channel_id)

        # --- Формируем карточку ---
        lines = [
            "📺 <b>%s</b>" % html.escape(channel["title"]),
            "",
        ]

        # Статус постинга
        if is_admin:
            lines.append("✅ Постинг подключён")
        else:
            lines.append("⚠️ Постинг недоступен — бот не админ")

        # Помощники
        lines.append("")
        lines.append("👥 <b>Помощники:</b>")
        if helpers:
            for h in helpers:
                name = h.get("alias") or h.get("helper_max_id", "?")
                lines.append("• %s" % name)
        else:
            lines.append("нет помощников")

        # Кнопки
        lines.append("")
        lines.append("🔘 <b>Активные кнопки:</b>")
        has_any_buttons = False

        ann_btns = all_sets.get("announcement", [])
        if ann_btns:
            has_any_buttons = True
            for b in ann_btns:
                lines.append("🔖 %s" % html.escape(b.get("text", "")))

        reg_btns = all_sets.get("regular", [])
        if reg_btns:
            has_any_buttons = True
            for b in reg_btns:
                lines.append("🆓 %s" % html.escape(b.get("text", "")))

        man_btns = all_sets.get("manual", [])
        if man_btns:
            has_any_buttons = True
            for b in man_btns:
                lines.append("✍️ %s" % html.escape(b.get("text", "")))

        # default (channel_buttons) отключён — не показываем

        if not has_any_buttons:
            lines.append("нет кнопок")

        buttons = [
            [{"type": "callback", "text": "🔘 Кнопки", "payload": "ch_buttons:%s" % channel_id}],
            [{"type": "callback", "text": "👥 Помощники", "payload": "ch_helpers:%s" % channel_id}],
            [{"type": "callback", "text": "📡 К списку каналов", "payload": "back_channels"}],
            self._back(),
        ]

        await self._reply(
            chat_id, max_user_id, "\n".join(lines),
            attachments=self._kb(buttons))

    async def _show_help(self, max_user_id: str, chat_id: str) -> None:
        await self._reply(
            chat_id, max_user_id,
            "ℹ️ <b>Инструкция</b>\n\n"
            "📖 <b>Как подключить бота?</b>\n"
            "Скопируйте 🆔 и отправьте в "
            "<a href=\"https://t.me/best_anons_bot\">@best_anons_bot</a>"
            " → настройки → автопостинг → MAX\n\n"
            "📖 <b>Как подключить каналы МАХ?</b>\n"
            "Добавьте бота в канал → назначьте администратором "
            "с правом публикации → бот пришлёт подтверждение\n"
            "<blockquote>💡 Анонсы из <a href=\"https://t.me/best_anons_bot\">@best_anons_bot</a>"
            " публикуются в MAX автоматически, после отправки из Telegram</blockquote>\n\n"
            "📖 <b>Настройка каналов</b>\n"
            "Список ваших каналов МАХ\n"
            "Здесь можно:\n"
            "• добавить помощников (🆔 в Telegram + админ в MAX)\n"
            "• настроить кнопки постов\n"
            "<blockquote>💡 Для каждого канала можно задать свой набор кнопок: "
            "для анонсов бота, своих анонсов и ручного постинга</blockquote>\n\n"
            "📖 <b>Мои кнопки</b>\n"
            "Библиотека созданных вами кнопок:\n"
            "• создать кнопку (название + ссылка)\n"
            "<blockquote>💡 Используются для анонсов бота, своих анонсов и ручного постинга</blockquote>\n\n"
            "📖 <b>Создать пост (ручной постинг)</b>\n"
            "Отправьте фото/видео/текст/ссылку → нажмите ✅ → выберите каналы "
            "→ при необходимости добавьте кнопки\n\n"
            "<b>Возникли сложности?</b>\n"
            "Помощь👉<a href=\"https://t.me/best_anons_tp\">@best_anons_tp</a>",
            attachments=self._kb([
                [{"type": "callback", "text": "📡 Настройка каналов", "payload": "menu_channels"}],
                [{"type": "callback", "text": "✏️ Создать пост", "payload": "menu_create_post"}],
                self._back(),
            ]))

    # ─── API info (только для админов) ───────────────────────────────────

    async def _show_api_info(self, max_user_id: str, chat_id: str) -> None:
        user = await self._db.get_user_by_max_id(max_user_id)
        if not user:
            return
        api_url = config.api_base_url
        await self._reply(
            chat_id, max_user_id,
            "🔑 <b>API информация</b>\n\n"
            "<b>Токен:</b>\n<code>%s</code>\n\n"
            "🌐 <b>Адрес API:</b>\n<code>%s</code>\n\n"
            "<b>Эндпоинты:</b>\n"
            "• <code>GET /health</code>\n"
            "• <code>GET /channels</code>\n"
            "• <code>POST /post</code>\n"
            "• <code>POST /edit</code>\n"
            "• <code>POST /delete</code>\n"
            "• <code>POST /delete-announcement</code>\n"
            "• <code>GET/POST /buttons</code>\n"
            "• <code>GET/POST/DELETE /buttons/{ch_id}</code>"
            % (user["token"], api_url),
            attachments=self._kb([
                [{"type": "callback", "text": "🔄 Сбросить токен", "payload": "reset_token"}],
                self._back(),
            ]))

    # ─── Токен ───────────────────────────────────────────────────────────

    async def _cb_reset_token(self, max_user_id: str, chat_id: str) -> None:
        await self._reply(
            chat_id, max_user_id,
            "⚠️ <b>Вы уверены?</b>\n\n"
            "Текущий токен перестанет работать.\n"
            "Все системы потеряют доступ.",
            attachments=self._kb([
                [{"type": "callback", "text": "⚠️ Да, сбросить", "payload": "confirm_reset_token"}],
                [{"type": "callback", "text": "↩️ Отмена", "payload": "cancel_reset_token"}],
                self._back(),
            ]))

    async def _cb_confirm_reset_token(self, max_user_id: str, chat_id: str) -> None:
        new_token = secrets.token_urlsafe(32)
        await self._db.update_user_token(max_user_id, new_token)
        logger.info("Token reset for user %s" % max_user_id)
        await self._reply(
            chat_id, max_user_id,
            "✅ <b>Токен сброшен!</b>\n\n"
            "Новый токен:\n<code>%s</code>\n\n"
            "⚠️ Обновите его во всех системах." % new_token,
            attachments=self._kb([self._back()]))

    # ═══════════════════════════════════════════════════════════════════════
    #  ПАКЕТНЫЙ ПОСТИНГ
    # ═══════════════════════════════════════════════════════════════════════

    async def _start_batch_post(self, max_user_id: str, chat_id: str) -> None:
        """Начало пакетного постинга."""
        user = await self._db.get_user_by_max_id(max_user_id)
        if not user:
            return

        channels = await self._db.get_accessible_channels(user["id"])
        if not channels:
            await self._reply(
                chat_id, max_user_id,
                "📭 Сначала подключите канал.\n"
                "Добавьте бота как подписчика и назначьте администратором.",
                attachments=self._kb([self._back()]))
            return

        self._set_pending(max_user_id, {
            "action": "batch_collecting",
            "posts": [],
        })

        await self._reply(
            chat_id, max_user_id,
            "✏️ <b>Создание постов</b>\n\n"
            "Отправьте боту:\n"
            "• фото/видео + текст/ссылку\n"
            "• только текст\n"
            "• или готовые посты\n"
            "<blockquote>💡Можно отправить несколько постов сразу "
            "или создать их прямо в боте</blockquote>\n\n"
            "Нажмите <b>✅ Готово</b>, когда закончите 👍",
            attachments=self._kb([self._back()]))

    async def _handle_batch_content(self, max_user_id: str, chat_id: str, msg: dict) -> None:
        """Пользователь отправил контент для пакетного постинга."""
        state = self._pending_states.get(max_user_id)
        if not state:
            return

        text, media_tokens, markup = self._extract_content(msg)

        if not text and not media_tokens:
            await self._reply(
                chat_id, max_user_id,
                "❌ Пустое сообщение. Отправьте текст, фото или видео.",
                attachments=self._kb([
                    [{"type": "callback", "text": "✅ Готово", "payload": "batch_done"}],
                    self._back(),
                ]))
            return

        state["posts"].append({
            "text": text,
            "media_tokens": media_tokens,
            "markup": markup,
            "source_mid": str(msg.get("body", {}).get("mid", "")),
        })
        count = len(state["posts"])

        preview_parts = []
        if text:
            short = text[:80] + "..." if len(text) > 80 else text
            preview_parts.append(short)
        if media_tokens:
            img = sum(1 for m in media_tokens if m["type"] == "image")
            vid = sum(1 for m in media_tokens if m["type"] == "video")
            parts = []
            if img:
                parts.append("%d фото" % img)
            if vid:
                parts.append("%d видео" % vid)
            preview_parts.append("📎 " + ", ".join(parts))

        preview = "\n".join(preview_parts) or "(контент)"

        await self._reply(
            chat_id, max_user_id,
            "✅ <b>Пост #%d добавлен</b>\n\n"
            "%s\n\n"
            "📝 Всего постов: <b>%d</b>\n"
            "<blockquote>💡 В канал перенесётся всё ваше форматирование, кроме цитат (MAX их не поддерживает)</blockquote>\n\n"
            "Отправьте ещё контент или нажмите <b>✅ Готово</b>" % (count, preview, count),
            attachments=self._kb([
                [{"type": "callback", "text": "✅ Готово (%d пост%s)" % (
                    count, self._pluralize(count)), "payload": "batch_done"}],
                self._back(),
            ]))

    async def _handle_message_edited(self, update: dict) -> None:
        """Обновить сохранённый черновик поста, если пользователь отредактировал сообщение."""
        msg = update.get("message", {})
        body = msg.get("body", {})
        source_mid = str(body.get("mid", ""))
        if not source_mid:
            return

        recipient = msg.get("recipient", {})
        chat_id = str(recipient.get("chat_id", ""))
        if chat_id.startswith("-"):
            return

        sender = msg.get("sender", {})
        max_user_id = str(sender.get("user_id", ""))
        if not max_user_id:
            return

        state = self._pending_states.get(max_user_id)
        if not state or not state.get("posts"):
            return

        text, media_tokens, markup = self._extract_content(msg)
        for post in reversed(state["posts"]):
            if post.get("source_mid") == source_mid:
                post["text"] = text
                post["media_tokens"] = media_tokens
                post["markup"] = markup
                logger.info("Draft post updated from edited message %s" % source_mid)
                return

    async def _batch_add_more(self, max_user_id: str, chat_id: str) -> None:
        """Кнопка 'Ещё пост' — вернуться к ожиданию контента."""
        state = self._pending_states.get(max_user_id)
        if not state:
            await self._show_main_menu(max_user_id, chat_id)
            return

        state["action"] = "batch_collecting"
        count = len(state.get("posts", []))

        await self._reply(
            chat_id, max_user_id,
            "✏️ Отправьте контент для следующего поста.\n\n"
            "📝 Уже добавлено: <b>%d</b>" % count,
            attachments=self._kb([
                [{"type": "callback", "text": "✅ Готово (%d)" % count, "payload": "batch_done"}],
                self._back(),
            ]))

    async def _batch_select_channels(self, max_user_id: str, chat_id: str) -> None:
        """Показать выбор каналов с чекбоксами."""
        state = self._pending_states.get(max_user_id)
        if not state or not state.get("posts"):
            await self._reply(
                chat_id, max_user_id,
                "❌ Нет постов для отправки.",
                attachments=self._kb([
                    [{"type": "callback", "text": "✏️ Создать пост", "payload": "menu_create_post"}],
                    self._back(),
                ]))
            return

        state["action"] = "batch_select_channels"
        if "selected" not in state:
            state["selected"] = set()

        user = await self._db.get_user_by_max_id(max_user_id)
        channels = await self._db.get_accessible_channels(user["id"])

        selected = state["selected"]
        buttons = []
        for ch in channels:
            is_sel = ch["channel_id"] in selected
            mark = "✅" if is_sel else "◻️"
            buttons.append([{
                "type": "callback",
                "text": "%s %s" % (mark, ch["title"]),
                "payload": "batch_toggle:%s" % ch["channel_id"],
            }])

        buttons.append([{
            "type": "callback",
            "text": "📺 Во все каналы",
            "payload": "batch_all",
        }])

        if selected:
            buttons.append([{
                "type": "callback",
                "text": "📤 Отправить сейчас",
                "payload": "batch_send",
            }])
            buttons.append([{
                "type": "callback",
                "text": "⏰ Запланировать пост",
                "payload": "batch_schedule:post",
            }])
            buttons.append([{
                "type": "callback",
                "text": "📌 Запланировать закреп",
                "payload": "batch_schedule:pin",
            }])

        buttons.append(self._back())

        count = len(state["posts"])
        await self._reply(
            chat_id, max_user_id,
            "📡 <b>Выберите каналы</b>\n\n"
            "📝 Постов к отправке: <b>%d</b>\n\n"
            "Отметьте каналы и выберите действие 👇" % count,
            attachments=self._kb(buttons))

    async def _batch_toggle_channel(self, max_user_id: str, chat_id: str, channel_id: str) -> None:
        state = self._pending_states.get(max_user_id)
        if not state:
            return
        selected = state.get("selected", set())
        if channel_id in selected:
            selected.discard(channel_id)
        else:
            selected.add(channel_id)
        state["selected"] = selected
        await self._batch_select_channels(max_user_id, chat_id)

    async def _batch_select_all(self, max_user_id: str, chat_id: str) -> None:
        state = self._pending_states.get(max_user_id)
        if not state:
            return
        user = await self._db.get_user_by_max_id(max_user_id)
        channels = await self._db.get_accessible_channels(user["id"])
        state["selected"] = {ch["channel_id"] for ch in channels}
        await self._batch_select_channels(max_user_id, chat_id)

    async def _batch_publish(self, max_user_id: str, chat_id: str) -> None:
        """Отправить все посты в выбранные каналы."""
        state = self._pending_states.get(max_user_id)
        if not state or not state.get("posts") or not state.get("selected"):
            await self._reply(
                chat_id, max_user_id,
                "❌ Выберите хотя бы один канал.",
                attachments=self._kb([self._back()]))
            return

        user = await self._db.get_user_by_max_id(max_user_id)
        if not user:
            return

        posts = state["posts"]
        selected = state["selected"]
        del self._pending_states[max_user_id]

        prev_mid = self._last_bot_msg.get(max_user_id)
        if prev_mid:
            try:
                await self._client.delete_message(prev_mid)
            except Exception:
                pass

        status_result = await self._client.send_message(
            chat_id,
            "⏳ <b>Отправка...</b>\n\n"
            "📝 %d пост%s → %d канал%s" % (
                len(posts), self._pluralize(len(posts)),
                len(selected), self._pluralize(len(selected)),
            ),
        )
        status_mid = str(status_result.get("message", {}).get("body", {}).get("mid", ""))

        sent_total = 0
        error_total = 0
        channel_reports = []
        channels = {ch["channel_id"]: ch for ch in await self._db.get_accessible_channels(user["id"])}

        for channel_id in selected:
            channel = channels.get(channel_id) or {"title": channel_id}
            channel_title = channel.get("title") or channel_id

            if not await self._db.user_can_post_to_channel(channel_id, user["id"]):
                channel_reports.append((channel_title, 0, len(posts), "Нет прав на публикацию"))
                error_total += len(posts)
                continue

            button_owner_id = await self._db.resolve_button_owner_id(channel_id, user["id"], require_manage_buttons=False) or user["id"]
            fixed_buttons = await self._db.get_effective_buttons(
                button_owner_id, channel_id, post_type="manual")
            button_layout = await self._db.get_effective_layout(
                button_owner_id, channel_id, post_type="manual")

            ch_sent = 0
            ch_errors = 0
            last_error = None

            for post in posts:
                text = post.get("text") or ""
                media_tokens = post.get("media_tokens", [])
                markup = post.get("markup") if isinstance(post.get("markup"), list) else None

                resolved_tokens = []
                for mt in media_tokens:
                    if mt.get("type") == "video":
                        token = mt.get("payload", {}).get("token", "")
                        new_token = await self._client.reupload_token(token, "video")
                        if new_token:
                            resolved_tokens.append({"type": "video", "payload": {"token": new_token}})
                        else:
                            logger.warning("video reupload failed, trying original token %s" % token[:16])
                            resolved_tokens.append(mt)
                    else:
                        resolved_tokens.append(mt)

                attachments = list(resolved_tokens)
                if fixed_buttons:
                    kb = self._build_keyboard_with_layout(fixed_buttons, button_layout)
                    if kb:
                        attachments.append({"type": "inline_keyboard", "payload": {"buttons": kb}})
                        # Логируем URL кнопок для диагностики
                        btn_urls = [b.get("url", "?")[:60] for row in kb for b in row]
                        logger.debug("BATCH_BUTTONS channel=%s urls=%s" % (channel_id, btn_urls))

                # Кнопка комментариев
                comment_key = ""
                if config.bot_link:
                    comment_key = self._generate_comment_key()
                    text_preview = (text or "")[:200]
                    await self._db.create_comment_link(
                        comment_key, channel_id, user["id"], text_preview)
                    self._append_comment_button_to_attachments(attachments, comment_key)

                try:
                    rendered_text, rendered_format, rendered_markup = self._render_post_text(text, markup)
                    logger.debug("BATCH_TEXT_DUMP channel=%s repr=%s" % (channel_id, repr(rendered_text)))
                    logger.debug(
                        "Batch payload preview channel=%s title=%s text_len=%s markup=%s media=%s buttons=%s"
                        % (channel_id, channel_title, len(rendered_text or ""), len(rendered_markup or []), len(resolved_tokens), bool(fixed_buttons))
                    )
                    result = await self._client.send_message(
                        channel_id,
                        rendered_text,
                        attachments=attachments or None,
                        format=rendered_format,
                        markup=rendered_markup,
                    )
                    ch_sent += 1
                    sent_total += 1
                    # Обновляем comment_link с msg_id
                    if comment_key:
                        msg_mid = str(result.get("message", {}).get("body", {}).get("mid", ""))
                        if msg_mid:
                            await self._db.update_comment_link_msg_id(comment_key, msg_mid)
                except Exception as e:
                    # Fallback: если ошибка связана с кнопками — пробуем без них
                    has_keyboard = any(
                        isinstance(a, dict) and a.get("type") == "inline_keyboard"
                        for a in attachments
                    )
                    if has_keyboard and "button" in str(e).lower():
                        logger.warning(
                            "Batch retry WITHOUT buttons → channel=%s error=%s" % (channel_id, e)
                        )
                        no_kb_attachments = [
                            a for a in attachments
                            if not (isinstance(a, dict) and a.get("type") == "inline_keyboard")
                        ]
                        try:
                            await self._client.send_message(
                                channel_id, rendered_text,
                                attachments=no_kb_attachments or None,
                                format=rendered_format, markup=rendered_markup,
                            )
                            ch_sent += 1
                            sent_total += 1
                            logger.info("Batch retry OK (без кнопок) → channel=%s" % channel_id)
                            continue
                        except Exception as e2:
                            logger.error("Batch retry also failed → channel=%s error=%s" % (channel_id, e2))

                    has_video = any(t.get("type") == "video" for t in resolved_tokens)
                    logger.error(
                        "Batch post failed → channel=%s title=%s has_video=%s error=%s"
                        % (channel_id, channel_title, has_video, e)
                    )
                    ch_errors += 1
                    error_total += 1
                    last_error = str(e)

            channel_reports.append((channel_title, ch_sent, ch_errors, last_error))

        if status_mid:
            try:
                await self._client.delete_message(status_mid)
            except Exception:
                pass

        lines = ["📤 <b>Отправка завершена</b>"]
        for channel_title, ch_sent, ch_errors, last_error in channel_reports:
            if ch_errors:
                line = "• <b>%s</b> — ✅ %d, ❌ %d" % (channel_title, ch_sent, ch_errors)
                if last_error:
                    short_error = last_error[:120] + "..." if len(last_error) > 120 else last_error
                    line += "\n  <i>%s</i>" % short_error
            else:
                line = "• <b>%s</b> — ✅ %d" % (channel_title, ch_sent)
            lines.append(line)

        lines.append("\n<b>Итого:</b> ✅ %d / ❌ %d" % (sent_total, error_total))

        await self._reply(
            chat_id, max_user_id, "\n".join(lines),
            attachments=self._kb([
                [{"type": "callback", "text": "✏️ Ещё посты", "payload": "menu_create_post"}],
                self._back(),
            ]))

    @staticmethod
    def _pluralize(n: int) -> str:
        """Окончание для 'пост(а/ов)', 'канал(а/ов)'."""
        if 11 <= n % 100 <= 14:
            return "ов"
        last = n % 10
        if last == 1:
            return ""
        if 2 <= last <= 4:
            return "а"
        return "ов"

    # ═══════════════════════════════════════════════════════════════════════
    #  ПЛАНИРОВАНИЕ И ОЧЕРЕДЬ
    # ═══════════════════════════════════════════════════════════════════════

    async def _batch_schedule_ask_date(self, max_user_id: str, chat_id: str, schedule_type: str) -> None:
        """Шаг 1: запросить дату отправки (текстовый ввод)."""
        state = self._pending_states.get(max_user_id)
        if not state:
            return
        state["schedule_type"] = schedule_type
        state["action"] = "awaiting_schedule_date"

        icon = "📌" if schedule_type == "pin" else "⏰"
        label = "закрепа" if schedule_type == "pin" else "поста"

        await self._reply(
            chat_id, max_user_id,
            "%s <b>Планирование %s</b>\n\n"
            "📅 Введите дату отправки в формате <b>ДД.ММ</b> или <b>ДД.ММ.ГГГГ</b>\n\n"
            "<blockquote>💡 Примеры: 05.04, 15.04.2026</blockquote>" % (icon, label),
            attachments=self._kb([
                [{"type": "callback", "text": "↩️ К каналам", "payload": "batch_done"}],
                self._back(),
            ]))

    async def _handle_schedule_date_input(self, max_user_id: str, chat_id: str, text: str) -> None:
        """Обработка текстового ввода даты."""
        state = self._pending_states.get(max_user_id)
        if not state:
            return

        tz = timezone(timedelta(hours=3))
        now = datetime.now(tz)

        # Парсим дату
        text = text.strip().replace("/", ".").replace("-", ".")
        parts = text.split(".")
        try:
            day = int(parts[0])
            month = int(parts[1])
            year = int(parts[2]) if len(parts) >= 3 else now.year
            if year < 100:
                year += 2000
            target_date = datetime(year, month, day, tzinfo=tz)

            # Проверяем что дата не в прошлом
            if target_date.date() < now.date():
                await self._reply(
                    chat_id, max_user_id,
                    "❌ Дата <b>%02d.%02d.%d</b> уже прошла.\n\n"
                    "Введите дату в будущем:" % (day, month, year),
                    attachments=self._kb([
                        [{"type": "callback", "text": "↩️ К каналам", "payload": "batch_done"}],
                        self._back(),
                    ]))
                return

        except (ValueError, IndexError):
            await self._reply(
                chat_id, max_user_id,
                "❌ Неверный формат даты.\n\n"
                "Введите дату в формате <b>ДД.ММ</b> или <b>ДД.ММ.ГГГГ</b>\n"
                "<blockquote>💡 Примеры: 05.04, 15.04.2026</blockquote>",
                attachments=self._kb([
                    [{"type": "callback", "text": "↩️ К каналам", "payload": "batch_done"}],
                    self._back(),
                ]))
            return

        # Дата валидна — сохраняем и спрашиваем время
        state["schedule_date"] = "%04d-%02d-%02d" % (year, month, day)
        state["schedule_date_display"] = "%02d.%02d.%04d" % (day, month, year)
        state["action"] = "awaiting_schedule_time"

        schedule_type = state.get("schedule_type", "post")
        icon = "📌" if schedule_type == "pin" else "⏰"

        posts = state.get("posts", [])
        post_count = len(posts)

        if post_count > 1 and schedule_type == "pin":
            date_info = "Начиная с <b>%02d.%02d.%04d</b> (%d постов по дням)" % (day, month, year, post_count)
        else:
            date_info = "Дата: <b>%02d.%02d.%04d</b>" % (day, month, year)

        await self._reply(
            chat_id, max_user_id,
            "%s %s\n\n"
            "🕐 Теперь введите время отправки в формате <b>ЧЧ:ММ</b>\n\n"
            "<blockquote>💡 Примеры: 09:00, 18:30</blockquote>" % (icon, date_info),
            attachments=self._kb([
                [{"type": "callback", "text": "↩️ К каналам", "payload": "batch_done"}],
                self._back(),
            ]))

    async def _handle_schedule_time_input(self, max_user_id: str, chat_id: str, text: str) -> None:
        """Обработка текстового ввода времени."""
        state = self._pending_states.get(max_user_id)
        if not state:
            return

        tz = timezone(timedelta(hours=3))
        now = datetime.now(tz)

        # Парсим время
        text = text.strip().replace(".", ":").replace("-", ":").replace(" ", ":")
        parts = text.split(":")
        try:
            hour = int(parts[0])
            minute = int(parts[1]) if len(parts) >= 2 else 0
            if not (0 <= hour <= 23 and 0 <= minute <= 59):
                raise ValueError("out of range")
        except (ValueError, IndexError):
            await self._reply(
                chat_id, max_user_id,
                "❌ Неверный формат времени.\n\n"
                "Введите время в формате <b>ЧЧ:ММ</b>\n"
                "<blockquote>💡 Примеры: 09:00, 18:30</blockquote>",
                attachments=self._kb([
                    [{"type": "callback", "text": "↩️ К каналам", "payload": "batch_done"}],
                    self._back(),
                ]))
            return

        # Проверяем что дата+время не в прошлом
        date_str = state.get("schedule_date", now.strftime("%Y-%m-%d"))
        date_parts = date_str.split("-")
        target_dt = datetime(int(date_parts[0]), int(date_parts[1]), int(date_parts[2]),
                             hour, minute, tzinfo=tz)

        if target_dt <= now:
            await self._reply(
                chat_id, max_user_id,
                "❌ Время <b>%02d:%02d</b> на <b>%s</b> уже прошло.\n\n"
                "Введите время в будущем:" % (hour, minute, state.get("schedule_date_display", "")),
                attachments=self._kb([
                    [{"type": "callback", "text": "📅 Другая дата", "payload": "batch_schedule:%s" % state.get("schedule_type", "post")}],
                    self._back(),
                ]))
            return

        # Время валидно — показываем превью
        state["schedule_time"] = "%02d:%02d" % (hour, minute)
        state["action"] = "batch_collecting"  # сбрасываем ожидание текста
        await self._batch_schedule_preview(max_user_id, chat_id)

    async def _batch_schedule_preview(self, max_user_id: str, chat_id: str) -> None:
        """Показать превью расписания перед подтверждением."""
        state = self._pending_states.get(max_user_id)
        if not state or not state.get("posts") or not state.get("selected"):
            if max_user_id in self._pending_states:
                del self._pending_states[max_user_id]
            await self._reply(chat_id, max_user_id,
                              "❌ Нет данных для планирования. Начните заново.",
                              attachments=self._kb([
                                  [{"type": "callback", "text": "✏️ Создать пост", "payload": "menu_create_post"}],
                                  self._back(),
                              ]))
            return

        tz = timezone(timedelta(hours=3))
        now = datetime.now(tz)
        posts = state["posts"]
        selected = state.get("selected", set())
        schedule_type = state.get("schedule_type", "post")
        time_str = state.get("schedule_time", "09:00")
        date_str = state.get("schedule_date", now.strftime("%Y-%m-%d"))

        hour, minute = int(time_str.split(":")[0]), int(time_str.split(":")[1])
        date_parts = date_str.split("-")
        base_dt = datetime(int(date_parts[0]), int(date_parts[1]), int(date_parts[2]),
                           hour, minute, tzinfo=tz)

        # Рассчитываем даты для каждого поста
        schedule_dates = []
        for i in range(len(posts)):
            schedule_dates.append(base_dt + timedelta(days=i))

        state["schedule_dates"] = [d.strftime("%Y-%m-%d %H:%M:%S") for d in schedule_dates]

        icon = "📌" if schedule_type == "pin" else "⏰"
        label = "закрепа" if schedule_type == "pin" else "поста"
        days_ru = ["пн", "вт", "ср", "чт", "пт", "сб", "вс"]

        lines = ["%s <b>Расписание %s</b>\n" % (icon, label)]

        # Каналы
        ch_names = []
        for ch_id in selected:
            title = await self._client.get_chat_title(ch_id)
            ch_names.append(title)
        lines.append("📡 %s\n" % ", ".join(ch_names))

        for i, (post, dt) in enumerate(zip(posts, schedule_dates)):
            day_name = days_ru[dt.weekday()]
            date_display = dt.strftime("%d.%m")
            text_preview = (post.get("text") or "")[:40]
            if text_preview:
                text_preview = " — %s" % text_preview
            media_count = len(post.get("media_tokens", []))
            media_str = "🖼" if media_count else ""
            lines.append(
                "%d. %s %s %s в %s%s %s"
                % (i + 1, icon, day_name, date_display, time_str, text_preview, media_str)
            )

        lines.append("")
        if len(posts) == 1:
            lines.append("Пост будет отправлен <b>%s в %s</b>" % (
                state.get("schedule_date_display", ""), time_str))
        lines.append("\nПодтвердите расписание 👇")

        await self._reply(
            chat_id, max_user_id,
            "\n".join(lines),
            attachments=self._kb([
                [{"type": "callback", "text": "✅ Подтвердить", "payload": "sched_confirm"}],
                [{"type": "callback", "text": "📅 Другая дата и время", "payload": "sched_change_datetime"}],
                self._back(),
            ]))

    async def _batch_schedule_confirm(self, max_user_id: str, chat_id: str) -> None:
        """Сохранить запланированные посты в БД."""
        state = self._pending_states.get(max_user_id)
        if not state:
            await self._reply(chat_id, max_user_id, "❌ Сессия истекла. Начните заново.",
                              attachments=self._kb([
                                  [{"type": "callback", "text": "✏️ Создать пост", "payload": "menu_create_post"}],
                                  self._back(),
                              ]))
            return
        user = await self._db.get_user_by_max_id(max_user_id)
        if not user:
            if max_user_id in self._pending_states:
                del self._pending_states[max_user_id]
            return

        posts = state.get("posts", [])
        selected = list(state.get("selected", set()))
        schedule_dates = state.get("schedule_dates", [])
        schedule_type = state.get("schedule_type", "post")

        if not posts or not selected or not schedule_dates:
            # Очищаем состояние при ошибке
            if max_user_id in self._pending_states:
                del self._pending_states[max_user_id]
            await self._reply(chat_id, max_user_id, "❌ Нет данных для планирования.",
                              attachments=self._kb([
                                  [{"type": "callback", "text": "✏️ Создать пост", "payload": "menu_create_post"}],
                                  self._back(),
                              ]))
            return

        saved = 0
        for i, post in enumerate(posts):
            if i >= len(schedule_dates):
                break
            scheduled_at = schedule_dates[i]
            text = post.get("text")
            markup = post.get("markup")
            media_tokens = post.get("media_tokens", [])

            await self._db.add_scheduled_post(
                user_id=user["id"],
                post_type=schedule_type,
                channels_json=json.dumps(selected),
                text=text,
                markup_json=json.dumps(markup) if markup else None,
                media_json=json.dumps(media_tokens),
                buttons_json=None,
                scheduled_at=scheduled_at,
            )
            saved += 1

        # Очищаем состояние
        if max_user_id in self._pending_states:
            del self._pending_states[max_user_id]

        icon = "📌" if schedule_type == "pin" else "⏰"
        label = "закреп" if schedule_type == "pin" else "пост"

        await self._reply(
            chat_id, max_user_id,
            "%s <b>Запланировано!</b>\n\n"
            "%d %s%s → %d канал%s\n\n"
            "Посмотреть в 📋 Очередь отправки"
            % (icon, saved, label, self._pluralize(saved),
               len(selected), self._pluralize(len(selected))),
            attachments=self._kb([
                [{"type": "callback", "text": "✏️ Создать пост", "payload": "menu_create_post"}],
                [{"type": "callback", "text": "📋 Очередь отправки", "payload": "menu_queue"}],
                self._back(),
            ]))
        logger.info("Scheduled %d %s posts for user %s" % (saved, schedule_type, max_user_id))

    async def _show_queue(self, max_user_id: str, chat_id: str) -> None:
        """Показать очередь запланированных постов."""
        user = await self._db.get_user_by_max_id(max_user_id)
        if not user:
            return

        posts = await self._db.get_user_scheduled(user["id"])

        if not posts:
            await self._reply(
                chat_id, max_user_id,
                "📋 <b>Нет запланированных постов</b>",
                attachments=self._kb([
                    [{"type": "callback", "text": "✏️ Создать пост", "payload": "menu_create_post"}],
                    self._back(),
                ]))
            return

        days_ru = ["пн", "вт", "ср", "чт", "пт", "сб", "вс"]

        lines = ["📋 <b>Очередь отправки</b> (%d)\n" % len(posts)]
        buttons = []

        for i, sp in enumerate(posts[:20]):
            post_type = sp.get("post_type", "post")
            icon = "📌" if post_type == "pin" else "⏰"
            label = "Закреп" if post_type == "pin" else "Пост"

            try:
                dt = datetime.fromisoformat(sp["scheduled_at"])
                day_name = days_ru[dt.weekday()]
                date_str = "%s %s в %s" % (day_name, dt.strftime("%d.%m"), dt.strftime("%H:%M"))
                btn_date = dt.strftime("%d.%m %H:%M")
            except Exception:
                date_str = sp.get("scheduled_at", "?")
                btn_date = date_str

            channels = json.loads(sp.get("channels_json", "[]"))
            ch_names = []
            for ch_id in channels[:3]:
                title = await self._client.get_chat_title(str(ch_id))
                ch_names.append(title)
            ch_str = ", ".join(ch_names)
            if len(channels) > 3:
                ch_str += " +%d" % (len(channels) - 3)

            text_preview = (sp.get("text") or "")[:50]
            media = json.loads(sp.get("media_json") or "[]")
            media_str = "🖼" if media else ""

            lines.append(
                "%d. %s <b>%s</b> — %s\n"
                "   → %s\n"
                "   %s %s" % (i + 1, icon, label, date_str, ch_str, media_str, text_preview)
            )

            buttons.append([{
                "type": "callback",
                "text": "%s %d. %s %s" % (icon, i + 1, label, btn_date),
                "payload": "sched_detail:%d" % sp["id"],
            }])

        buttons.append(self._back())

        await self._reply(
            chat_id, max_user_id,
            "\n".join(lines),
            attachments=self._kb(buttons))

    async def _show_sched_detail(self, max_user_id: str, chat_id: str, post_id: int) -> None:
        """Показать детали запланированного поста."""
        user = await self._db.get_user_by_max_id(max_user_id)
        if not user:
            return
        sp = await self._db.get_scheduled_by_id(post_id)
        if not sp or sp.get("user_id") != user["id"]:
            await self._show_queue(max_user_id, chat_id)
            return

        post_type = sp.get("post_type", "post")
        icon = "📌" if post_type == "pin" else "⏰"
        label = "Закреп" if post_type == "pin" else "Пост"

        days_ru = ["пн", "вт", "ср", "чт", "пт", "сб", "вс"]
        try:
            dt = datetime.fromisoformat(sp["scheduled_at"])
            day_name = days_ru[dt.weekday()]
            date_str = "%s %s в %s" % (day_name, dt.strftime("%d.%m"), dt.strftime("%H:%M"))
        except Exception:
            date_str = sp.get("scheduled_at", "?")

        channels = json.loads(sp.get("channels_json", "[]"))
        ch_names = []
        for ch_id in channels:
            title = await self._client.get_chat_title(str(ch_id))
            ch_names.append(title)

        text_preview = (sp.get("text") or "(без текста)")[:200]
        media = json.loads(sp.get("media_json") or "[]")
        media_info = ""
        if media:
            img = sum(1 for m in media if m.get("type") == "image")
            vid = sum(1 for m in media if m.get("type") == "video")
            parts = []
            if img:
                parts.append("%d фото" % img)
            if vid:
                parts.append("%d видео" % vid)
            media_info = "\n📎 " + ", ".join(parts)

        await self._reply(
            chat_id, max_user_id,
            "%s <b>%s</b>\n\n"
            "📅 %s\n"
            "📡 %s%s\n\n"
            "%s" % (icon, label, date_str, ", ".join(ch_names), media_info, html.escape(text_preview)),
            attachments=self._kb([
                [{"type": "callback", "text": "❌ Отменить", "payload": "sched_cancel:%d" % post_id}],
                [{"type": "callback", "text": "📋 К очереди", "payload": "menu_queue"}],
                self._back(),
            ]))

    async def _cancel_scheduled(self, max_user_id: str, chat_id: str, post_id: int) -> None:
        """Отменить запланированный пост."""
        user = await self._db.get_user_by_max_id(max_user_id)
        if not user:
            return
        success = await self._db.cancel_scheduled(post_id, user["id"])
        if success:
            await self._reply(
                chat_id, max_user_id,
                "✅ Запланированный пост отменён.",
                attachments=self._kb([
                    [{"type": "callback", "text": "✏️ Создать пост", "payload": "menu_create_post"}],
                    [{"type": "callback", "text": "📋 К очереди", "payload": "menu_queue"}],
                    self._back(),
                ]))
        else:
            await self._show_queue(max_user_id, chat_id)

    # ═══════════════════════════════════════════════════════════════════════
    #  ПОМОЩНИКИ
    # ═══════════════════════════════════════════════════════════════════════

    async def _show_helpers(self, max_user_id: str, chat_id: str, channel_id: str) -> None:
        """Показать список помощников канала."""
        user = await self._db.get_user_by_max_id(max_user_id)
        if not user:
            return

        # Только владелец может управлять помощниками
        if not await self._db.channel_belongs_to_user(channel_id, user["id"]):
            await self._reply(
                chat_id, max_user_id,
                "❌ Только владелец канала может управлять помощниками.",
                attachments=self._kb([
                    [{"type": "callback", "text": "📡 К каналу", "payload": "ch_info:%s" % channel_id}],
                    self._back(),
                ]))
            return

        helpers = await self._db.get_helpers(channel_id)
        title = await self._client.get_chat_title(channel_id)

        if not helpers:
            await self._reply(
                chat_id, max_user_id,
                "👥 <b>Помощники канала «%s»</b>\n\n"
                "Пока нет помощников.\n"
                "Добавьте администратора канала как помощника." % title,
                attachments=self._kb([
                    [{"type": "callback", "text": "➕ Добавить помощника", "payload": "add_helper:%s" % channel_id}],
                    [{"type": "callback", "text": "📺 К каналу", "payload": "ch_info:%s" % channel_id}],
                    self._back(),
                ]))
            return

        lines = ["👥 <b>Помощники канала «%s»:</b>\n" % title]
        buttons = []
        for h in helpers:
            name = h.get("alias") or h.get("helper_max_id", "?")
            perms = []
            if h.get("can_post"):
                perms.append("постинг")
            if h.get("can_manage_buttons"):
                perms.append("кнопки")
            lines.append("• %s (%s)" % (name, ", ".join(perms) if perms else "без прав"))
            buttons.append([{
                "type": "callback",
                "text": "❌ %s" % name,
                "payload": "rm_helper:%s:%s" % (channel_id, h["helper_user_id"]),
            }])

        buttons.append([{"type": "callback", "text": "➕ Добавить помощника",
                         "payload": "add_helper:%s" % channel_id}])
        buttons.append([{"type": "callback", "text": "📺 К каналу",
                         "payload": "ch_info:%s" % channel_id}])
        buttons.append(self._back())

        await self._reply(
            chat_id, max_user_id, "\n".join(lines),
            attachments=self._kb(buttons))

    async def _add_helper_start(self, max_user_id: str, chat_id: str, channel_id: str) -> None:
        """Получить список админов канала из MAX и показать для выбора."""
        members = await self._client.get_chat_members(channel_id)

        # Фильтруем: только те, кто зарегистрирован в боте и не является владельцем
        user = await self._db.get_user_by_max_id(max_user_id)
        candidates = []
        for m in members:
            m_id = str(m.get("user_id", ""))
            if not m_id or m_id == max_user_id:
                continue
            if m.get("is_admin") or m.get("is_owner"):
                # Проверяем, зарегистрирован ли в боте
                m_user = await self._db.get_user_by_max_id(m_id)
                if m_user:
                    name = m.get("name") or m.get("first_name", "") + " " + m.get("last_name", "")
                    name = name.strip() or m.get("username", m_id)
                    candidates.append({"max_id": m_id, "name": name, "user_id": m_user["id"]})

        if not candidates:
            await self._reply(
                chat_id, max_user_id,
                "📭 <b>Нет доступных кандидатов</b>\n\n"
                "ℹ️ <b>Помощнику нужно:</b>\n"
                "• зайти в <a href=\"https://t.me/best_anons_bot\">@best_anons_bot</a>"
                " → настройки → автопостинг → MAX\n"
                "• перейти по ссылке из Telegram-бота в MAX и отправить "
                "свой 🆔 обратно в бот\n"
                "(🆔 — кнопка в главном меню)\n"
                "<blockquote>💡 Связка MAX и Telegram делается один раз\n"
                "Если уже подключал — повторять не нужно</blockquote>\n\n"
                "ℹ️ <b>Вам нужно:</b>\n"
                "• назначить помощника администратором канала в MAX\n"
                "• добавить ниже👇 — он появится автоматически\n\n"
                "Помощь👉<a href=\"https://t.me/best_anons_tp\">@best_anons_tp</a>",
                attachments=self._kb([
                    [{"type": "callback", "text": "👥 К помощникам",
                      "payload": "ch_helpers:%s" % channel_id}],
                    self._back(),
                ]))
            return

        buttons = []
        for c in candidates:
            buttons.append([{
                "type": "callback",
                "text": "👤 %s" % c["name"],
                "payload": "sel_helper:%s:%s" % (channel_id, c["max_id"]),
            }])
        buttons.append([{"type": "callback", "text": "↩️ Назад",
                         "payload": "ch_helpers:%s" % channel_id}])
        buttons.append(self._back())

        await self._reply(
            chat_id, max_user_id,
            "👤 <b>Выберите помощника:</b>",
            attachments=self._kb(buttons))

    async def _select_helper_perms(self, max_user_id: str, chat_id: str,
                                    channel_id: str, helper_max_id: str) -> None:
        """Показать выбор прав для помощника."""
        buttons = [
            [{"type": "callback", "text": "✅ Постинг",
              "payload": "helper_perm:%s:%s:1:0" % (channel_id, helper_max_id)}],
            [{"type": "callback", "text": "✅ Кнопки",
              "payload": "helper_perm:%s:%s:0:1" % (channel_id, helper_max_id)}],
            [{"type": "callback", "text": "✅ Постинг + Кнопки",
              "payload": "helper_perm:%s:%s:1:1" % (channel_id, helper_max_id)}],
            [{"type": "callback", "text": "↩️ Назад",
              "payload": "add_helper:%s" % channel_id}],
            self._back(),
        ]

        await self._reply(
            chat_id, max_user_id,
            "⚙️ <b>Права помощника</b>\n\nВыберите уровень доступа:",
            attachments=self._kb(buttons))

    async def _confirm_add_helper(self, max_user_id: str, chat_id: str,
                                   channel_id: str, helper_max_id: str,
                                   can_post: bool, can_manage_buttons: bool) -> None:
        """Добавить помощника с выбранными правами."""
        user = await self._db.get_user_by_max_id(max_user_id)
        helper_user = await self._db.get_user_by_max_id(helper_max_id)
        if not user or not helper_user:
            return

        await self._db.add_helper(
            channel_id=channel_id,
            owner_user_id=user["id"],
            helper_user_id=helper_user["id"],
            can_post=can_post,
            can_manage_buttons=can_manage_buttons,
        )

        perms = []
        if can_post:
            perms.append("постинг")
        if can_manage_buttons:
            perms.append("управление кнопками")

        await self._reply(
            chat_id, max_user_id,
            "✅ <b>Помощник добавлен!</b>\n\n"
            "Права: %s" % (", ".join(perms) or "—"),
            attachments=self._kb([
                [{"type": "callback", "text": "👥 К помощникам",
                  "payload": "ch_helpers:%s" % channel_id}],
                self._back(),
            ]))

    async def _remove_helper(self, max_user_id: str, chat_id: str,
                              channel_id: str, helper_user_id: int) -> None:
        await self._db.remove_helper(channel_id, helper_user_id)
        await self._reply(
            chat_id, max_user_id, "✅ Помощник удалён")
        await self._show_helpers(max_user_id, chat_id, channel_id)

    async def _start_helper_alias(self, max_user_id: str, chat_id: str,
                                   channel_id: str, helper_user_id: int) -> None:
        self._set_pending(max_user_id, {
            "action": "awaiting_helper_alias",
            "channel_id": channel_id,
            "helper_user_id": helper_user_id,
        })
        await self._reply(
            chat_id, max_user_id,
            "✏️ Введите имя для помощника:",
            attachments=self._kb([
                [{"type": "callback", "text": "↩️ Отмена",
                  "payload": "ch_helpers:%s" % channel_id}],
                self._back(),
            ]))

    async def _handle_helper_alias(self, max_user_id: str, chat_id: str, text: str) -> None:
        state = self._pending_states.get(max_user_id)
        if not state:
            return
        channel_id = state["channel_id"]
        helper_user_id = state["helper_user_id"]
        del self._pending_states[max_user_id]

        await self._db.update_helper_alias(channel_id, helper_user_id, text)
        await self._reply(chat_id, max_user_id, "✅ Имя обновлено: %s" % text)
        await self._show_helpers(max_user_id, chat_id, channel_id)

    # ═══════════════════════════════════════════════════════════════════════
    #  КНОПКИ КАНАЛА
    # ═══════════════════════════════════════════════════════════════════════

    async def _show_button_sets(self, max_user_id: str, chat_id: str, channel_id: str) -> None:
        user = await self._db.get_user_by_max_id(max_user_id)
        if not user:
            return
        owner_user_id = await self._resolve_button_owner_id(user["id"], channel_id)
        if owner_user_id is None:
            await self._reply(
                chat_id, max_user_id,
                "❌ У вас нет прав на управление кнопками этого канала.",
                attachments=self._kb([
                    [{"type": "callback", "text": "📺 К каналу", "payload": "ch_info:%s" % channel_id}],
                    self._back(),
                ]))
            return

        title = await self._client.get_chat_title(channel_id)
        counts = await self._db.get_button_scope_counts(owner_user_id, channel_id)
        buttons = []
        for scope in ("announcement", "regular", "manual"):  # default отключен
            label, _ = self._button_scope_meta(scope)
            buttons.append([{
                "type": "callback",
                "text": "%s — %d" % (label, counts.get(scope, 0)),
                "payload": "ch_btn_scope:%s:%s" % (channel_id, scope),
            }])
        buttons.append([{"type": "callback", "text": "↩️ Назад", "payload": "ch_info:%s" % channel_id}])
        buttons.append(self._back())

        await self._reply(
            chat_id, max_user_id,
            "🔘 <b>Кнопки канала «%s»</b>\n"
            "<blockquote>💡 Здесь можно задать кнопки для разных типов постов</blockquote>\n\n"
            "🔖 <b>Анонсы бота</b> — посты из "
            "<a href=\"https://t.me/best_anons_bot\">@best_anons_bot</a>"
            " (после отправки из Telegram)\n"
            "🆓 <b>Свои анонсы</b> — ваши посты в вашем канале "
            "(кнопки добавляются при наличии цены в посте)\n"
            "✍️ <b>Ручной постинг</b> — через «Создать пост» "
            "(кнопки добавляются автоматически)\n\n"
            "«Купи в Америке» подключается в "
            "<a href=\"https://t.me/best_anons_bot\">@best_anons_bot</a>"
            " → в МАХ автоматически добавляется кнопка «🛍Заказать»\n\n"
            "<blockquote>💡 К одному посту можно добавить несколько кнопок</blockquote>\n\n"
            "Помощь👉<a href=\"https://t.me/best_anons_tp\">@best_anons_tp</a>"
            % html.escape(title),
            attachments=self._kb(buttons),
        )

    async def _show_button_group(self, max_user_id: str, chat_id: str, channel_id: str, scope: str) -> None:
        user = await self._db.get_user_by_max_id(max_user_id)
        if not user:
            return
        owner_user_id = await self._resolve_button_owner_id(user["id"], channel_id)
        if owner_user_id is None:
            await self._show_button_sets(max_user_id, chat_id, channel_id)
            return

        title = await self._client.get_chat_title(channel_id)
        buttons_list = await self._get_scope_buttons(owner_user_id, channel_id, scope)
        scope_label, _ = self._button_scope_meta(scope)

        lines = ["🔘 <b>канал %s</b>" % html.escape(title), "", "%s" % scope_label]
        if buttons_list:
            lines.append("кнопки для постов из <a href=\"https://t.me/best_anons_bot\">@best_anons_bot</a>"
                         if scope == "announcement" else "")
            lines.append("")
            for idx, btn in enumerate(buttons_list, 1):
                lines.append("%d. %s" % (idx, html.escape(btn.get("text", ""))))
        else:
            lines.append("")
            lines.append("📭 Кнопки не настроены")
            lines.append("")
            lines.append("ℹ️ Добавьте кнопки — они будут автоматически применяться к постам")
            lines.append("")
            lines.append("💡")
            lines.append("• можно создать новую кнопку или выбрать из библиотеки")
            lines.append("• настройте порядок и расположение кнопок")
            lines.append("• проверьте, как они будут выглядеть перед публикацией")

        # Кнопки по две в строке как требует заказчик
        buttons = [
            [
                {"type": "callback", "text": "➕ Добавить кнопку", "payload": "ch_btn_add:%s:%s" % (channel_id, scope)},
                {"type": "callback", "text": "➖ Удалить кнопку", "payload": "ch_btn_del_menu:%s:%s" % (channel_id, scope)},
            ],
            [
                {"type": "callback", "text": "📚 Из библиотеки", "payload": "ch_btn_lib:%s:%s" % (channel_id, scope)},
                {"type": "callback", "text": "🗑 Удалить все кнопки", "payload": "ch_btn_clear:%s:%s" % (channel_id, scope)},
            ],
            [{"type": "callback", "text": "🔄 Расположение кнопок", "payload": "ch_btn_layout:%s:%s" % (channel_id, scope)}],
            [{"type": "callback", "text": "🔄 Изменить порядок", "payload": "ch_btn_reorder:%s:%s" % (channel_id, scope)}],
            [{"type": "callback", "text": "🔍 Вид кнопок", "payload": "ch_btn_test:%s:%s" % (channel_id, scope)}],
            [
                {"type": "callback", "text": "↩️ Назад", "payload": "ch_buttons:%s" % channel_id},
                {"type": "callback", "text": "📁 Главное меню", "payload": "menu_main"},
            ],
        ]
        await self._reply(chat_id, max_user_id, "\n".join(lines), attachments=self._kb(buttons))

    async def _start_add_channel_button(self, max_user_id: str, chat_id: str, channel_id: str, scope: str) -> None:
        user = await self._db.get_user_by_max_id(max_user_id)
        if not user:
            return
        owner_user_id = await self._resolve_button_owner_id(user["id"], channel_id)
        if owner_user_id is None:
            await self._show_button_sets(max_user_id, chat_id, channel_id)
            return
        scope_label, _ = self._button_scope_meta(scope)
        self._set_pending(max_user_id, {
            "action": "awaiting_channel_button_text",
            "channel_id": channel_id,
            "button_scope": scope,
            "button_owner_user_id": owner_user_id,
        })
        await self._reply(
            chat_id, max_user_id,
            "<b>➕ Отправьте название кнопки</b>\n\n"
            "<blockquote>💡 Можно добавить смайл и задать любое название (до 15 символов)</blockquote>\n"
            "<b>Примеры:</b>\n"
            "🛒 Купить • 📦 Условия • 🎁 Акция • 🤝 Помощь\n"
            "🛍 Оформить заказ • ✈️ Условия доставки",
            attachments=self._kb([
                [{"type": "callback", "text": "↩️ Назад", "payload": "ch_btn_scope:%s:%s" % (channel_id, scope)}],
                self._back(),
            ]))

    async def _handle_channel_button_text(self, max_user_id: str, chat_id: str, btn_text: str) -> None:
        state = self._pending_states.get(max_user_id)
        if not state:
            return
        channel_id = state.get("channel_id", "")
        scope = state.get("button_scope", "default")
        if not btn_text:
            await self._reply(
                chat_id, max_user_id,
                "❌ Название кнопки не может быть пустым. Отправьте название ещё раз.",
                attachments=self._kb([
                    [{"type": "callback", "text": "↩️ Назад", "payload": "ch_btn_scope:%s:%s" % (channel_id, scope)}],
                    self._back(),
                ]))
            return
        state["action"] = "awaiting_channel_button_url"
        state["button_text"] = btn_text[:80]
        state["_ts"] = time.monotonic()
        await self._reply(
            chat_id, max_user_id,
            "Кнопка <b>%s</b>\n\n🔗 Пришлите ссылку (URL) для этой кнопки" % html.escape(state["button_text"]),
            attachments=self._kb([
                [{"type": "callback", "text": "↩️ Назад", "payload": "ch_btn_scope:%s:%s" % (channel_id, scope)}],
                self._back(),
            ]))

    async def _handle_channel_button_url(self, max_user_id: str, chat_id: str, btn_url: str) -> None:
        state = self._pending_states.get(max_user_id)
        if not state:
            return
        channel_id = state.get("channel_id", "")
        btn_text = state.get("button_text", "")
        scope = state.get("button_scope", "default")
        owner_user_id = state.get("button_owner_user_id")
        if not btn_url or not (btn_url.startswith("http://") or btn_url.startswith("https://")):
            await self._reply(
                chat_id, max_user_id,
                "❌ Ссылка должна начинаться с <code>http://</code> или <code>https://</code>.\nОтправьте URL ещё раз.",
                attachments=self._kb([
                    [{"type": "callback", "text": "↩️ Назад", "payload": "ch_btn_scope:%s:%s" % (channel_id, scope)}],
                    self._back(),
                ]))
            return
        if owner_user_id is None:
            user = await self._db.get_user_by_max_id(max_user_id)
            if not user:
                return
            owner_user_id = await self._resolve_button_owner_id(user["id"], channel_id)
        if owner_user_id is None:
            self._pending_states.pop(max_user_id, None)
            await self._show_button_sets(max_user_id, chat_id, channel_id)
            return

        current = await self._get_scope_buttons(owner_user_id, channel_id, scope)
        new_buttons = [{"text": btn.get("text", ""), "url": btn.get("url", "")} for btn in current]
        new_buttons.append({"text": btn_text, "url": btn_url})
        if scope == "default":
            await self._db.set_channel_buttons(owner_user_id, channel_id, new_buttons)
        else:
            await self._db.set_button_set(owner_user_id, channel_id, scope, new_buttons)
        self._pending_states.pop(max_user_id, None)

        # Автоматически сохраняем кнопку в библиотеку пользователя
        user = await self._db.get_user_by_max_id(max_user_id)
        if user:
            await self._db.save_button(user["id"], btn_text, btn_url)

        await self._reply(
            chat_id, max_user_id,
            "✅ <b>Кнопка добавлена</b>\n\n<b>%s</b>\n<code>%s</code>\n\n"
            "💾 Сохранена в библиотеку" % (html.escape(btn_text), html.escape(btn_url)),
            attachments=self._kb([
                [{"type": "callback", "text": "➕ Добавить ещё кнопку", "payload": "ch_btn_add:%s:%s" % (channel_id, scope)}],
                [{"type": "callback", "text": "↩️ Назад", "payload": "ch_btn_scope:%s:%s" % (channel_id, scope)}],
                self._back(),
            ]))

    async def _show_delete_channel_buttons(self, max_user_id: str, chat_id: str, channel_id: str, scope: str) -> None:
        user = await self._db.get_user_by_max_id(max_user_id)
        if not user:
            return
        owner_user_id = await self._resolve_button_owner_id(user["id"], channel_id)
        if owner_user_id is None:
            await self._show_button_sets(max_user_id, chat_id, channel_id)
            return
        buttons_list = await self._get_scope_buttons(owner_user_id, channel_id, scope)
        scope_label, _ = self._button_scope_meta(scope)
        if not buttons_list:
            await self._reply(
                chat_id, max_user_id,
                "%s\n\nСписок кнопок пуст." % scope_label,
                attachments=self._kb([
                    [{"type": "callback", "text": "↩️ Назад", "payload": "ch_btn_scope:%s:%s" % (channel_id, scope)}],
                    self._back(),
                ]))
            return
        buttons = []
        for idx, btn in enumerate(buttons_list):
            buttons.append([{
                "type": "callback",
                "text": "❌ %s" % btn.get("text", "Кнопка"),
                "payload": "ch_btn_del:%s:%s:%s" % (channel_id, scope, idx),
            }])
        buttons.append([{"type": "callback", "text": "↩️ Назад", "payload": "ch_btn_scope:%s:%s" % (channel_id, scope)}])
        buttons.append(self._back())
        await self._reply(chat_id, max_user_id, "🗑 <b>Удаление кнопок</b>\n\n%s" % scope_label, attachments=self._kb(buttons))

    async def _delete_channel_button(self, max_user_id: str, chat_id: str, channel_id: str, scope: str, button_index: int) -> None:
        user = await self._db.get_user_by_max_id(max_user_id)
        if not user:
            return
        owner_user_id = await self._resolve_button_owner_id(user["id"], channel_id)
        if owner_user_id is None:
            await self._show_button_sets(max_user_id, chat_id, channel_id)
            return
        buttons_list = await self._get_scope_buttons(owner_user_id, channel_id, scope)
        if 0 <= button_index < len(buttons_list):
            del buttons_list[button_index]
            if scope == "default":
                await self._db.set_channel_buttons(owner_user_id, channel_id, buttons_list)
            else:
                await self._db.set_button_set(owner_user_id, channel_id, scope, buttons_list)
        await self._reply(
            chat_id, max_user_id,
            "✅ Кнопка удалена.",
            attachments=self._kb([
                [{"type": "callback", "text": "🗑 Продолжить удаление", "payload": "ch_btn_del_menu:%s:%s" % (channel_id, scope)}],
                [{"type": "callback", "text": "↩️ Назад", "payload": "ch_btn_scope:%s:%s" % (channel_id, scope)}],
                self._back(),
            ]))

    async def _clear_button_group(self, max_user_id: str, chat_id: str, channel_id: str, scope: str) -> None:
        """Показать подтверждение удаления всех кнопок."""
        scope_label, _ = self._button_scope_meta(scope)
        title = await self._client.get_chat_title(channel_id)
        await self._reply(
            chat_id, max_user_id,
            "⚠️ Удалить все кнопки для %s в канале <b>%s</b>?"
            % (scope_label, html.escape(title)),
            attachments=self._kb([
                [{"type": "callback", "text": "⚠️ Да, удалить все", "payload": "ch_btn_clear_yes:%s:%s" % (channel_id, scope)}],
                [{"type": "callback", "text": "↩️ Отмена", "payload": "ch_btn_scope:%s:%s" % (channel_id, scope)}],
                self._back(),
            ]))

    async def _clear_button_group_confirmed(self, max_user_id: str, chat_id: str, channel_id: str, scope: str) -> None:
        """Фактически удалить все кнопки после подтверждения."""
        user = await self._db.get_user_by_max_id(max_user_id)
        if not user:
            return
        owner_user_id = await self._resolve_button_owner_id(user["id"], channel_id)
        if owner_user_id is None:
            await self._show_button_sets(max_user_id, chat_id, channel_id)
            return
        if scope == "default":
            await self._db.clear_channel_buttons(owner_user_id, channel_id)
        else:
            await self._db.set_button_set(owner_user_id, channel_id, scope, [])

        scope_label, _ = self._button_scope_meta(scope)
        title = await self._client.get_chat_title(channel_id)
        await self._reply(
            chat_id, max_user_id,
            "Вы удалили все кнопки для %s в канале <b>%s</b>\n"
            "💡 Кнопки больше не будут добавляться к постам\n\n"
            "Чтобы снова включить их — добавьте кнопки заново"
            % (scope_label, html.escape(title)),
            attachments=self._kb([
                [{"type": "callback", "text": "↩️ Назад", "payload": "ch_btn_scope:%s:%s" % (channel_id, scope)}],
                self._back(),
            ]))

    # ═══════════════════════════════════════════════════════════════════════
    #  ИЗМЕНИТЬ ПОРЯДОК КНОПОК
    # ═══════════════════════════════════════════════════════════════════════

    async def _show_reorder_buttons(self, max_user_id: str, chat_id: str,
                                     channel_id: str, scope: str) -> None:
        """Показать список кнопок с возможностью перемещения вверх/вниз."""
        user = await self._db.get_user_by_max_id(max_user_id)
        if not user:
            return
        owner_user_id = await self._resolve_button_owner_id(user["id"], channel_id)
        if owner_user_id is None:
            return

        buttons_list = await self._get_scope_buttons(owner_user_id, channel_id, scope)
        scope_label, _ = self._button_scope_meta(scope)

        if len(buttons_list) < 2:
            await self._reply(
                chat_id, max_user_id,
                "ℹ️ Нужно минимум 2 кнопки для изменения порядка.",
                attachments=self._kb([
                    [{"type": "callback", "text": "↩️ Назад", "payload": "ch_btn_scope:%s:%s" % (channel_id, scope)}],
                    self._back(),
                ]))
            return

        lines = ["🔄 <b>Изменить порядок</b>\n", scope_label, ""]
        for idx, btn in enumerate(buttons_list, 1):
            lines.append("%d. %s" % (idx, html.escape(btn.get("text", ""))))

        buttons = []
        for idx in range(len(buttons_list)):
            row = []
            if idx > 0:
                row.append({
                    "type": "callback",
                    "text": "⬆️ %s" % buttons_list[idx]["text"][:20],
                    "payload": "ch_btn_swap:%s:%s:%d:%d" % (channel_id, scope, idx, idx - 1),
                })
            if idx < len(buttons_list) - 1:
                row.append({
                    "type": "callback",
                    "text": "⬇️ %s" % buttons_list[idx]["text"][:20],
                    "payload": "ch_btn_swap:%s:%s:%d:%d" % (channel_id, scope, idx, idx + 1),
                })
            if row:
                buttons.append(row)

        buttons.append([{"type": "callback", "text": "↩️ Назад", "payload": "ch_btn_scope:%s:%s" % (channel_id, scope)}])
        buttons.append(self._back())

        await self._reply(
            chat_id, max_user_id,
            "\n".join(lines),
            attachments=self._kb(buttons))

    async def _swap_buttons(self, max_user_id: str, chat_id: str,
                             channel_id: str, scope: str,
                             idx_from: int, idx_to: int) -> None:
        """Поменять местами две кнопки."""
        user = await self._db.get_user_by_max_id(max_user_id)
        if not user:
            return
        owner_user_id = await self._resolve_button_owner_id(user["id"], channel_id)
        if owner_user_id is None:
            return

        buttons_list = await self._get_scope_buttons(owner_user_id, channel_id, scope)
        if 0 <= idx_from < len(buttons_list) and 0 <= idx_to < len(buttons_list):
            buttons_list[idx_from], buttons_list[idx_to] = buttons_list[idx_to], buttons_list[idx_from]
            new_buttons = [{"text": b.get("text", ""), "url": b.get("url", "")} for b in buttons_list]
            if scope == "default":
                await self._db.set_channel_buttons(owner_user_id, channel_id, new_buttons)
            else:
                await self._db.set_button_set(owner_user_id, channel_id, scope, new_buttons)

        # Показываем обновлённый список
        await self._show_reorder_buttons(max_user_id, chat_id, channel_id, scope)

    # ═══════════════════════════════════════════════════════════════════════
    #  РАСПОЛОЖЕНИЕ КНОПОК (LAYOUT)
    # ═══════════════════════════════════════════════════════════════════════

    async def _show_layout_picker(self, max_user_id: str, chat_id: str,
                                   channel_id: str, scope: str) -> None:
        """Показать выбор расположения кнопок."""
        user = await self._db.get_user_by_max_id(max_user_id)
        if not user:
            return
        owner_user_id = await self._resolve_button_owner_id(user["id"], channel_id)
        if owner_user_id is None:
            return

        current_layout = await self._db.get_button_set_layout(owner_user_id, channel_id, scope)
        scope_label, _ = self._button_scope_meta(scope)

        options = [
            (1, "↕️ По одной в ряд"),
            (2, "↔️ По две в ряд"),
            (3, "↔️ По три в ряд"),
        ]
        buttons = []
        for val, label in options:
            mark = "✅" if val == current_layout else "◻️"
            buttons.append([{
                "type": "callback",
                "text": "%s %s" % (mark, label),
                "payload": "ch_btn_layout_set:%s:%s:%d" % (channel_id, scope, val),
            }])
        buttons.append([{"type": "callback", "text": "🔍 Вид кнопок",
                         "payload": "ch_btn_test:%s:%s" % (channel_id, scope)}])
        buttons.append([{"type": "callback", "text": "↩️ Назад",
                         "payload": "ch_btn_scope:%s:%s" % (channel_id, scope)}])
        buttons.append(self._back())

        await self._reply(
            chat_id, max_user_id,
            "📐 <b>Расположение кнопок</b>\n%s\n\n"
            "💡 Выберите вариант и нажмите 🔍 Вид кнопок для предпросмотра" % scope_label,
            attachments=self._kb(buttons))

    async def _set_button_layout(self, max_user_id: str, chat_id: str,
                                  channel_id: str, scope: str, layout: int) -> None:
        """Установить layout и вернуться к группе."""
        user = await self._db.get_user_by_max_id(max_user_id)
        if not user:
            return
        owner_user_id = await self._resolve_button_owner_id(user["id"], channel_id)
        if owner_user_id is None:
            return
        await self._db.set_button_set_layout(owner_user_id, channel_id, scope, layout)
        layout_labels = {1: "по одной ↕️", 2: "по две ↔️", 3: "по три ↔️"}
        await self._reply(
            chat_id, max_user_id,
            "✅ Расположение: <b>%s</b>" % layout_labels.get(layout, "?"))
        await self._show_button_group(max_user_id, chat_id, channel_id, scope)

    # ═══════════════════════════════════════════════════════════════════════
    #  БИБЛИОТЕКА КНОПОК (SAVED BUTTONS)
    # ═══════════════════════════════════════════════════════════════════════

    async def _show_saved_buttons(self, max_user_id: str, chat_id: str) -> None:
        """Показать библиотеку: каждая кнопка как inline callback."""
        user = await self._db.get_user_by_max_id(max_user_id)
        if not user:
            return

        saved = await self._db.get_saved_buttons(user["id"])
        if not saved:
            await self._reply(
                chat_id, max_user_id,
                "📚 <b>Мои кнопки</b>\n\n"
                "Пока пусто.\n"
                "Создайте кнопку — она сохранится и будет\n"
                "доступна при настройке любого канала.",
                attachments=self._kb([
                    [{"type": "callback", "text": "➕ Создать кнопку", "payload": "saved_btn_create"}],
                    self._back(),
                ]))
            return

        buttons = []
        for btn in saved:
            buttons.append([{
                "type": "callback",
                "text": "🔘 %s" % btn["text"][:50],
                "payload": "saved_btn_view:%d" % btn["id"],
            }])
        buttons.append([{"type": "callback", "text": "➕ Создать кнопку", "payload": "saved_btn_create"}])
        buttons.append(self._back())

        await self._reply(
            chat_id, max_user_id,
            "📚 <b>Мои кнопки (%d):</b>\n\n"
            "Нажмите на кнопку для просмотра и редактирования." % len(saved),
            attachments=self._kb(buttons))

    async def _show_saved_button_detail(self, max_user_id: str, chat_id: str,
                                         button_id: int) -> None:
        """Карточка одной кнопки: название + URL + действия."""
        user = await self._db.get_user_by_max_id(max_user_id)
        if not user:
            return
        btn = await self._db.get_saved_button_by_id(user["id"], button_id)
        if not btn:
            await self._reply(chat_id, max_user_id, "❌ Кнопка не найдена.",
                              attachments=self._kb([
                                  [{"type": "callback", "text": "📚 К списку", "payload": "menu_saved_btns"}],
                                  self._back(),
                              ]))
            return

        await self._reply(
            chat_id, max_user_id,
            "🔘 <b>%s</b>\n\n"
            "🔗 <code>%s</code>" % (html.escape(btn["text"]), html.escape(btn["url"])),
            attachments=self._kb([
                [{"type": "callback", "text": "✏️ Изменить название", "payload": "saved_btn_edit_text:%d" % button_id}],
                [{"type": "callback", "text": "🔗 Изменить ссылку", "payload": "saved_btn_edit_url:%d" % button_id}],
                [{"type": "callback", "text": "❌ Удалить", "payload": "saved_btn_del:%d" % button_id}],
                [{"type": "callback", "text": "↩️ Назад", "payload": "menu_saved_btns"}],
                self._back(),
            ]))

    async def _delete_saved_button_action(self, max_user_id: str, chat_id: str,
                                           button_id: int) -> None:
        """Удалить кнопку из библиотеки."""
        user = await self._db.get_user_by_max_id(max_user_id)
        if not user:
            return
        await self._db.delete_saved_button(user["id"], button_id)
        await self._reply(chat_id, max_user_id, "✅ Кнопка удалена.")
        await self._show_saved_buttons(max_user_id, chat_id)

    async def _start_create_saved_button(self, max_user_id: str, chat_id: str) -> None:
        """Начать создание кнопки в библиотеке."""
        self._set_pending(max_user_id, {"action": "awaiting_saved_btn_text"})
        await self._reply(
            chat_id, max_user_id,
            "➕ <b>Новая кнопка</b>\n\n"
            "Отправьте <b>название кнопки</b> одним сообщением.",
            attachments=self._kb([
                [{"type": "callback", "text": "↩️ Назад", "payload": "menu_saved_btns"}],
                self._back(),
            ]))

    async def _handle_saved_btn_text(self, max_user_id: str, chat_id: str, btn_text: str) -> None:
        """Обработать название кнопки для библиотеки."""
        if not btn_text:
            await self._reply(
                chat_id, max_user_id,
                "❌ Название не может быть пустым. Отправьте ещё раз.",
                attachments=self._kb([
                    [{"type": "callback", "text": "↩️ Назад", "payload": "menu_saved_btns"}],
                    self._back(),
                ]))
            return
        state = self._pending_states.get(max_user_id, {})
        state["action"] = "awaiting_saved_btn_url"
        state["button_text"] = btn_text[:80]
        state["_ts"] = time.monotonic()
        await self._reply(
            chat_id, max_user_id,
            "Кнопка <b>%s</b>\n\n"
            "🔗 Пришлите ссылку (URL) для этой кнопки" % html.escape(btn_text[:80]),
            attachments=self._kb([
                [{"type": "callback", "text": "↩️ Назад", "payload": "menu_saved_btns"}],
                self._back(),
            ]))

    async def _handle_saved_btn_url(self, max_user_id: str, chat_id: str, btn_url: str) -> None:
        """Обработать URL кнопки и сохранить в библиотеку."""
        state = self._pending_states.get(max_user_id, {})
        btn_text = state.get("button_text", "")
        if not btn_url or not (btn_url.startswith("http://") or btn_url.startswith("https://")):
            await self._reply(
                chat_id, max_user_id,
                "❌ Ссылка должна начинаться с <code>http://</code> или <code>https://</code>.\n"
                "Отправьте URL ещё раз.",
                attachments=self._kb([
                    [{"type": "callback", "text": "↩️ Назад", "payload": "menu_saved_btns"}],
                    self._back(),
                ]))
            return
        user = await self._db.get_user_by_max_id(max_user_id)
        if not user:
            return
        await self._db.save_button(user["id"], btn_text, btn_url)
        self._pending_states.pop(max_user_id, None)
        await self._reply(
            chat_id, max_user_id,
            "✅ <b>Кнопка сохранена!</b>\n\n"
            "<b>%s</b>\n<code>%s</code>" % (html.escape(btn_text), html.escape(btn_url)),
            attachments=self._kb([
                [{"type": "callback", "text": "➕ Ещё кнопку", "payload": "saved_btn_create"}],
                [{"type": "callback", "text": "📚 К списку", "payload": "menu_saved_btns"}],
                self._back(),
            ]))

    # ─── Редактирование кнопок ───────────────────────────────────────────

    async def _start_edit_saved_btn_text(self, max_user_id: str, chat_id: str,
                                          button_id: int) -> None:
        """Начать редактирование названия кнопки."""
        user = await self._db.get_user_by_max_id(max_user_id)
        if not user:
            return
        btn = await self._db.get_saved_button_by_id(user["id"], button_id)
        if not btn:
            return
        self._set_pending(max_user_id, {
            "action": "awaiting_saved_btn_edit_text",
            "button_id": button_id,
        })
        await self._reply(
            chat_id, max_user_id,
            "✏️ <b>Изменить название</b>\n\n"
            "Текущее: <b>%s</b>\n\n"
            "Отправьте новое название:" % html.escape(btn["text"]),
            attachments=self._kb([
                [{"type": "callback", "text": "↩️ Назад", "payload": "saved_btn_view:%d" % button_id}],
                self._back(),
            ]))

    async def _handle_saved_btn_edit_text(self, max_user_id: str, chat_id: str,
                                           new_text: str) -> None:
        """Применить новое название кнопки."""
        state = self._pending_states.get(max_user_id, {})
        button_id = state.get("button_id")
        if not button_id or not new_text:
            await self._reply(chat_id, max_user_id, "❌ Название не может быть пустым.")
            return
        user = await self._db.get_user_by_max_id(max_user_id)
        if not user:
            return
        await self._db.update_saved_button_text(user["id"], button_id, new_text[:80])
        self._pending_states.pop(max_user_id, None)
        await self._reply(chat_id, max_user_id, "✅ Название обновлено: <b>%s</b>" % html.escape(new_text[:80]))
        await self._show_saved_button_detail(max_user_id, chat_id, button_id)

    async def _start_edit_saved_btn_url(self, max_user_id: str, chat_id: str,
                                         button_id: int) -> None:
        """Начать редактирование URL кнопки."""
        user = await self._db.get_user_by_max_id(max_user_id)
        if not user:
            return
        btn = await self._db.get_saved_button_by_id(user["id"], button_id)
        if not btn:
            return
        self._set_pending(max_user_id, {
            "action": "awaiting_saved_btn_edit_url",
            "button_id": button_id,
        })
        await self._reply(
            chat_id, max_user_id,
            "🔗 <b>Изменить ссылку</b>\n\n"
            "Текущая: <code>%s</code>\n\n"
            "Отправьте новый URL:" % html.escape(btn["url"]),
            attachments=self._kb([
                [{"type": "callback", "text": "↩️ Назад", "payload": "saved_btn_view:%d" % button_id}],
                self._back(),
            ]))

    async def _handle_saved_btn_edit_url(self, max_user_id: str, chat_id: str,
                                          new_url: str) -> None:
        """Применить новый URL кнопки."""
        state = self._pending_states.get(max_user_id, {})
        button_id = state.get("button_id")
        if not button_id:
            return
        if not new_url or not (new_url.startswith("http://") or new_url.startswith("https://")):
            await self._reply(
                chat_id, max_user_id,
                "❌ Ссылка должна начинаться с <code>http://</code> или <code>https://</code>.\n"
                "Отправьте URL ещё раз.",
                attachments=self._kb([
                    [{"type": "callback", "text": "↩️ Назад", "payload": "saved_btn_view:%d" % button_id}],
                    self._back(),
                ]))
            return
        user = await self._db.get_user_by_max_id(max_user_id)
        if not user:
            return
        await self._db.update_saved_button_url(user["id"], button_id, new_url)
        self._pending_states.pop(max_user_id, None)
        await self._reply(chat_id, max_user_id, "✅ Ссылка обновлена.")
        await self._show_saved_button_detail(max_user_id, chat_id, button_id)

    # ═══════════════════════════════════════════════════════════════════════
    #  ТЕСТ КНОПОК
    # ═══════════════════════════════════════════════════════════════════════

    async def _show_test_buttons(self, max_user_id: str, chat_id: str,
                                  channel_id: str, scope: str) -> None:
        """
        Отправить тестовое сообщение с картинкой и кнопками — как его видит клиент в канале.
        """
        user = await self._db.get_user_by_max_id(max_user_id)
        if not user:
            return
        owner_user_id = await self._resolve_button_owner_id(user["id"], channel_id)
        if owner_user_id is None:
            return

        buttons_list = await self._get_scope_buttons(owner_user_id, channel_id, scope)
        layout = await self._db.get_button_set_layout(owner_user_id, channel_id, scope)
        scope_label, _ = self._button_scope_meta(scope)

        if not buttons_list:
            await self._reply(
                chat_id, max_user_id,
                "📭 Кнопки не настроены для тестирования.",
                attachments=self._kb([
                    [{"type": "callback", "text": "↩️ Назад", "payload": "ch_btn_layout:%s:%s" % (channel_id, scope)}],
                    self._back(),
                ]))
            return

        # Формируем кнопки как link-type (как в канале)
        kb = self._build_keyboard_with_layout(buttons_list, layout)

        # Загружаем тестовую картинку
        import os
        test_image_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "test_preview.png")
        image_attachment = None
        if os.path.exists(test_image_path):
            try:
                image_attachment = await self._client.upload_local_image(test_image_path)
            except Exception as e:
                logger.warning("Failed to upload test image: %s" % e)

        test_text = (
            "Ваш текст и цены автоматически добавляются к посту\n\n"
            "Кнопки будут выглядеть так 👇\n\n"
            "Для изменения вернитесь в предыдущее меню и выберите другой вариант"
        )

        # Собираем вложения: картинка (если есть) + клавиатура
        attachments = []
        if image_attachment:
            attachments.append(image_attachment)
        if kb:
            attachments.append({"type": "inline_keyboard", "payload": {"buttons": kb}})

        await self._reply(
            chat_id, max_user_id, test_text,
            attachments=attachments)

        # Навигация после тестового сообщения
        await self._client.send_message(
            chat_id,
            "⬆️ Превью кнопок выше.",
            attachments=self._kb([
                [{"type": "callback", "text": "🔄 Расположение кнопок",
                  "payload": "ch_btn_layout:%s:%s" % (channel_id, scope)}],
                [{"type": "callback", "text": "↩️ Назад",
                  "payload": "ch_btn_scope:%s:%s" % (channel_id, scope)}],
                self._back(),
            ]))

    async def _show_saved_buttons_for_scope(self, max_user_id: str, chat_id: str,
                                             channel_id: str, scope: str) -> None:
        """Показать сохранённые кнопки с галочками для вкл/выкл."""
        user = await self._db.get_user_by_max_id(max_user_id)
        if not user:
            return

        saved = await self._db.get_saved_buttons(user["id"])
        scope_label, _ = self._button_scope_meta(scope)

        if not saved:
            await self._reply(
                chat_id, max_user_id,
                "📚 <b>Библиотека пуста</b>\n\n"
                "Создайте кнопку через [➕ Добавить кнопку],\n"
                "она автоматически сохранится в библиотеку.",
                attachments=self._kb([
                    [{"type": "callback", "text": "➕ Добавить кнопку",
                      "payload": "ch_btn_add:%s:%s" % (channel_id, scope)}],
                    [{"type": "callback", "text": "↩️ Назад",
                      "payload": "ch_btn_scope:%s:%s" % (channel_id, scope)}],
                    self._back(),
                ]))
            return

        # Получаем текущие кнопки для пометки
        owner_user_id = await self._resolve_button_owner_id(user["id"], channel_id)
        current = []
        if owner_user_id is not None:
            current = await self._get_scope_buttons(owner_user_id, channel_id, scope)
        current_urls = {b.get("url", "").strip() for b in current}

        buttons = []
        for btn in saved:
            already = btn["url"].strip() in current_urls
            prefix = "✅" if already else "☐"
            buttons.append([{
                "type": "callback",
                "text": "%s %s" % (prefix, btn["text"][:40]),
                "payload": "ch_btn_toggle:%s:%s:%d" % (channel_id, scope, btn["id"]),
            }])
        buttons.append([{"type": "callback", "text": "↩️ Назад",
                         "payload": "ch_btn_scope:%s:%s" % (channel_id, scope)}])
        buttons.append(self._back())

        await self._reply(
            chat_id, max_user_id,
            "📚 <b>Выберите кнопки из библиотеки</b>\n\n%s\n\n"
            "✅ — кнопка подключена\n"
            "☐ — кнопка не подключена\n\n"
            "Нажмите чтобы включить/выключить" % scope_label,
            attachments=self._kb(buttons))

    async def _toggle_saved_button(self, max_user_id: str, chat_id: str,
                                    channel_id: str, scope: str, saved_btn_id: int) -> None:
        """Переключить кнопку из библиотеки: добавить или удалить из набора."""
        user = await self._db.get_user_by_max_id(max_user_id)
        if not user:
            return

        saved_btn = await self._db.get_saved_button_by_id(user["id"], saved_btn_id)
        if not saved_btn:
            await self._reply(chat_id, max_user_id, "❌ Кнопка не найдена")
            return

        owner_user_id = await self._resolve_button_owner_id(user["id"], channel_id)
        if owner_user_id is None:
            return

        current = await self._get_scope_buttons(owner_user_id, channel_id, scope)
        current_urls = [b.get("url", "").strip() for b in current]

        if saved_btn["url"].strip() in current_urls:
            # Удаляем кнопку из набора
            new_buttons = [
                {"text": b.get("text", ""), "url": b.get("url", "")}
                for b in current
                if b.get("url", "").strip() != saved_btn["url"].strip()
            ]
        else:
            # Добавляем кнопку в набор
            new_buttons = [{"text": b.get("text", ""), "url": b.get("url", "")} for b in current]
            new_buttons.append({"text": saved_btn["text"], "url": saved_btn["url"]})

        if scope == "default":
            await self._db.set_channel_buttons(owner_user_id, channel_id, new_buttons)
        else:
            await self._db.set_button_set(owner_user_id, channel_id, scope, new_buttons)

        # Обновляем экран
        await self._show_saved_buttons_for_scope(max_user_id, chat_id, channel_id, scope)

    async def _pick_saved_button(self, max_user_id: str, chat_id: str,
                                  channel_id: str, scope: str, saved_btn_id: int) -> None:
        """Обратная совместимость — перенаправляем на toggle."""
        await self._toggle_saved_button(max_user_id, chat_id, channel_id, scope, saved_btn_id)
