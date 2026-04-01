"""
api.py — aiohttp REST API сервер v3.0.

Новое в v3:
- GET/POST /buttons/{channel_id}/{set_type} — наборы кнопок (announcement/regular/manual)
- POST /post принимает post_type для выбора набора кнопок
- Обратная совместимость со всеми v2 эндпоинтами

Эндпоинты:
- GET  /health               — проверка работоспособности
- GET  /channels             — список каналов
- POST /post                 — отправка поста
- POST /edit                 — редактирование поста (24ч)
- POST /delete               — удаление поста
- POST /delete-announcement  — удаление анонса
- GET  /buttons              — глобальные кнопки
- POST /buttons              — задать глобальные кнопки
- GET  /buttons/{channel_id} — кнопки канала
- POST /buttons/{channel_id} — задать кнопки канала
- DELETE /buttons/{channel_id} — удалить кнопки канала
- GET  /buttons/{channel_id}/{set_type} — набор кнопок
- POST /buttons/{channel_id}/{set_type} — задать набор кнопок
"""

import asyncio
import json as _json
import secrets

from aiohttp import web
from loguru import logger

from config import config
from database import Database
from max_client import MaxClient, MaxAPIError


async def create_api_server(
    db: Database, client: MaxClient, port: int, heartbeat=None
) -> None:

    PUBLIC_PATHS = {"/health"}

    @web.middleware
    async def auth_middleware(request: web.Request, handler) -> web.Response:
        if request.path in PUBLIC_PATHS:
            return await handler(request)

        auth_header = request.headers.get("Authorization", "")
        if not auth_header.startswith("Bearer "):
            return web.json_response({"error": "Unauthorized"}, status=401)

        token = auth_header[7:]
        user = await db.get_user_by_token(token)
        if not user:
            return web.json_response({"error": "Unauthorized"}, status=401)

        request["user"] = user

        # Если пользователь ещё не привязан к Telegram — привязываем.
        # Любой API-запрос с валидным токеном = Telegram-бот знает токен = связка есть.
        if not user.get("telegram_linked_at"):
            await db.set_telegram_linked(user["max_user_id"])

        return await handler(request)

    def _clean_buttons(raw_buttons) -> list[dict]:
        if raw_buttons is None:
            return []
        if not isinstance(raw_buttons, list):
            raise ValueError("buttons must be an array")
        buttons = []
        for btn in raw_buttons:
            if not isinstance(btn, dict):
                continue
            t = str(btn.get("text", "")).strip()
            u = str(btn.get("url", "")).strip()
            if t and u:
                buttons.append({"text": t, "url": u})
        return buttons

    async def _resolve_button_owner_or_403(user: dict, channel_id: str) -> int | None:
        owner_user_id = await db.resolve_button_owner_id(channel_id, user["id"], require_manage_buttons=True)
        if owner_user_id is None:
            return None
        return owner_user_id

    # ─── Health ───────────────────────────────────────────────────────────

    async def health(request: web.Request) -> web.Response:
        if heartbeat:
            heartbeat.ping("api")
        result = {"status": "ok", "version": "3.0"}
        if heartbeat:
            result["uptime_seconds"] = heartbeat.uptime_seconds
            result["components"] = heartbeat.get_status()
            stale = heartbeat.check_stale(120)
            if stale:
                result["status"] = "degraded"
                result["stale_components"] = stale
                return web.json_response(result, status=503)
        return web.json_response(result)

    # ─── Channels ─────────────────────────────────────────────────────────

    async def get_channels(request: web.Request) -> web.Response:
        user = request["user"]
        channels = await db.get_channels(user["id"])
        return web.json_response({
            "channels": [
                {"id": ch["channel_id"], "title": ch["title"],
                 "bot_is_admin": bool(ch.get("bot_is_admin"))}
                for ch in channels
            ]
        })


    async def _resolve_button_owner(
        channel_id: str, acting_user_id: int, require_manage_buttons: bool = False
    ) -> int | None:
        return await db.resolve_button_owner_id(
            channel_id, acting_user_id, require_manage_buttons=require_manage_buttons
        )

    async def _require_button_owner(
        request: web.Request, channel_id: str, require_manage_buttons: bool = False
    ) -> tuple[int | None, web.Response | None]:
        user = request["user"]
        owner_id = await _resolve_button_owner(channel_id, user["id"], require_manage_buttons)
        if owner_id is None:
            return None, web.json_response({"ok": False, "error": "Channel not found"}, status=404)
        return owner_id, None

    # ─── Buttons (глобальные) ─────────────────────────────────────────────

    async def get_user_buttons(request: web.Request) -> web.Response:
        user = request["user"]
        buttons = await db.get_user_buttons(user["id"])
        return web.json_response({
            "buttons": [{"text": b["text"], "url": b["url"]} for b in buttons]
        })

    async def set_user_buttons(request: web.Request) -> web.Response:
        user = request["user"]
        try:
            body = await request.json()
        except Exception:
            return web.json_response({"ok": False, "error": "Invalid JSON"}, status=400)

        try:
            buttons = _clean_buttons(body.get("buttons", []))
        except ValueError as e:
            return web.json_response({"ok": False, "error": str(e)}, status=400)

        await db.set_user_buttons(user["id"], buttons)
        return web.json_response({"ok": True, "buttons": buttons})

    # ─── Buttons (канала) ────────────────────────────────────────────────

    async def get_ch_buttons(request: web.Request) -> web.Response:
        user = request["user"]
        channel_id = request.match_info["channel_id"]
        owner_user_id = await _resolve_button_owner_or_403(user, channel_id)
        if owner_user_id is None:
            return web.json_response({"ok": False, "error": "Channel not found or access denied"}, status=403)
        buttons = await db.get_channel_buttons(owner_user_id, channel_id)
        return web.json_response({
            "buttons": [{"text": b["text"], "url": b["url"]} for b in buttons]
        })

    async def set_ch_buttons(request: web.Request) -> web.Response:
        user = request["user"]
        channel_id = request.match_info["channel_id"]
        owner_user_id = await _resolve_button_owner_or_403(user, channel_id)
        if owner_user_id is None:
            return web.json_response({"ok": False, "error": "Channel not found or access denied"}, status=403)
        try:
            body = await request.json()
        except Exception:
            return web.json_response({"ok": False, "error": "Invalid JSON"}, status=400)

        try:
            buttons = _clean_buttons(body.get("buttons", []))
        except ValueError as e:
            return web.json_response({"ok": False, "error": str(e)}, status=400)

        await db.set_channel_buttons(owner_user_id, channel_id, buttons)
        return web.json_response({"ok": True, "buttons": buttons})

    async def delete_ch_buttons(request: web.Request) -> web.Response:
        user = request["user"]
        channel_id = request.match_info["channel_id"]
        owner_user_id = await _resolve_button_owner_or_403(user, channel_id)
        if owner_user_id is None:
            return web.json_response({"ok": False, "error": "Channel not found or access denied"}, status=403)
        await db.clear_channel_buttons(owner_user_id, channel_id)
        return web.json_response({"ok": True, "message": "Channel buttons removed"})

    # ─── Button Sets (наборы кнопок) ─────────────────────────────────────

    VALID_SET_TYPES = {"announcement", "regular", "manual"}

    async def get_button_set(request: web.Request) -> web.Response:
        user = request["user"]
        channel_id = request.match_info["channel_id"]
        set_type = request.match_info["set_type"]

        if set_type not in VALID_SET_TYPES:
            return web.json_response(
                {"ok": False, "error": "set_type must be: announcement, regular, manual"},
                status=400)

        owner_user_id = await _resolve_button_owner_or_403(user, channel_id)
        if owner_user_id is None:
            return web.json_response({"ok": False, "error": "Channel not found or access denied"}, status=403)
        buttons = await db.get_button_set(owner_user_id, channel_id, set_type)
        return web.json_response({"set_type": set_type, "buttons": buttons})

    async def set_button_set(request: web.Request) -> web.Response:
        user = request["user"]
        channel_id = request.match_info["channel_id"]
        set_type = request.match_info["set_type"]

        if set_type not in VALID_SET_TYPES:
            return web.json_response(
                {"ok": False, "error": "set_type must be: announcement, regular, manual"},
                status=400)

        owner_user_id = await _resolve_button_owner_or_403(user, channel_id)
        if owner_user_id is None:
            return web.json_response({"ok": False, "error": "Channel not found or access denied"}, status=403)
        try:
            body = await request.json()
        except Exception:
            return web.json_response({"ok": False, "error": "Invalid JSON"}, status=400)

        try:
            buttons = _clean_buttons(body.get("buttons", []))
        except ValueError as e:
            return web.json_response({"ok": False, "error": str(e)}, status=400)

        await db.set_button_set(owner_user_id, channel_id, set_type, buttons)
        return web.json_response({"ok": True, "set_type": set_type, "buttons": buttons})

    async def get_all_button_sets(request: web.Request) -> web.Response:
        """Получить все наборы кнопок канала одним запросом."""
        user = request["user"]
        channel_id = request.match_info["channel_id"]
        owner_user_id = await _resolve_button_owner_or_403(user, channel_id)
        if owner_user_id is None:
            return web.json_response({"ok": False, "error": "Channel not found or access denied"}, status=403)
        sets = await db.get_all_button_sets(owner_user_id, channel_id)
        default_buttons = await db.get_channel_buttons(owner_user_id, channel_id)
        return web.json_response({
            "channel_id": channel_id,
            "default": [{"text": b["text"], "url": b["url"]} for b in default_buttons],
            "sets": sets,
        })

    # ─── Post ─────────────────────────────────────────────────────────────

    async def post_message(request: web.Request) -> web.Response:
        user = request["user"]

        try:
            body = await request.json()
        except Exception:
            return web.json_response({"ok": False, "error": "Invalid JSON"}, status=400)

        channel_id = str(body.get("channel_id", "")).strip()
        text = str(body.get("text", "")).strip()
        has_price = bool(body.get("has_price", False))
        raw_buttons = body.get("buttons")
        order_url = str(body.get("order_url", "")).strip() or None
        announcement_name = str(body.get("announcement_name", "")).strip() or None
        # post_type для выбора набора кнопок: announcement, regular, manual, all
        post_type = str(body.get("post_type", "")).strip() or "all"

        # media_urls
        raw_media = body.get("media_urls")
        if isinstance(raw_media, list):
            media_urls = [str(u).strip() for u in raw_media if str(u).strip()]
        else:
            single = str(body.get("media_url", "")).strip()
            media_urls = [single] if single else []

        if not channel_id:
            return web.json_response({"ok": False, "error": "channel_id is required"}, status=400)
        if not text and not media_urls:
            return web.json_response({"ok": False, "error": "text or media_urls is required"}, status=400)
        if not await db.channel_belongs_to_user(channel_id, user["id"]):
            # Проверяем как помощник
            if not await db.user_can_post_to_channel(channel_id, user["id"]):
                return web.json_response({"ok": False, "error": "Channel not found"}, status=400)

        # Медиа — загрузка
        attachments = []
        for url in media_urls:
            try:
                attachments.append(await client.upload_media(url))
            except Exception as e:
                logger.error("Media upload failed for %s: %s" % (url, e))
                return web.json_response(
                    {"ok": False, "error": "Media upload failed: %s" % e}, status=400)

        # Кнопки — приоритет: запрос → button_set → канал → глобальные
        keyboard_buttons = []
        if isinstance(raw_buttons, list) and raw_buttons:
            for btn in raw_buttons:
                t = str(btn.get("text", "")).strip()
                u = str(btn.get("url", "")).strip()
                if t and u:
                    keyboard_buttons.append([{"type": "link", "text": t, "url": u}])
        elif order_url:
            keyboard_buttons.append([{"type": "link", "text": "🛍️ Заказать", "url": order_url}])
        else:
            # Определяем тип поста для выбора набора кнопок
            effective_type = post_type
            if effective_type == "all":
                # Автоопределение: если есть announcement_name → announcement
                if announcement_name:
                    effective_type = "announcement"
                elif has_price:
                    effective_type = "announcement"

            effective_owner_id = await db.resolve_button_owner_id(channel_id, user["id"], require_manage_buttons=False) or user["id"]
            fixed = await db.get_effective_buttons(effective_owner_id, channel_id, effective_type)
            if fixed:
                layout = await db.get_effective_layout(effective_owner_id, channel_id, effective_type)
                layout = max(1, min(3, layout))
                row = []
                for btn in fixed:
                    row.append({"type": "link", "text": btn["text"], "url": btn["url"]})
                    if len(row) >= layout:
                        keyboard_buttons.append(row)
                        row = []
                if row:
                    keyboard_buttons.append(row)

        if keyboard_buttons:
            attachments.append({"type": "inline_keyboard", "payload": {"buttons": keyboard_buttons}})

        # Кнопка комментариев (deeplink)
        api_comment_key = ""
        if config.bot_link:
            api_comment_key = secrets.token_urlsafe(8)
            await db.create_comment_link(
                api_comment_key, channel_id, user["id"],
                (text or "")[:200])
            comment_url = "%s?start=c_%s" % (config.bot_link, api_comment_key)
            comment_btn_row = [{"type": "link", "text": "💬 Комментарии", "url": comment_url}]
            # Добавляем в существующую клавиатуру или создаём новую
            found_kb = False
            for att in attachments:
                if isinstance(att, dict) and att.get("type") == "inline_keyboard":
                    att["payload"]["buttons"].append(comment_btn_row)
                    found_kb = True
                    break
            if not found_kb:
                attachments.append({"type": "inline_keyboard", "payload": {"buttons": [comment_btn_row]}})

        media_urls_json = _json.dumps(media_urls) if media_urls else None
        buttons_json = _json.dumps(raw_buttons) if isinstance(raw_buttons, list) else None

        try:
            result = await client.send_message(channel_id, text or "", attachments=attachments or None)
            max_msg_id = str(result.get("message", {}).get("body", {}).get("mid", ""))

            # Обновляем comment_link с msg_id
            if api_comment_key and max_msg_id:
                await db.update_comment_link_msg_id(api_comment_key, max_msg_id)

            await db.save_post(
                user_id=user["id"], channel_id=channel_id, text=text,
                media_urls=media_urls_json, buttons_json=buttons_json,
                order_url=order_url, has_price=has_price,
                announcement_name=announcement_name, max_msg_id=max_msg_id, status="sent",
            )
            return web.json_response({"ok": True, "message_id": max_msg_id,
                                       "comment_key": api_comment_key or None})

        except MaxAPIError as e:
            await db.save_post(
                user_id=user["id"], channel_id=channel_id, text=text,
                media_urls=media_urls_json, buttons_json=buttons_json,
                order_url=order_url, has_price=has_price,
                announcement_name=announcement_name, max_msg_id=None,
                status="error", error_text=str(e),
            )
            logger.error("MAX API error on /post: %s" % e)
            return web.json_response({"ok": False, "error": "MAX API error: %s" % e}, status=502)

    # ─── Edit ─────────────────────────────────────────────────────────────

    async def edit_message(request: web.Request) -> web.Response:
        user = request["user"]
        try:
            body = await request.json()
        except Exception:
            return web.json_response({"ok": False, "error": "Invalid JSON"}, status=400)

        message_id = str(body.get("message_id", "")).strip()
        text = str(body.get("text", "")).strip()

        if not message_id:
            return web.json_response({"ok": False, "error": "message_id is required"}, status=400)

        post = await db.get_post_by_msg_id(message_id, user["id"])
        if not post:
            return web.json_response({"ok": False, "error": "Post not found"}, status=404)

        try:
            edit_body = {}
            if text:
                edit_body["text"] = text
                edit_body["format"] = "html"

            raw_buttons = body.get("buttons")
            if isinstance(raw_buttons, list):
                kb = []
                for btn in raw_buttons:
                    t = str(btn.get("text", "")).strip()
                    u = str(btn.get("url", "")).strip()
                    if t and u:
                        kb.append([{"type": "link", "text": t, "url": u}])
                if kb:
                    edit_body["attachments"] = [{"type": "inline_keyboard", "payload": {"buttons": kb}}]

            result = await client.edit_message(message_id, text=text if text else None,
                                                attachments=edit_body.get("attachments"))
            return web.json_response({"ok": True, "message": "Updated"})
        except MaxAPIError as e:
            logger.error("Edit failed: %s" % e)
            return web.json_response({"ok": False, "error": str(e)}, status=502)

    # ─── Delete ───────────────────────────────────────────────────────────

    async def delete_message(request: web.Request) -> web.Response:
        user = request["user"]
        try:
            body = await request.json()
        except Exception:
            return web.json_response({"ok": False, "error": "Invalid JSON"}, status=400)

        message_id = str(body.get("message_id", "")).strip()
        if not message_id:
            return web.json_response({"ok": False, "error": "message_id is required"}, status=400)

        post = await db.get_post_by_msg_id(message_id, user["id"])
        if not post:
            return web.json_response({"ok": False, "error": "Post not found"}, status=404)

        try:
            await client.delete_message(message_id)
            await db.mark_post_deleted(message_id)
            return web.json_response({"ok": True, "message": "Deleted"})
        except Exception as e:
            logger.error("Delete failed: %s" % e)
            return web.json_response({"ok": False, "error": str(e)}, status=502)

    # ─── Delete Announcement ──────────────────────────────────────────────

    async def delete_announcement(request: web.Request) -> web.Response:
        user = request["user"]
        try:
            body = await request.json()
        except Exception:
            return web.json_response({"ok": False, "error": "Invalid JSON"}, status=400)

        channel_id = str(body.get("channel_id", "")).strip()
        ann_name = str(body.get("announcement_name", "")).strip()

        if not channel_id or not ann_name:
            return web.json_response(
                {"ok": False, "error": "channel_id and announcement_name are required"}, status=400)

        posts = await db.get_posts_by_announcement(user["id"], channel_id, ann_name)
        if not posts:
            return web.json_response({"ok": False, "error": "Posts not found"}, status=404)

        deleted = 0
        errors = 0

        for post in posts:
            msg_id = post.get("max_msg_id")
            if not msg_id:
                continue
            try:
                await client.delete_message(msg_id)
                await db.mark_post_deleted(msg_id)
                deleted += 1
                await asyncio.sleep(0.05)
            except Exception as e:
                logger.error("Delete announcement post %s: %s" % (msg_id, e))
                errors += 1

        return web.json_response({
            "ok": True, "deleted": deleted, "errors": errors, "total": len(posts),
        })

    # ─── Routes ───────────────────────────────────────────────────────────

    app = web.Application(middlewares=[auth_middleware])

    # Public
    app.router.add_get("/health", health)

    # Channels
    app.router.add_get("/channels", get_channels)

    # Buttons (глобальные)
    app.router.add_get("/buttons", get_user_buttons)
    app.router.add_post("/buttons", set_user_buttons)

    # Buttons (канала)
    app.router.add_get("/buttons/{channel_id}", get_ch_buttons)
    app.router.add_post("/buttons/{channel_id}", set_ch_buttons)
    app.router.add_delete("/buttons/{channel_id}", delete_ch_buttons)

    # Button Sets (наборы кнопок)
    app.router.add_get("/buttons/{channel_id}/sets", get_all_button_sets)
    app.router.add_get("/buttons/{channel_id}/{set_type}", get_button_set)
    app.router.add_post("/buttons/{channel_id}/{set_type}", set_button_set)

    # Posts
    app.router.add_post("/post", post_message)
    app.router.add_post("/edit", edit_message)
    app.router.add_post("/delete", delete_message)
    app.router.add_post("/delete-announcement", delete_announcement)

    runner = web.AppRunner(app)
    try:
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", port)
        await site.start()
        logger.info("REST API v3.0 started on port %s" % port)
        await asyncio.Event().wait()
    finally:
        await runner.cleanup()
