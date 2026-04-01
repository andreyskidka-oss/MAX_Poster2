"""
database.py — все операции с SQLite через aiosqlite.

Таблицы:
- users: зарегистрированные пользователи и их API-токены
- channels: каналы пользователей (active=1/0, bot_is_admin=1/0)
- user_buttons: глобальные фиксированные кнопки пользователя
- channel_buttons: кнопки для конкретного канала
- button_sets: три набора кнопок на канал (announcement/regular/manual)
- button_templates: сохранённые шаблоны кнопок
- helpers: помощники каналов с правами
- posts: история отправленных постов
- admins: администраторы системы
"""

import json
from datetime import datetime, timedelta

import aiosqlite
from loguru import logger


class Database:
    def __init__(self, db_path: str):
        self._db_path = db_path
        self._db: aiosqlite.Connection | None = None

    async def init(self) -> None:
        self._db = await aiosqlite.connect(self._db_path)
        self._db.row_factory = aiosqlite.Row

        # WAL mode — лучше для конкурентного доступа (API + бот одновременно)
        await self._db.execute("PRAGMA journal_mode=WAL")
        await self._db.execute("PRAGMA busy_timeout=5000")

        await self._db.executescript("""
            CREATE TABLE IF NOT EXISTS users (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                max_user_id  TEXT UNIQUE NOT NULL,
                chat_id      TEXT NOT NULL DEFAULT '',
                token        TEXT UNIQUE NOT NULL,
                created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS channels (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id      INTEGER NOT NULL REFERENCES users(id),
                channel_id   TEXT NOT NULL,
                title        TEXT NOT NULL DEFAULT '',
                active       INTEGER NOT NULL DEFAULT 1,
                bot_is_admin INTEGER NOT NULL DEFAULT 0,
                added_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(user_id, channel_id)
            );

            CREATE TABLE IF NOT EXISTS user_buttons (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id      INTEGER NOT NULL REFERENCES users(id),
                text         TEXT NOT NULL,
                url          TEXT NOT NULL,
                position     INTEGER NOT NULL DEFAULT 0,
                UNIQUE(user_id, text, url)
            );

            CREATE TABLE IF NOT EXISTS channel_buttons (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id      INTEGER NOT NULL REFERENCES users(id),
                channel_id   TEXT NOT NULL,
                text         TEXT NOT NULL,
                url          TEXT NOT NULL,
                position     INTEGER NOT NULL DEFAULT 0,
                UNIQUE(user_id, channel_id, text, url)
            );

            CREATE TABLE IF NOT EXISTS button_sets (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id      INTEGER NOT NULL REFERENCES users(id),
                channel_id   TEXT NOT NULL,
                set_type     TEXT NOT NULL CHECK(set_type IN ('announcement','regular','manual')),
                buttons_json TEXT NOT NULL DEFAULT '[]',
                layout       INTEGER NOT NULL DEFAULT 1,
                UNIQUE(user_id, channel_id, set_type)
            );

            CREATE TABLE IF NOT EXISTS button_templates (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id      INTEGER NOT NULL REFERENCES users(id),
                name         TEXT NOT NULL,
                buttons_json TEXT NOT NULL DEFAULT '[]',
                created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(user_id, name)
            );

            CREATE TABLE IF NOT EXISTS saved_buttons (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id      INTEGER NOT NULL REFERENCES users(id),
                text         TEXT NOT NULL,
                url          TEXT NOT NULL,
                created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(user_id, text, url)
            );

            CREATE TABLE IF NOT EXISTS helpers (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_id      TEXT NOT NULL,
                owner_user_id   INTEGER NOT NULL REFERENCES users(id),
                helper_user_id  INTEGER NOT NULL REFERENCES users(id),
                can_post        INTEGER NOT NULL DEFAULT 1,
                can_manage_buttons INTEGER NOT NULL DEFAULT 0,
                alias           TEXT,
                added_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(channel_id, helper_user_id)
            );

            CREATE TABLE IF NOT EXISTS posts (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id           INTEGER NOT NULL REFERENCES users(id),
                channel_id        TEXT NOT NULL,
                text              TEXT,
                media_urls        TEXT,
                buttons_json      TEXT,
                order_url         TEXT,
                has_price         INTEGER NOT NULL DEFAULT 0,
                announcement_name TEXT,
                max_msg_id        TEXT,
                status            TEXT NOT NULL DEFAULT 'sent'
                                  CHECK(status IN ('sent','error','deleted')),
                error_text        TEXT,
                published_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS admins (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                max_user_id  TEXT UNIQUE NOT NULL,
                added_by     TEXT,
                added_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        await self._db.commit()
        await self._run_migrations()
        logger.info("Таблицы БД созданы/проверены")

    async def _run_migrations(self) -> None:
        migrations = [
            ("users", "chat_id", "TEXT NOT NULL DEFAULT ''"),
            ("posts", "buttons_json", "TEXT"),
            ("posts", "has_price", "INTEGER NOT NULL DEFAULT 0"),
            ("posts", "announcement_name", "TEXT"),
            ("channels", "bot_is_admin", "INTEGER NOT NULL DEFAULT 0"),
            ("channels", "default_layout", "INTEGER NOT NULL DEFAULT 1"),
            ("button_sets", "layout", "INTEGER NOT NULL DEFAULT 1"),
            # Тарифная блокировка
            ("users", "is_active", "INTEGER NOT NULL DEFAULT 1"),
            ("users", "telegram_linked_at", "TEXT"),
            ("users", "reminder_count", "INTEGER NOT NULL DEFAULT 0"),
        ]
        for table, column, col_type in migrations:
            try:
                await self._db.execute("SELECT %s FROM %s LIMIT 1" % (column, table))
            except Exception:
                await self._db.execute(
                    "ALTER TABLE %s ADD COLUMN %s %s" % (table, column, col_type)
                )
                await self._db.commit()
                logger.info("Миграция: %s.%s" % (table, column))

        new_tables = [
            """CREATE TABLE IF NOT EXISTS button_sets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL, channel_id TEXT NOT NULL,
                set_type TEXT NOT NULL CHECK(set_type IN ('announcement','regular','manual')),
                buttons_json TEXT NOT NULL DEFAULT '[]',
                layout INTEGER NOT NULL DEFAULT 1,
                UNIQUE(user_id, channel_id, set_type))""",
            """CREATE TABLE IF NOT EXISTS button_templates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL, name TEXT NOT NULL,
                buttons_json TEXT NOT NULL DEFAULT '[]',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(user_id, name))""",
            """CREATE TABLE IF NOT EXISTS helpers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_id TEXT NOT NULL, owner_user_id INTEGER NOT NULL,
                helper_user_id INTEGER NOT NULL,
                can_post INTEGER NOT NULL DEFAULT 1,
                can_manage_buttons INTEGER NOT NULL DEFAULT 0,
                alias TEXT, added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(channel_id, helper_user_id))""",
            """CREATE TABLE IF NOT EXISTS saved_buttons (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL, text TEXT NOT NULL,
                url TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(user_id, text, url))""",
            """CREATE TABLE IF NOT EXISTS scheduled_posts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                post_type TEXT NOT NULL DEFAULT 'post'
                    CHECK(post_type IN ('post','pin')),
                channels_json TEXT NOT NULL DEFAULT '[]',
                text TEXT,
                markup_json TEXT,
                media_json TEXT DEFAULT '[]',
                buttons_json TEXT,
                scheduled_at TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending'
                    CHECK(status IN ('pending','sent','error','cancelled')),
                sent_at TEXT,
                error_text TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""",
            """CREATE TABLE IF NOT EXISTS pin_links (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                channel_id TEXT NOT NULL,
                button_set_type TEXT NOT NULL,
                button_index INTEGER NOT NULL,
                linked INTEGER NOT NULL DEFAULT 0,
                last_pin_url TEXT,
                UNIQUE(user_id, channel_id, button_set_type, button_index))""",
            """CREATE TABLE IF NOT EXISTS comment_links (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                comment_key TEXT UNIQUE NOT NULL,
                max_msg_id TEXT,
                channel_id TEXT NOT NULL,
                owner_user_id INTEGER,
                post_text_preview TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""",
            """CREATE TABLE IF NOT EXISTS comments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                comment_key TEXT NOT NULL,
                commenter_max_user_id TEXT NOT NULL,
                commenter_name TEXT NOT NULL DEFAULT '',
                text TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""",
        ]
        for sql in new_tables:
            await self._db.execute(sql)
        await self._db.commit()

    async def seed_admins(self, admin_ids: list[str]) -> None:
        for aid in admin_ids:
            await self._db.execute(
                "INSERT OR IGNORE INTO admins (max_user_id, added_by) VALUES (?, ?)",
                (aid, "system"),
            )
        await self._db.commit()

    # ─── Users ────────────────────────────────────────────────────────────

    async def get_user_by_max_id(self, max_user_id: str) -> dict | None:
        cursor = await self._db.execute(
            "SELECT * FROM users WHERE max_user_id = ?", (max_user_id,))
        row = await cursor.fetchone()
        return dict(row) if row else None

    async def get_user_by_token(self, token: str) -> dict | None:
        cursor = await self._db.execute(
            "SELECT * FROM users WHERE token = ?", (token,))
        row = await cursor.fetchone()
        return dict(row) if row else None

    async def create_user(self, max_user_id: str, chat_id: str, token: str) -> dict:
        await self._db.execute(
            "INSERT INTO users (max_user_id, chat_id, token) VALUES (?, ?, ?)",
            (max_user_id, chat_id, token))
        await self._db.commit()
        return await self.get_user_by_max_id(max_user_id)

    async def update_chat_id(self, max_user_id: str, chat_id: str) -> None:
        await self._db.execute(
            "UPDATE users SET chat_id = ? WHERE max_user_id = ?",
            (chat_id, max_user_id))
        await self._db.commit()

    async def update_user_token(self, max_user_id: str, new_token: str) -> None:
        await self._db.execute(
            "UPDATE users SET token = ? WHERE max_user_id = ?",
            (new_token, max_user_id))
        await self._db.commit()

    # ─── Channels ─────────────────────────────────────────────────────────

    async def add_channel(self, user_id: int, channel_id: str, title: str,
                          bot_is_admin: bool = False) -> None:
        await self._db.execute(
            """INSERT INTO channels (user_id, channel_id, title, active, bot_is_admin)
               VALUES (?, ?, ?, 1, ?)
               ON CONFLICT(user_id, channel_id)
               DO UPDATE SET title = excluded.title, active = 1,
                             bot_is_admin = excluded.bot_is_admin""",
            (user_id, channel_id, title, 1 if bot_is_admin else 0))
        await self._db.commit()

    async def update_bot_admin_status(self, channel_id: str, is_admin: bool) -> None:
        await self._db.execute(
            "UPDATE channels SET bot_is_admin = ? WHERE channel_id = ? AND active = 1",
            (1 if is_admin else 0, channel_id))
        await self._db.commit()

    async def deactivate_channel(self, channel_id: str) -> None:
        await self._db.execute(
            "UPDATE channels SET active = 0 WHERE channel_id = ?", (channel_id,))
        await self._db.commit()

    async def get_channels(self, user_id: int) -> list[dict]:
        cursor = await self._db.execute(
            "SELECT * FROM channels WHERE user_id = ? AND active = 1", (user_id,))
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    async def get_all_active_channels(self) -> list[dict]:
        """Все активные каналы (уникальные channel_id) с user_id и bot_is_admin."""
        cursor = await self._db.execute(
            """SELECT c.channel_id, c.title, c.bot_is_admin, c.user_id, u.max_user_id, u.chat_id
               FROM channels c JOIN users u ON u.id = c.user_id
               WHERE c.active = 1
               GROUP BY c.channel_id""")
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    async def channel_belongs_to_user(self, channel_id: str, user_id: int) -> bool:
        cursor = await self._db.execute(
            "SELECT 1 FROM channels WHERE channel_id = ? AND user_id = ? AND active = 1",
            (channel_id, user_id))
        return await cursor.fetchone() is not None

    async def get_users_by_channel(self, channel_id: str) -> list[dict]:
        cursor = await self._db.execute(
            """SELECT u.* FROM users u
               JOIN channels c ON c.user_id = u.id
               WHERE c.channel_id = ? AND c.active = 1""",
            (channel_id,))
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    async def user_can_post_to_channel(self, channel_id: str, user_id: int) -> bool:
        if await self.channel_belongs_to_user(channel_id, user_id):
            return True
        cursor = await self._db.execute(
            "SELECT 1 FROM helpers WHERE channel_id = ? AND helper_user_id = ? AND can_post = 1",
            (channel_id, user_id))
        return await cursor.fetchone() is not None

    async def get_accessible_channels(self, user_id: int) -> list[dict]:
        """Все каналы: собственные + как помощник."""
        cursor = await self._db.execute(
            """SELECT DISTINCT c.* FROM channels c
               LEFT JOIN helpers h ON h.channel_id = c.channel_id AND h.helper_user_id = ?
               WHERE c.active = 1 AND (c.user_id = ? OR h.helper_user_id IS NOT NULL)""",
            (user_id, user_id))
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    # ─── Helpers ──────────────────────────────────────────────────────────

    async def add_helper(self, channel_id: str, owner_user_id: int,
                         helper_user_id: int, can_post: bool = True,
                         can_manage_buttons: bool = False, alias: str | None = None) -> None:
        await self._db.execute(
            """INSERT INTO helpers
               (channel_id, owner_user_id, helper_user_id, can_post, can_manage_buttons, alias)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(channel_id, helper_user_id)
               DO UPDATE SET can_post = excluded.can_post,
                             can_manage_buttons = excluded.can_manage_buttons,
                             alias = COALESCE(excluded.alias, helpers.alias)""",
            (channel_id, owner_user_id, helper_user_id,
             1 if can_post else 0, 1 if can_manage_buttons else 0, alias))
        await self._db.commit()

    async def remove_helper(self, channel_id: str, helper_user_id: int) -> None:
        await self._db.execute(
            "DELETE FROM helpers WHERE channel_id = ? AND helper_user_id = ?",
            (channel_id, helper_user_id))
        await self._db.commit()

    async def update_helper_alias(self, channel_id: str, helper_user_id: int,
                                  alias: str) -> None:
        await self._db.execute(
            "UPDATE helpers SET alias = ? WHERE channel_id = ? AND helper_user_id = ?",
            (alias, channel_id, helper_user_id))
        await self._db.commit()

    async def get_helpers(self, channel_id: str) -> list[dict]:
        cursor = await self._db.execute(
            """SELECT h.*, u.max_user_id as helper_max_id
               FROM helpers h JOIN users u ON u.id = h.helper_user_id
               WHERE h.channel_id = ? ORDER BY h.added_at""",
            (channel_id,))
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    async def can_manage_buttons(self, channel_id: str, user_id: int) -> bool:
        cursor = await self._db.execute(
            "SELECT 1 FROM channels WHERE channel_id = ? AND user_id = ? AND active = 1",
            (channel_id, user_id))
        if await cursor.fetchone():
            return True
        cursor = await self._db.execute(
            "SELECT 1 FROM helpers WHERE channel_id = ? AND helper_user_id = ? AND can_manage_buttons = 1",
            (channel_id, user_id))
        return await cursor.fetchone() is not None

    async def get_channel_owner_id(self, channel_id: str) -> int | None:
        cursor = await self._db.execute(
            "SELECT user_id FROM channels WHERE channel_id = ? AND active = 1 ORDER BY id LIMIT 1",
            (channel_id,))
        row = await cursor.fetchone()
        return int(row[0]) if row else None

    async def resolve_button_owner_id(
        self, channel_id: str, acting_user_id: int, require_manage_buttons: bool = False
    ) -> int | None:
        """
        Вернуть owner user_id, под которым должны храниться кнопки канала.
        Для владельца возвращается его user_id. Для помощника — owner_user_id,
        если у него есть соответствующие права.
        """
        cursor = await self._db.execute(
            "SELECT user_id FROM channels WHERE channel_id = ? AND user_id = ? AND active = 1",
            (channel_id, acting_user_id),
        )
        row = await cursor.fetchone()
        if row:
            return int(row[0])

        if require_manage_buttons:
            cursor = await self._db.execute(
                """SELECT owner_user_id FROM helpers
                   WHERE channel_id = ? AND helper_user_id = ? AND can_manage_buttons = 1""",
                (channel_id, acting_user_id),
            )
        else:
            cursor = await self._db.execute(
                """SELECT owner_user_id FROM helpers
                   WHERE channel_id = ? AND helper_user_id = ?""",
                (channel_id, acting_user_id),
            )
        row = await cursor.fetchone()
        return int(row[0]) if row else None

    async def get_button_scope_counts(self, user_id: int, channel_id: str) -> dict[str, int]:
        sets = await self.get_all_button_sets(user_id, channel_id)
        default_buttons = await self.get_channel_buttons(user_id, channel_id)
        return {
            "announcement": len(sets.get("announcement", [])),
            "regular": len(sets.get("regular", [])),
            "manual": len(sets.get("manual", [])),
            "default": len(default_buttons),
        }


    # ─── User Buttons ────────────────────────────────────────────────────

    async def get_user_buttons(self, user_id: int) -> list[dict]:
        """Глобальные кнопки пользователя из таблицы user_buttons."""
        cursor = await self._db.execute(
            "SELECT * FROM user_buttons WHERE user_id = ? ORDER BY position",
            (user_id,))
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    async def add_user_button(self, user_id: int, text: str, url: str,
                              position: int = 0) -> None:
        await self._db.execute(
            """INSERT INTO user_buttons (user_id, text, url, position)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(user_id, text, url)
               DO UPDATE SET position = excluded.position""",
            (user_id, text, url, position))
        await self._db.commit()

    async def delete_user_button(self, user_id: int, button_id: int) -> None:
        await self._db.execute(
            "DELETE FROM user_buttons WHERE id = ? AND user_id = ?",
            (button_id, user_id))
        await self._db.commit()

    async def set_user_buttons(self, user_id: int, buttons: list[dict]) -> None:
        """Полностью заменить глобальные кнопки пользователя."""
        await self._db.execute(
            "DELETE FROM user_buttons WHERE user_id = ?", (user_id,))
        for i, b in enumerate(buttons):
            await self._db.execute(
                "INSERT INTO user_buttons (user_id, text, url, position) VALUES (?, ?, ?, ?)",
                (user_id, b["text"], b["url"], i))
        await self._db.commit()

    # ─── Channel Buttons ─────────────────────────────────────────────────

    async def get_channel_buttons(self, user_id: int, channel_id: str) -> list[dict]:
        """Кнопки для конкретного канала из таблицы channel_buttons."""
        cursor = await self._db.execute(
            """SELECT * FROM channel_buttons
               WHERE user_id = ? AND channel_id = ? ORDER BY position""",
            (user_id, channel_id))
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    async def add_channel_button(self, user_id: int, channel_id: str,
                                 text: str, url: str, position: int = 0) -> None:
        await self._db.execute(
            """INSERT INTO channel_buttons (user_id, channel_id, text, url, position)
               VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(user_id, channel_id, text, url)
               DO UPDATE SET position = excluded.position""",
            (user_id, channel_id, text, url, position))
        await self._db.commit()

    async def delete_channel_button(self, user_id: int, channel_id: str,
                                    button_id: int) -> None:
        await self._db.execute(
            "DELETE FROM channel_buttons WHERE id = ? AND user_id = ? AND channel_id = ?",
            (button_id, user_id, channel_id))
        await self._db.commit()

    async def set_channel_buttons(self, user_id: int, channel_id: str,
                                  buttons: list[dict]) -> None:
        """Полностью заменить кнопки канала."""
        await self._db.execute(
            "DELETE FROM channel_buttons WHERE user_id = ? AND channel_id = ?",
            (user_id, channel_id))
        for i, b in enumerate(buttons):
            await self._db.execute(
                """INSERT OR IGNORE INTO channel_buttons (user_id, channel_id, text, url, position)
                   VALUES (?, ?, ?, ?, ?)""",
                (user_id, channel_id, b["text"], b["url"], i))
        await self._db.commit()

    async def clear_channel_buttons(self, user_id: int, channel_id: str) -> None:
        """Удалить кнопки канала — будут использоваться глобальные."""
        await self._db.execute(
            "DELETE FROM channel_buttons WHERE user_id = ? AND channel_id = ?",
            (user_id, channel_id))
        await self._db.commit()

    # ─── Button Sets ─────────────────────────────────────────────────────

    async def get_button_set(self, user_id: int, channel_id: str, set_type: str) -> list[dict]:
        cursor = await self._db.execute(
            "SELECT buttons_json FROM button_sets WHERE user_id = ? AND channel_id = ? AND set_type = ?",
            (user_id, channel_id, set_type))
        row = await cursor.fetchone()
        if row and row[0]:
            try:
                return json.loads(row[0])
            except Exception:
                return []
        return []

    async def get_button_set_layout(self, user_id: int, channel_id: str, set_type: str) -> int:
        """Получить layout (1/2/3 кнопки в ряд) для набора кнопок."""
        if set_type == "default":
            cursor = await self._db.execute(
                "SELECT default_layout FROM channels WHERE user_id = ? AND channel_id = ?",
                (user_id, channel_id))
            row = await cursor.fetchone()
            return int(row[0]) if row and row[0] else 1
        cursor = await self._db.execute(
            "SELECT layout FROM button_sets WHERE user_id = ? AND channel_id = ? AND set_type = ?",
            (user_id, channel_id, set_type))
        row = await cursor.fetchone()
        if row:
            return int(row[0]) if row[0] else 1
        return 1

    async def set_button_set_layout(self, user_id: int, channel_id: str,
                                     set_type: str, layout: int) -> None:
        """Установить layout для набора кнопок (1/2/3)."""
        layout = max(1, min(3, layout))
        if set_type == "default":
            await self._db.execute(
                "UPDATE channels SET default_layout = ? WHERE user_id = ? AND channel_id = ?",
                (layout, user_id, channel_id))
            await self._db.commit()
            return
        # Если запись уже существует — обновляем layout, иначе создаём пустую
        await self._db.execute(
            """INSERT INTO button_sets (user_id, channel_id, set_type, buttons_json, layout)
               VALUES (?, ?, ?, '[]', ?)
               ON CONFLICT(user_id, channel_id, set_type)
               DO UPDATE SET layout = excluded.layout""",
            (user_id, channel_id, set_type, layout))
        await self._db.commit()

    async def set_button_set(self, user_id: int, channel_id: str,
                             set_type: str, buttons: list[dict]) -> None:
        await self._db.execute(
            """INSERT INTO button_sets (user_id, channel_id, set_type, buttons_json)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(user_id, channel_id, set_type)
               DO UPDATE SET buttons_json = excluded.buttons_json""",
            (user_id, channel_id, set_type, json.dumps(buttons, ensure_ascii=False)))
        await self._db.commit()

    async def get_all_button_sets(self, user_id: int, channel_id: str) -> dict:
        cursor = await self._db.execute(
            "SELECT set_type, buttons_json FROM button_sets WHERE user_id = ? AND channel_id = ?",
            (user_id, channel_id))
        rows = await cursor.fetchall()
        result = {}
        for r in rows:
            try:
                result[r[0]] = json.loads(r[1])
            except Exception:
                result[r[0]] = []
        return result

    async def get_all_button_sets_with_layout(self, user_id: int, channel_id: str) -> dict:
        """Получить все наборы с layout: {set_type: {"buttons": [...], "layout": N}}."""
        cursor = await self._db.execute(
            "SELECT set_type, buttons_json, layout FROM button_sets WHERE user_id = ? AND channel_id = ?",
            (user_id, channel_id))
        rows = await cursor.fetchall()
        result = {}
        for r in rows:
            try:
                buttons = json.loads(r[1])
            except Exception:
                buttons = []
            result[r[0]] = {"buttons": buttons, "layout": int(r[2]) if r[2] else 1}
        return result

    async def delete_button_set(self, user_id: int, channel_id: str, set_type: str) -> None:
        await self._db.execute(
            "DELETE FROM button_sets WHERE user_id = ? AND channel_id = ? AND set_type = ?",
            (user_id, channel_id, set_type))
        await self._db.commit()

    async def get_effective_buttons(self, user_id: int, channel_id: str,
                                    post_type: str = "all") -> list[dict]:
        """
        Приоритет: button_sets[post_type] → user_buttons → []
        (channel_buttons / default отключены)
        """
        if post_type != "all":
            typed = await self.get_button_set(user_id, channel_id, post_type)
            if typed:
                logger.debug("BUTTONS_SOURCE: button_sets[%s] user=%s ch=%s count=%d" % (post_type, user_id, channel_id, len(typed)))
                return typed
        # default (channel_buttons) отключён — пропускаем
        usr_buttons = await self.get_user_buttons(user_id)
        if usr_buttons:
            logger.debug("BUTTONS_SOURCE: user_buttons user=%s count=%d" % (user_id, len(usr_buttons)))
            return [{"text": b["text"], "url": b["url"]} for b in usr_buttons]
        return []

    async def get_effective_layout(self, user_id: int, channel_id: str,
                                    post_type: str = "all") -> int:
        """Получить layout для эффективного набора кнопок."""
        if post_type != "all":
            layout = await self.get_button_set_layout(user_id, channel_id, post_type)
            buttons = await self.get_button_set(user_id, channel_id, post_type)
            if buttons:
                return layout
        return 1  # default — по одной в строку

    # ─── Saved Buttons (библиотека кнопок) ───────────────────────────────

    async def get_saved_buttons(self, user_id: int) -> list[dict]:
        """Получить все сохранённые кнопки пользователя."""
        cursor = await self._db.execute(
            "SELECT * FROM saved_buttons WHERE user_id = ? ORDER BY created_at DESC",
            (user_id,))
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    async def save_button(self, user_id: int, text: str, url: str) -> None:
        """Сохранить кнопку в библиотеку пользователя."""
        await self._db.execute(
            """INSERT OR IGNORE INTO saved_buttons (user_id, text, url)
               VALUES (?, ?, ?)""",
            (user_id, text, url))
        await self._db.commit()

    async def delete_saved_button(self, user_id: int, button_id: int) -> None:
        """Удалить кнопку из библиотеки."""
        await self._db.execute(
            "DELETE FROM saved_buttons WHERE id = ? AND user_id = ?",
            (button_id, user_id))
        await self._db.commit()

    async def get_saved_button_by_id(self, user_id: int, button_id: int) -> dict | None:
        """Получить сохранённую кнопку по ID."""
        cursor = await self._db.execute(
            "SELECT * FROM saved_buttons WHERE id = ? AND user_id = ?",
            (button_id, user_id))
        row = await cursor.fetchone()
        return dict(row) if row else None

    async def update_saved_button_text(self, user_id: int, button_id: int, text: str) -> None:
        """Обновить название сохранённой кнопки."""
        await self._db.execute(
            "UPDATE saved_buttons SET text = ? WHERE id = ? AND user_id = ?",
            (text, button_id, user_id))
        await self._db.commit()

    async def update_saved_button_url(self, user_id: int, button_id: int, url: str) -> None:
        """Обновить URL сохранённой кнопки."""
        await self._db.execute(
            "UPDATE saved_buttons SET url = ? WHERE id = ? AND user_id = ?",
            (url, button_id, user_id))
        await self._db.commit()

    # ─── Button Templates ────────────────────────────────────────────────

    async def save_button_template(self, user_id: int, name: str, buttons: list[dict]) -> None:
        await self._db.execute(
            """INSERT INTO button_templates (user_id, name, buttons_json)
               VALUES (?, ?, ?) ON CONFLICT(user_id, name)
               DO UPDATE SET buttons_json = excluded.buttons_json""",
            (user_id, name, json.dumps(buttons, ensure_ascii=False)))
        await self._db.commit()

    async def get_button_templates(self, user_id: int) -> list[dict]:
        cursor = await self._db.execute(
            "SELECT * FROM button_templates WHERE user_id = ? ORDER BY name",
            (user_id,))
        rows = await cursor.fetchall()
        result = []
        for r in rows:
            d = dict(r)
            try:
                d["buttons"] = json.loads(d["buttons_json"])
            except Exception:
                d["buttons"] = []
            result.append(d)
        return result

    async def delete_button_template(self, user_id: int, name: str) -> None:
        await self._db.execute(
            "DELETE FROM button_templates WHERE user_id = ? AND name = ?",
            (user_id, name))
        await self._db.commit()

    # ─── Posts ────────────────────────────────────────────────────────────

    async def save_post(self, user_id: int, channel_id: str, text: str | None,
                        media_urls: str | None, buttons_json: str | None = None,
                        order_url: str | None = None, has_price: bool = False,
                        announcement_name: str | None = None,
                        max_msg_id: str | None = None, status: str = "sent",
                        error_text: str | None = None) -> int:
        cursor = await self._db.execute(
            """INSERT INTO posts
               (user_id, channel_id, text, media_urls, buttons_json, order_url,
                has_price, announcement_name, max_msg_id, status, error_text)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (user_id, channel_id, text, media_urls, buttons_json, order_url,
             1 if has_price else 0, announcement_name, max_msg_id, status, error_text))
        await self._db.commit()
        return cursor.lastrowid

    async def get_post_by_msg_id(self, max_msg_id: str, user_id: int) -> dict | None:
        cursor = await self._db.execute(
            "SELECT * FROM posts WHERE max_msg_id = ? AND user_id = ?",
            (max_msg_id, user_id))
        row = await cursor.fetchone()
        return dict(row) if row else None

    async def mark_post_deleted(self, max_msg_id: str) -> None:
        await self._db.execute(
            "UPDATE posts SET status = 'deleted' WHERE max_msg_id = ?", (max_msg_id,))
        await self._db.commit()

    async def get_recent_announcements(self, user_id: int, channel_id: str, days: int = 3) -> list[dict]:
        cutoff = (datetime.utcnow() - timedelta(days=days)).isoformat()
        cursor = await self._db.execute(
            """SELECT announcement_name, COUNT(*) as post_count,
                      MIN(published_at) as first_post_at, MAX(published_at) as last_post_at
               FROM posts WHERE user_id = ? AND channel_id = ?
                 AND announcement_name IS NOT NULL AND announcement_name != ''
                 AND status = 'sent' AND published_at >= ?
               GROUP BY announcement_name ORDER BY MIN(published_at) DESC""",
            (user_id, channel_id, cutoff))
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    async def get_posts_by_announcement(self, user_id: int, channel_id: str,
                                        announcement_name: str) -> list[dict]:
        cursor = await self._db.execute(
            """SELECT * FROM posts
               WHERE user_id = ? AND channel_id = ? AND announcement_name = ?
                 AND status = 'sent' ORDER BY published_at""",
            (user_id, channel_id, announcement_name))
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    # ─── Admins ───────────────────────────────────────────────────────────

    async def get_all_admins(self) -> list[dict]:
        cursor = await self._db.execute("SELECT * FROM admins ORDER BY added_at")
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    async def is_admin(self, max_user_id: str) -> bool:
        cursor = await self._db.execute(
            "SELECT 1 FROM admins WHERE max_user_id = ?", (max_user_id,))
        return await cursor.fetchone() is not None

    async def add_admin(self, max_user_id: str, added_by: str) -> None:
        await self._db.execute(
            "INSERT OR IGNORE INTO admins (max_user_id, added_by) VALUES (?, ?)",
            (max_user_id, added_by))
        await self._db.commit()

    async def delete_admin(self, max_user_id: str) -> None:
        await self._db.execute(
            "DELETE FROM admins WHERE max_user_id = ?", (max_user_id,))
        await self._db.commit()

    async def count_admins(self) -> int:
        cursor = await self._db.execute("SELECT COUNT(*) FROM admins")
        row = await cursor.fetchone()
        return row[0]

    # ─── Tariff / Active status ──────────────────────────────────────────

    async def set_user_active(self, max_user_id: str, is_active: bool) -> None:
        """Установить флаг активности пользователя (тарифная блокировка)."""
        await self._db.execute(
            "UPDATE users SET is_active = ? WHERE max_user_id = ?",
            (1 if is_active else 0, max_user_id))
        await self._db.commit()

    async def is_user_active(self, max_user_id: str) -> bool:
        """Проверить активен ли пользователь."""
        cursor = await self._db.execute(
            "SELECT is_active FROM users WHERE max_user_id = ?", (max_user_id,))
        row = await cursor.fetchone()
        if not row:
            return True  # Не найден — считаем активным (новый)
        return bool(row[0])

    async def get_all_users_tokens(self) -> list[dict]:
        """Получить токены и статусы всех пользователей."""
        cursor = await self._db.execute(
            "SELECT max_user_id, token, is_active, chat_id FROM users")
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    async def set_telegram_linked(self, max_user_id: str) -> None:
        """Отметить что пользователь связал Telegram."""
        await self._db.execute(
            "UPDATE users SET telegram_linked_at = CURRENT_TIMESTAMP WHERE max_user_id = ?",
            (max_user_id,))
        await self._db.commit()

    async def is_telegram_linked(self, max_user_id: str) -> bool:
        """Проверить связан ли Telegram."""
        cursor = await self._db.execute(
            "SELECT telegram_linked_at FROM users WHERE max_user_id = ?",
            (max_user_id,))
        row = await cursor.fetchone()
        return bool(row and row[0])

    async def increment_reminder(self, max_user_id: str) -> int:
        """Увеличить счётчик напоминаний, вернуть новое значение."""
        await self._db.execute(
            "UPDATE users SET reminder_count = reminder_count + 1 WHERE max_user_id = ?",
            (max_user_id,))
        await self._db.commit()
        cursor = await self._db.execute(
            "SELECT reminder_count FROM users WHERE max_user_id = ?", (max_user_id,))
        row = await cursor.fetchone()
        return row[0] if row else 0

    async def delete_user_full(self, max_user_id: str) -> None:
        """Полностью удалить пользователя и все его данные."""
        cursor = await self._db.execute(
            "SELECT id FROM users WHERE max_user_id = ?", (max_user_id,))
        row = await cursor.fetchone()
        if not row:
            return
        user_id = row[0]
        await self._db.execute("DELETE FROM channels WHERE user_id = ?", (user_id,))
        await self._db.execute("DELETE FROM user_buttons WHERE user_id = ?", (user_id,))
        await self._db.execute("DELETE FROM channel_buttons WHERE user_id = ?", (user_id,))
        await self._db.execute("DELETE FROM button_sets WHERE user_id = ?", (user_id,))
        await self._db.execute("DELETE FROM saved_buttons WHERE user_id = ?", (user_id,))
        await self._db.execute("DELETE FROM button_templates WHERE user_id = ?", (user_id,))
        await self._db.execute("DELETE FROM posts WHERE user_id = ?", (user_id,))
        await self._db.execute("DELETE FROM helpers WHERE owner_user_id = ? OR helper_user_id = ?",
                               (user_id, user_id))
        await self._db.execute("DELETE FROM users WHERE id = ?", (user_id,))
        await self._db.commit()

    async def get_unlinked_users(self, hours: int = 3) -> list[dict]:
        """Получить пользователей без связки Telegram старше N часов."""
        cursor = await self._db.execute(
            """SELECT max_user_id, chat_id, reminder_count, created_at
               FROM users
               WHERE telegram_linked_at IS NULL
                 AND created_at <= datetime('now', '-%d hours')""" % hours)
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    # ─── Scheduled Posts (планирование) ──────────────────────────────────

    async def add_scheduled_post(self, user_id: int, post_type: str,
                                  channels_json: str, text: str | None,
                                  markup_json: str | None, media_json: str,
                                  buttons_json: str | None,
                                  scheduled_at: str) -> int:
        """Добавить запланированный пост. Возвращает ID."""
        cursor = await self._db.execute(
            """INSERT INTO scheduled_posts
               (user_id, post_type, channels_json, text, markup_json,
                media_json, buttons_json, scheduled_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (user_id, post_type, channels_json, text, markup_json,
             media_json, buttons_json, scheduled_at))
        await self._db.commit()
        return cursor.lastrowid

    async def get_due_posts(self, now_iso: str) -> list[dict]:
        """Получить все посты, время отправки которых наступило."""
        cursor = await self._db.execute(
            """SELECT sp.*, u.max_user_id, u.chat_id, u.token
               FROM scheduled_posts sp
               JOIN users u ON u.id = sp.user_id
               WHERE sp.status = 'pending' AND sp.scheduled_at <= ?
               ORDER BY sp.scheduled_at""", (now_iso,))
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    async def get_user_scheduled(self, user_id: int) -> list[dict]:
        """Получить все pending-посты пользователя для очереди."""
        cursor = await self._db.execute(
            """SELECT * FROM scheduled_posts
               WHERE user_id = ? AND status = 'pending'
               ORDER BY scheduled_at""", (user_id,))
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    async def mark_scheduled_sent(self, post_id: int) -> None:
        await self._db.execute(
            "UPDATE scheduled_posts SET status = 'sent', sent_at = datetime('now') WHERE id = ?",
            (post_id,))
        await self._db.commit()

    async def mark_scheduled_error(self, post_id: int, error: str) -> None:
        await self._db.execute(
            "UPDATE scheduled_posts SET status = 'error', error_text = ? WHERE id = ?",
            (error[:500], post_id))
        await self._db.commit()

    async def cancel_scheduled(self, post_id: int, user_id: int) -> bool:
        """Отменить запланированный пост. Возвращает True если отменён."""
        cursor = await self._db.execute(
            """UPDATE scheduled_posts SET status = 'cancelled'
               WHERE id = ? AND user_id = ? AND status = 'pending'""",
            (post_id, user_id))
        await self._db.commit()
        return cursor.rowcount > 0

    async def update_scheduled_time(self, post_id: int, user_id: int,
                                     new_time: str) -> bool:
        cursor = await self._db.execute(
            """UPDATE scheduled_posts SET scheduled_at = ?
               WHERE id = ? AND user_id = ? AND status = 'pending'""",
            (new_time, post_id, user_id))
        await self._db.commit()
        return cursor.rowcount > 0

    async def get_scheduled_by_id(self, post_id: int) -> dict | None:
        cursor = await self._db.execute(
            "SELECT * FROM scheduled_posts WHERE id = ?", (post_id,))
        row = await cursor.fetchone()
        return dict(row) if row else None

    # ─── Pin Links (привязка кнопок к закрепу) ───────────────────────────

    async def set_pin_link(self, user_id: int, channel_id: str,
                            button_set_type: str, button_index: int,
                            linked: bool) -> None:
        await self._db.execute(
            """INSERT INTO pin_links (user_id, channel_id, button_set_type, button_index, linked)
               VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(user_id, channel_id, button_set_type, button_index)
               DO UPDATE SET linked = excluded.linked""",
            (user_id, channel_id, button_set_type, button_index, int(linked)))
        await self._db.commit()

    async def get_pin_linked_buttons(self, user_id: int, channel_id: str) -> list[dict]:
        """Получить все кнопки с привязкой к закрепу."""
        cursor = await self._db.execute(
            """SELECT * FROM pin_links
               WHERE user_id = ? AND channel_id = ? AND linked = 1""",
            (user_id, channel_id))
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    async def update_pin_url(self, user_id: int, channel_id: str,
                              pin_url: str) -> int:
        """Обновить URL закрепа во всех привязанных кнопках. Возвращает кол-во обновлённых."""
        cursor = await self._db.execute(
            """UPDATE pin_links SET last_pin_url = ?
               WHERE user_id = ? AND channel_id = ? AND linked = 1""",
            (pin_url, user_id, channel_id))
        await self._db.commit()
        return cursor.rowcount

    # ─── Comments (комментарии к постам) ────────────────────────────────

    async def create_comment_link(self, comment_key: str, channel_id: str,
                                   owner_user_id: int | None = None,
                                   post_text_preview: str | None = None) -> None:
        """Создать запись comment_link перед отправкой поста."""
        await self._db.execute(
            """INSERT OR IGNORE INTO comment_links
               (comment_key, channel_id, owner_user_id, post_text_preview)
               VALUES (?, ?, ?, ?)""",
            (comment_key, channel_id, owner_user_id,
             (post_text_preview or "")[:200]))
        await self._db.commit()

    async def update_comment_link_msg_id(self, comment_key: str,
                                          max_msg_id: str) -> None:
        """Обновить msg_id после успешной отправки поста."""
        await self._db.execute(
            "UPDATE comment_links SET max_msg_id = ? WHERE comment_key = ?",
            (max_msg_id, comment_key))
        await self._db.commit()

    async def get_comment_link(self, comment_key: str) -> dict | None:
        """Получить comment_link по ключу."""
        cursor = await self._db.execute(
            "SELECT * FROM comment_links WHERE comment_key = ?",
            (comment_key,))
        row = await cursor.fetchone()
        return dict(row) if row else None

    async def add_comment(self, comment_key: str, commenter_max_user_id: str,
                           commenter_name: str, text: str) -> int:
        """Добавить комментарий. Возвращает ID."""
        cursor = await self._db.execute(
            """INSERT INTO comments
               (comment_key, commenter_max_user_id, commenter_name, text)
               VALUES (?, ?, ?, ?)""",
            (comment_key, commenter_max_user_id, commenter_name, text))
        await self._db.commit()
        return cursor.lastrowid

    async def get_comments(self, comment_key: str, limit: int = 50,
                            offset: int = 0) -> list[dict]:
        """Получить комментарии к посту."""
        cursor = await self._db.execute(
            """SELECT * FROM comments
               WHERE comment_key = ?
               ORDER BY created_at ASC
               LIMIT ? OFFSET ?""",
            (comment_key, limit, offset))
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    async def count_comments(self, comment_key: str) -> int:
        """Количество комментариев к посту."""
        cursor = await self._db.execute(
            "SELECT COUNT(*) FROM comments WHERE comment_key = ?",
            (comment_key,))
        row = await cursor.fetchone()
        return row[0]

    async def delete_comment(self, comment_id: int,
                              commenter_max_user_id: str) -> bool:
        """Удалить комментарий (только автор). Возвращает True если удалён."""
        cursor = await self._db.execute(
            "DELETE FROM comments WHERE id = ? AND commenter_max_user_id = ?",
            (comment_id, commenter_max_user_id))
        await self._db.commit()
        return cursor.rowcount > 0

    async def close(self) -> None:
        if self._db:
            await self._db.close()
            self._db = None
