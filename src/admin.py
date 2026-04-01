"""
admin.py — управление администраторами системы (полностью визуальное).

Доступ к командам только у администраторов (проверка в bot.py).
Все взаимодействия через inline-кнопки:
- Список администраторов с кнопками удаления
- Кнопка добавления нового администратора
- Подтверждение удаления через отдельный экран
- Кнопка [🔙 Главное меню] на каждом экране

Защиты:
- Нельзя удалить себя (случайная потеря доступа)
- Нельзя удалить последнего администратора
- Подтверждение перед удалением
"""

import time

from loguru import logger

from database import Database
from max_client import MaxClient


# ─── Вспомогательные функции для клавиатур ───────────────────────────

def _back_button() -> list:
    """Кнопка возврата в главное меню."""
    return [{"type": "callback", "text": "🔙 Главное меню", "payload": "menu_main"}]


def _admins_back_button() -> list:
    """Кнопка возврата к списку администраторов."""
    return [{"type": "callback", "text": "👥 К списку админов", "payload": "menu_admins"}]


def _make_keyboard(buttons: list) -> list:
    """Обернуть список кнопок в attachment inline_keyboard."""
    return [{"type": "inline_keyboard", "payload": {"buttons": buttons}}]


class AdminManager:
    """
    Обработчики команд управления администраторами.

    Все ответы содержат inline-кнопки для навигации.
    Использует pending_states из BotDispatcher для
    многошагового ввода (добавление нового администратора).
    """

    def __init__(self, db: Database, client: MaxClient, pending_states: dict):
        self._db = db
        self._client = client
        # Ссылка на общий dict pending_states из BotDispatcher
        self._pending = pending_states

    async def cmd_admins(self, max_user_id: str, chat_id: str) -> None:
        """Показать список администраторов с кнопками удаления/добавления."""
        admins = await self._db.get_all_admins()

        if not admins:
            await self._client.send_message(
                chat_id, "Администраторов нет.",
                attachments=_make_keyboard([_back_button()]),
            )
            return

        lines = ["👥 <b>Администраторы системы:</b>\n"]
        for a in admins:
            lines.append(
                f"• ID: <code>{a['max_user_id']}</code> (с {a['added_at'][:10]})"
            )

        # Inline-клавиатура: кнопка удаления на каждого + кнопка добавления + навигация
        buttons = []
        for a in admins:
            buttons.append([{
                "type": "callback",
                "text": f"❌ Удалить {a['max_user_id']}",
                "payload": f"del_admin:{a['max_user_id']}",
            }])
        buttons.append([{
            "type": "callback",
            "text": "➕ Добавить администратора",
            "payload": "add_admin",
        }])
        buttons.append(_back_button())

        await self._client.send_message(
            chat_id, "\n".join(lines), attachments=_make_keyboard(buttons)
        )

    async def cb_delete_admin(
        self, current_admin_id: str, target_id: str, chat_id: str
    ) -> None:
        """
        Удалить администратора с тройной проверкой:
        1. Нельзя удалить себя
        2. Нельзя удалить если останется 0
        3. Целевой администратор должен существовать
        """

        # Нельзя удалить себя — иначе администратор случайно лишится доступа
        if current_admin_id == target_id:
            await self._client.send_message(
                chat_id, "❌ Нельзя удалить самого себя",
                attachments=_make_keyboard([_admins_back_button(), _back_button()]),
            )
            return

        # Нельзя удалить последнего — система должна иметь хотя бы одного администратора.
        # Проверяем ДО удаления, а не после.
        count = await self._db.count_admins()
        if count <= 1:
            await self._client.send_message(
                chat_id, "❌ Нельзя удалить единственного администратора",
                attachments=_make_keyboard([_admins_back_button(), _back_button()]),
            )
            return

        if not await self._db.is_admin(target_id):
            await self._client.send_message(
                chat_id, f"❌ Администратор {target_id} не найден",
                attachments=_make_keyboard([_admins_back_button(), _back_button()]),
            )
            return

        await self._db.delete_admin(target_id)
        logger.info(f"Admin {target_id} deleted by {current_admin_id}")
        await self._client.send_message(
            chat_id, f"✅ Администратор {target_id} удалён",
        )

        # Обновляем список — пользователь сразу видит актуальное состояние
        await self.cmd_admins(current_admin_id, chat_id)

    async def cb_add_admin_start(self, max_user_id: str, chat_id: str) -> None:
        """Начать процесс добавления: перевести пользователя в состояние ожидания ввода ID."""
        self._pending[max_user_id] = {"action": "awaiting_admin_id", "_ts": time.monotonic()}

        # Кнопка отмены — чтобы не вводить /cancel вручную
        buttons = [
            [{"type": "callback", "text": "↩️ Отмена", "payload": "menu_admins"}],
        ]

        await self._client.send_message(
            chat_id,
            "✏️ <b>Добавление администратора</b>\n\n"
            "Отправьте MAX User ID нового администратора\n"
            "(числовой ID):",
            attachments=_make_keyboard(buttons),
        )

    async def handle_awaiting_admin_id(
        self, max_user_id: str, chat_id: str, text: str
    ) -> None:
        """Обработать ввод ID нового администратора."""
        # Валидация: ID должен быть числом
        if not text.strip().lstrip("-").isdigit():
            buttons = [
                [{"type": "callback", "text": "↩️ Отмена", "payload": "menu_admins"}],
            ]
            await self._client.send_message(
                chat_id,
                "❌ ID должен быть числом. Попробуйте ещё раз:",
                attachments=_make_keyboard(buttons),
            )
            return

        new_id = text.strip()

        if await self._db.is_admin(new_id):
            del self._pending[max_user_id]
            await self._client.send_message(
                chat_id, f"⚠️ {new_id} уже является администратором",
            )
            # Показываем обновлённый список
            await self.cmd_admins(max_user_id, chat_id)
            return

        await self._db.add_admin(new_id, added_by=max_user_id)
        del self._pending[max_user_id]
        logger.info(f"Admin {new_id} added by {max_user_id}")
        await self._client.send_message(
            chat_id, f"✅ Администратор {new_id} добавлен",
        )
        # Показываем обновлённый список
        await self.cmd_admins(max_user_id, chat_id)
