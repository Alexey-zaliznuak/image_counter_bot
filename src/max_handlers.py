"""
Обработчики бота MAX: учёт фото и метаданных чата в общей БД с Telegram-ботом.

Событий реакций (👍/👎) в публичном API MAX / maxapi на момент реализации нет —
строки в reaction_counts для чатов MAX заполняет только TG-бот, когда появится
ивент реакций в MAX, сюда можно добавить обработчик по аналогии с bot/handlers.
"""

from __future__ import annotations

import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from maxapi import Dispatcher
from maxapi.enums.attachment import AttachmentType
from maxapi.enums.chat_type import ChatType
from maxapi.filters import F
from maxapi.types import (
    BotStarted,
    CallbackButton,
    ChatTitleChanged,
    Command,
    MessageCallback,
    MessageCreated,
    Message,
)
from maxapi.utils.inline_keyboard import InlineKeyboardBuilder

from bot.handlers import TOPIC_TYPES
from config import COUNT_EACH_PHOTO_IN_ALBUM, TIMEZONE
from database import Database
from database.repository import MESSENGER_MAX

logger = logging.getLogger(__name__)

_db: Database | None = None

# В MAX один групповой чат = один «топик» в смысле отчётов (как «Продукция города Z»).
MAX_TOPIC_ID = 0


def _message_date_from_max_ts(timestamp_ms: int) -> str:
    dt = datetime.fromtimestamp(int(timestamp_ms) / 1000, tz=ZoneInfo(TIMEZONE))
    return dt.strftime("%Y-%m-%d")


def _group_chat_id_from_message(msg: Message) -> int | None:
    r = msg.recipient
    if r.chat_type != ChatType.CHAT or r.chat_id is None:
        return None
    return r.chat_id


def _count_image_attachments(message: Message) -> int:
    body = message.body
    if not body or not body.attachments:
        return 0
    n = 0
    for att in body.attachments:
        t = getattr(att, "type", None)
        if t == AttachmentType.IMAGE or t == AttachmentType.IMAGE.value:
            n += 1
    return n


def _sync_chat_and_topic_title(db: Database, chat_id: int, chat) -> None:
    if chat is None or not chat.title:
        return
    db.update_chat_title(chat_id, chat.title)
    db.update_topic_title(chat_id, MAX_TOPIC_ID, chat.title)


def _type_keyboard(chat_id: int) -> list:
    kb = InlineKeyboardBuilder()
    for idx, topic_type in enumerate(TOPIC_TYPES):
        payload = f"st:{chat_id}:{MAX_TOPIC_ID}:{idx}"
        kb.row(CallbackButton(text=topic_type[:64], payload=payload))
    return [kb.as_markup()]


def _callback_group_chat_id(event: MessageCallback) -> int | None:
    m = event.message
    if m is None:
        return None
    return _group_chat_id_from_message(m)


def setup_max_handlers(dp: Dispatcher, db: Database) -> None:
    global _db
    _db = db

    @dp.chat_title_changed()
    async def on_chat_title_changed(event: ChatTitleChanged) -> None:
        if _db is None:
            return
        _db.update_chat_title(event.chat_id, event.title)
        _db.update_topic_title(event.chat_id, MAX_TOPIC_ID, event.title)

    @dp.bot_started()
    async def bot_started(event: BotStarted) -> None:
        await event.bot.send_message(
            chat_id=event.chat_id,
            text=(
                "Бот учёта для MAX: в групповом чате используйте /set_city и /set_type, "
                "затем фото будут писаться в общую базу с Telegram. /id — идентификатор чата."
            ),
        )

    @dp.message_created(Command("start"))
    async def cmd_start(event: MessageCreated) -> None:
        await event.message.answer(
            "Чат MAX и Telegram используют одну базу: "
            "в группе задайте город (/set_city) и тип чата (/set_type), "
            "фото учитываются автоматически. В личке команды /set_city и /set_type недоступны."
        )

    @dp.message_created(Command("id"))
    async def cmd_id(event: MessageCreated) -> None:
        if _db is None:
            return
        chat_id = _group_chat_id_from_message(event.message)
        if chat_id is None:
            await event.message.answer("Команда /id только в групповом чате.")
            return
        await event.message.answer(f"{chat_id}({MAX_TOPIC_ID})")

    @dp.message_created(Command("set_city"))
    async def cmd_set_city(event: MessageCreated) -> None:
        if _db is None:
            await event.message.answer("База данных не инициализирована.")
            return
        chat_id = _group_chat_id_from_message(event.message)
        if chat_id is None:
            await event.message.answer("Команда /set_city только в групповом чате.")
            return
        _db.ensure_active_chat(chat_id)
        _sync_chat_and_topic_title(_db, chat_id, event.chat)
        text_body = event.message.body.text if event.message.body else None
        if text_body:
            parts = text_body.split(maxsplit=1)
            if len(parts) > 1:
                city = parts[1].strip()
                _db.set_chat_city(chat_id, city)
                await event.message.answer(f"Город для чата установлен: {city}")
                logger.info("MAX /set_city chat_id=%s city=%s", chat_id, city)
                return
        current = _db.get_chat_city(chat_id)
        await event.message.answer(
            f"Текущий город: {current}\n\nИспользование: /set_city Название города"
        )

    @dp.message_created(Command("set_type"))
    async def cmd_set_type(event: MessageCreated) -> None:
        if _db is None:
            await event.message.answer("База данных не инициализирована.")
            return
        chat_id = _group_chat_id_from_message(event.message)
        if chat_id is None:
            await event.message.answer("Команда /set_type только в групповом чате.")
            return
        _db.ensure_active_chat(chat_id)
        _sync_chat_and_topic_title(_db, chat_id, event.chat)
        current_type = _db.get_topic_type(chat_id, MAX_TOPIC_ID)
        topic_title = _db.get_topic_title(chat_id, MAX_TOPIC_ID)
        await event.message.answer(
            f"Чат (топик): {topic_title}\n"
            f"Текущий тип: {current_type}\n\n"
            f"Выберите тип:",
            attachments=_type_keyboard(chat_id),
        )
        logger.info("MAX /set_type chat_id=%s", chat_id)

    @dp.message_callback(F.callback.payload.startswith("st:"))
    async def callback_set_type(event: MessageCallback) -> None:
        if _db is None:
            await event.answer(notification="Ошибка: нет базы данных")
            return
        chat_id_cb = _callback_group_chat_id(event)
        if chat_id_cb is None:
            await event.answer(notification="Только в групповом чате")
            return
        payload = event.callback.payload or ""
        parts = payload.split(":")
        if len(parts) != 4:
            await event.answer(notification="Неверные данные")
            return
        _, chat_id_str, topic_id_str, type_idx_str = parts
        try:
            chat_id = int(chat_id_str)
            topic_id = int(topic_id_str)
            type_idx = int(type_idx_str)
        except ValueError:
            await event.answer(notification="Неверные данные")
            return
        if chat_id != chat_id_cb or topic_id != MAX_TOPIC_ID:
            await event.answer(notification="Несовпадение чата")
            return
        if type_idx < 0 or type_idx >= len(TOPIC_TYPES):
            await event.answer(notification="Неверный тип")
            return
        topic_type = TOPIC_TYPES[type_idx]
        _db.set_topic_type(chat_id, MAX_TOPIC_ID, topic_type)
        topic_title = _db.get_topic_title(chat_id, MAX_TOPIC_ID)
        await event.answer(
            notification=f"Тип: {topic_type}",
            new_text=f"Тип для «{topic_title}» установлен: {topic_type}",
        )
        logger.info(
            "MAX set_type callback chat_id=%s type=%s", chat_id, topic_type
        )

    @dp.message_created()
    async def handle_messages(event: MessageCreated) -> None:
        if _db is None:
            return
        msg = event.message
        if msg.sender is not None and msg.sender.is_bot:
            return
        chat_id = _group_chat_id_from_message(msg)
        if chat_id is None:
            return
        if not _db.is_chat_active(chat_id):
            return
        _sync_chat_and_topic_title(_db, chat_id, event.chat)
        topic_type = _db.get_topic_type(chat_id, MAX_TOPIC_ID)
        body = msg.body
        mid = body.mid if body else None
        msg_date = _message_date_from_max_ts(msg.timestamp)
        if topic_type == "Продукция" and mid:
            _db.save_message_topic(
                chat_id, mid, MAX_TOPIC_ID, created_at_date=msg_date
            )
        n_img = _count_image_attachments(msg)
        if n_img == 0:
            return
        inc = n_img if COUNT_EACH_PHOTO_IN_ALBUM else 1
        _db.increment_image_count(
            chat_id, MAX_TOPIC_ID, inc, messenger=MESSENGER_MAX
        )
        display = _db.get_display_name(chat_id, MAX_TOPIC_ID)
        logger.info("MAX фото: %s, +%s", display, inc)
