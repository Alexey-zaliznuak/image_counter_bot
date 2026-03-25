"""
POST на порт FLASK_PORT (по умолчанию 3001): ?city=...&type=...
Тело запроса — байты изображения или multipart (поле file).

Сообщения от бота в MAX не обрабатываются handle_messages; после отправки фото
дублируется учёт: message_topics (для «Продукция») и increment_image_count.
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

_SRC = Path(__file__).resolve().parent
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from flask import Flask, jsonify, request

import config
from bot.handlers import TOPIC_TYPES
from database import Database
from database.repository import MESSENGER_MAX
from max_handlers import (
    MAX_TOPIC_ID,
    _count_image_attachments,
    _message_date_from_max_ts,
)
from maxapi import Bot
from maxapi.types.input_media import InputMediaBuffer


def setup_logging() -> None:
    """Консоль + logs/<дата>/mobile_server.log — как у max.log / log.log."""
    tz = ZoneInfo(config.TIMEZONE)
    date_str = datetime.now(tz).strftime("%Y-%m-%d")
    log_dir = f"logs/{date_str}"
    os.makedirs(log_dir, exist_ok=True)
    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(
                f"{log_dir}/mobile_server.log", encoding="utf-8"
            ),
        ],
        force=True,
    )


setup_logging()
logger = logging.getLogger(__name__)

app = Flask(__name__)
_db: Database | None = None


def get_db() -> Database:
    global _db
    if _db is None:
        _db = Database()
    return _db


async def _send_photo_and_account(
    chat_id: int, image_bytes: bytes, filename: str | None
) -> tuple[str, int]:
    db = get_db()
    if not db.is_chat_active(chat_id):
        raise ValueError("chat_inactive")

    if not config.MAX_BOT_TOKEN:
        raise RuntimeError("MAX_BOT_TOKEN не задан")

    media = InputMediaBuffer(image_bytes, filename=filename or "photo.jpg")
    bot = Bot(config.MAX_BOT_TOKEN)
    try:
        sent = await bot.send_message(chat_id=chat_id, attachments=[media])
    finally:
        await bot.close_session()

    if sent is None or sent.message is None or not sent.message.body:
        raise RuntimeError("Не удалось отправить сообщение в MAX")

    msg = sent.message
    mid = msg.body.mid
    topic_type = db.get_topic_type(chat_id, MAX_TOPIC_ID)
    msg_date = _message_date_from_max_ts(msg.timestamp)
    if topic_type == "Продукция" and mid:
        db.save_message_topic(
            chat_id, mid, MAX_TOPIC_ID, created_at_date=msg_date
        )
    n_img = _count_image_attachments(msg)
    if n_img == 0:
        n_img = 1
    inc = n_img if config.COUNT_EACH_PHOTO_IN_ALBUM else 1
    db.increment_image_count(chat_id, MAX_TOPIC_ID, inc, messenger=MESSENGER_MAX)
    logger.info(
        "HTTP→MAX фото: chat_id=%s mid=%s +%s (город/тип по БД)",
        chat_id,
        mid,
        inc,
    )
    return mid, inc

# POST http://127.0.0.1:3001/upload-mobile-photo?city=Москва&type=Продукция
# Content-Type: image/jpeg
# <бинарное тело JPEG>
@app.post("/upload-mobile-photo")
def ingest_photo():
    if config.HTTP_INGEST_SECRET:
        if request.headers.get("X-Ingest-Secret") != config.HTTP_INGEST_SECRET:
            return jsonify({"ok": False, "error": "unauthorized"}), 401

    city = request.args.get("city", "").strip()
    topic_type = request.args.get("type", "").strip()
    if not city or not topic_type:
        return (
            jsonify({"ok": False, "error": "Нужны query-параметры city и type"}),
            400,
        )

    db = get_db()
    chat_ids = db.find_max_chat_ids_by_city_type(city, topic_type)
    if not chat_ids:
        return (
            jsonify({"ok": False, "error": "Нет чата с таким городом и типом"}),
            404,
        )
    if len(chat_ids) > 1:
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "Несколько чатов с этой парой город/тип",
                    "chat_ids": chat_ids,
                }
            ),
            409,
        )
    chat_id = chat_ids[0]

    filename: str | None = None
    raw = request.get_data(cache=False)
    if not raw and request.files:
        for _key, f in request.files.items():
            if f and f.filename:
                raw = f.read()
                filename = f.filename
                break
    if not raw:
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "Пустое тело: байты фото или multipart с файлом",
                }
            ),
            400,
        )
    if filename is None:
        filename = request.headers.get("X-Filename")

    try:
        mid, inc = asyncio.run(
            _send_photo_and_account(chat_id, raw, filename)
        )
    except ValueError as e:
        if str(e) == "chat_inactive":
            return (
                jsonify({"ok": False, "error": "Чат не в списке активных"}),
                403,
            )
        raise
    except RuntimeError as e:
        logger.exception("Ошибка отправки в MAX")
        return jsonify({"ok": False, "error": str(e)}), 502

    return jsonify(
        {"ok": True, "chat_id": chat_id, "message_id": mid, "counted": inc}
    )


@app.get("/meta")
def meta_cities_types():
    """Справочник для клиента: города из active_chats, типы — как в боте /set_type."""
    db = get_db()
    cities = db.get_unique_cities()
    return jsonify({"cities": cities, "types": list(TOPIC_TYPES)})


@app.get("/health")
def health():
    return jsonify({"status": "ok"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=config.FLASK_PORT, threaded=True)
