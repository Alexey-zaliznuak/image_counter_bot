import logging
from typing import Optional

from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import Command
from aiogram.types import Message

from config import COUNT_EACH_PHOTO_IN_ALBUM
from database import Database

logger = logging.getLogger(__name__)

router = Router()

# Глобальная ссылка на БД (устанавливается при setup)
_db: Optional[Database] = None


def get_topic_id(message: Message) -> int:
    """
    Извлекает ID топика из сообщения.
    Возвращает 0 для обычных групп или General топика.
    """
    # message_thread_id содержит ID топика в супергруппах с форумами
    # Для General топика или обычных групп будет None
    return message.message_thread_id or 0


def format_chat_topic(chat_id: int, topic_id: int) -> str:
    """Форматирует строку ChatId(TopicId)."""
    return f"{chat_id}({topic_id})"


@router.message(Command("id"))
async def cmd_id(message: Message) -> None:
    """Обработчик команды /id - показывает ID чата и топика."""
    chat_id = message.chat.id
    topic_id = get_topic_id(message)
    
    response = format_chat_topic(chat_id, topic_id)
    await message.reply(response)
    
    logger.info(f"Команда /id: chat_id={chat_id}, topic_id={topic_id}")


@router.message(Command("help"))
async def cmd_id(message: Message) -> None:
    chat_id = message.chat.id
    topic_id = get_topic_id(message)

    response = (
        "Основные команды:\n"
        "/help - помощь\n"
        "/set_chat_active - Включить статистику для этого чата\n"
        "/set_chat_inactive - Отключить статистику для этого чата\n"
    )
    await message.reply(response)

    logger.info(f"Команда /help: chat_id={chat_id}, topic_id={topic_id}")

@router.message(Command("set_chat_active"))
async def cmd_set_chat_active(message: Message) -> None:
    """Обработчик команды /set_chat_active - активирует чат для статистики (все топики)."""
    if _db is None:
        await message.reply("❌ Ошибка: база данных не инициализирована")
        return

    chat_id = message.chat.id
    
    if _db.add_active_chat(chat_id):
        await message.reply(f"✅ Чат {chat_id} добавлен в отслеживаемые (все топики)")
        logger.info(f"Чат активирован: chat_id={chat_id}")
    else:
        await message.reply(f"ℹ️ Чат {chat_id} уже отслеживается")


@router.message(Command("set_chat_inactive"))
async def cmd_set_chat_inactive(message: Message) -> None:
    """Обработчик команды /set_chat_inactive - деактивирует чат."""
    if _db is None:
        await message.reply("❌ Ошибка: база данных не инициализирована")
        return

    chat_id = message.chat.id
    
    if _db.remove_active_chat(chat_id):
        await message.reply(f"✅ Чат {chat_id} удален из отслеживаемых")
        logger.info(f"Чат деактивирован: chat_id={chat_id}")
    else:
        await message.reply(f"ℹ️ Чат {chat_id} не был в списке отслеживаемых")


@router.message(Command("set_topic_name"))
async def cmd_set_topic_name(message: Message) -> None:
    """
    Обработчик команды /set_topic_name - устанавливает название топика.
    Использование: /set_topic_name Название топика
    """
    if _db is None:
        await message.reply("❌ Ошибка: база данных не инициализирована")
        return

    chat_id = message.chat.id
    topic_id = get_topic_id(message)
    
    # Извлекаем название из текста команды
    if message.text:
        parts = message.text.split(maxsplit=1)
        if len(parts) > 1:
            topic_name = parts[1].strip()
            _db.update_topic_title(chat_id, topic_id, topic_name)
            await message.reply(f"✅ Название топика установлено: {topic_name}")
            logger.info(f"Название топика установлено: {topic_name} (chat_id={chat_id}, topic_id={topic_id})")
            return
    
    await message.reply("❌ Использование: /set_topic_name Название топика")


def update_titles_from_message(message: Message) -> None:
    """Обновляет названия чата и топика из сообщения."""
    if _db is None:
        return
    
    chat_id = message.chat.id
    topic_id = get_topic_id(message)
    
    # Обновляем название чата
    if message.chat.title:
        _db.update_chat_title(chat_id, message.chat.title)
    
    # Обновляем название топика если это системное сообщение о создании/редактировании
    if message.forum_topic_created:
        _db.update_topic_title(chat_id, topic_id, message.forum_topic_created.name)
        logger.info(f"Топик создан: {message.forum_topic_created.name} в чате {chat_id}")
    
    if message.forum_topic_edited and message.forum_topic_edited.name:
        _db.update_topic_title(chat_id, topic_id, message.forum_topic_edited.name)
        logger.info(f"Топик переименован: {message.forum_topic_edited.name} в чате {chat_id}")
    
    # Получаем название топика из reply_to_message (работает для существующих топиков)
    if (
        topic_id != 0
        and message.reply_to_message
        and message.reply_to_message.forum_topic_created
    ):
        topic_name = message.reply_to_message.forum_topic_created.name
        _db.update_topic_title(chat_id, topic_id, topic_name)
        logger.info(f"Название топика получено: {topic_name} в чате {chat_id}")


@router.message(F.forum_topic_created | F.forum_topic_edited)
async def handle_forum_topic_events(message: Message) -> None:
    """Обработчик событий создания/редактирования топиков."""
    update_titles_from_message(message)


@router.message(F.photo)
async def handle_photo(message: Message) -> None:
    """Обработчик фотографий - подсчитывает изображения в активных чатах."""
    if _db is None:
        return

    chat_id = message.chat.id
    topic_id = get_topic_id(message)
    
    # Обновляем название чата при каждом сообщении
    update_titles_from_message(message)
    
    # Проверяем, активен ли этот чат (все топики отслеживаются)
    if not _db.is_chat_active(chat_id):
        return

    # Определяем сколько фото считать
    if COUNT_EACH_PHOTO_IN_ALBUM:
        # Если это часть альбома, считаем как 1 фото
        # (каждое фото в альбоме приходит отдельным сообщением)
        count = 1
    else:
        # Если альбом считается как одно сообщение
        # и это часть media_group, то уже подсчитывали
        if message.media_group_id:
            # Для упрощения в режиме "альбом = 1" считаем только первое фото
            # Можно улучшить с помощью кэширования media_group_id
            count = 1
        else:
            count = 1

    _db.increment_image_count(chat_id, topic_id, count)
    
    display_name = _db.get_display_name(chat_id, topic_id)
    logger.info(f"📷 Фото получено: {display_name}")


def setup_handlers(dp: Dispatcher, db: Database) -> None:
    """Настраивает обработчики и передает зависимости."""
    global _db
    _db = db
    dp.include_router(router)
    logger.info("Обработчики настроены")

