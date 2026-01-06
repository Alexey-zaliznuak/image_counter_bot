import asyncio
import logging
import sys

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.types import BotCommand

from bot import setup_handlers
from config import BOT_TOKEN
from database import Database
from services import SyncScheduler

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
    ]
)
logger = logging.getLogger(__name__)


async def set_bot_commands(bot: Bot) -> None:
    """Устанавливает команды бота для меню."""
    commands = [
        BotCommand(command="id", description="Показать ID чата и топика"),
        BotCommand(command="set_chat_active", description="Активировать отслеживание чата"),
        BotCommand(command="set_chat_inactive", description="Деактивировать отслеживание чата"),
        BotCommand(command="set_topic_name", description="Задать название топика"),
    ]
    await bot.set_my_commands(commands)
    logger.info("Команды бота установлены")


async def main() -> None:
    """Главная функция запуска бота."""
    
    # Проверка наличия токена
    if not BOT_TOKEN:
        logger.error("BOT_TOKEN не установлен в переменных окружения!")
        sys.exit(1)

    # Инициализация базы данных
    logger.info("Инициализация базы данных...")
    db = Database()

    # Инициализация бота
    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML)
    )
    dp = Dispatcher()

    # Настройка обработчиков
    setup_handlers(dp, db)

    # Инициализация и запуск планировщика синхронизации
    scheduler = SyncScheduler(db)
    scheduler.start()

    try:
        logger.info("Бот запускается...")
        # Устанавливаем команды бота
        await set_bot_commands(bot)
        # Удаляем вебхук и запускаем polling
        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(bot)
    finally:
        logger.info("Остановка бота...")
        await scheduler.stop()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())

