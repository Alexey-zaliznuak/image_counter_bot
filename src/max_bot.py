import asyncio
import logging
import os
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

from maxapi import Bot, Dispatcher

import config
from database import Database
from max_handlers import setup_max_handlers


def setup_logging() -> None:
    """Консоль + файл; отдельный файл от Telegram-бота, чтобы строки не перемешивались."""
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
            logging.FileHandler(f"{log_dir}/max.log", encoding="utf-8"),
        ],
    )


setup_logging()
logger = logging.getLogger(__name__)


async def main() -> None:
    if not config.MAX_BOT_TOKEN:
        logger.error("MAX_BOT_TOKEN не задан в окружении или .env")
        sys.exit(1)

    db = Database()
    bot = Bot(config.MAX_BOT_TOKEN)
    dp = Dispatcher()
    setup_max_handlers(dp, db)

    await bot.delete_webhook()
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
