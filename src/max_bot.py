import asyncio
import logging
import sys

from maxapi import Bot, Dispatcher

import config
from database import Database
from max_handlers import setup_max_handlers

logging.basicConfig(level=logging.INFO)
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
