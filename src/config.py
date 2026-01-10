import os

from dotenv import load_dotenv

load_dotenv()

# Telegram Bot
BOT_TOKEN: str = os.getenv("BOT_TOKEN", "")

# Google Sheets
SPREADSHEET_ID: str = os.getenv("SPREADSHEET_ID", "")
SERVICE_ACCOUNT_EMAIL: str = os.getenv("SERVICE_ACCOUNT_EMAIL", "")
SERVICE_ACCOUNT_FILE: str = "service_account.json"

# Sheet settings
REPORT_SHEET_NAME: str = "Отчет по фотографиям"

# Sync settings
SYNC_INTERVAL_MINUTES: int = 2
SYNC_BATCH_SIZE: int = 20  # Количество строк за один запрос

# Timezone
TIMEZONE: str = "Europe/Moscow"

# Counting mode for albums
# True - каждое фото в альбоме считается отдельно (5 фото = 5)
# False - альбом считается как одно сообщение (5 фото = 1)
COUNT_EACH_PHOTO_IN_ALBUM: bool = True

# Database
DATABASE_PATH: str = "data/bot.db"

