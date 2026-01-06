import logging
from typing import Any

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from config import (
    REPORT_SHEET_NAME,
    SERVICE_ACCOUNT_FILE,
    SPREADSHEET_ID,
    SYNC_BATCH_SIZE,
)
from database import Database

logger = logging.getLogger(__name__)

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


class GoogleSheetsService:
    def __init__(self, db: Database):
        self.db = db
        self.spreadsheet_id = SPREADSHEET_ID
        self.sheet_name = REPORT_SHEET_NAME
        self._service = None

    def _get_service(self) -> Any:
        """Создает и возвращает сервис Google Sheets API."""
        if self._service is None:
            credentials = Credentials.from_service_account_file(
                SERVICE_ACCOUNT_FILE, scopes=SCOPES
            )
            self._service = build("sheets", "v4", credentials=credentials)
        return self._service

    def _format_chat_topic(self, chat_id: int, topic_id: int) -> str:
        """Форматирует название столбца: ChatId(TopicId)."""
        return f"{chat_id}({topic_id})"

    def _ensure_sheet_exists(self) -> None:
        """Создает лист, если он не существует."""
        service = self._get_service()
        try:
            # Получаем информацию о таблице
            spreadsheet = service.spreadsheets().get(
                spreadsheetId=self.spreadsheet_id
            ).execute()
            
            # Проверяем, существует ли лист
            sheet_names = [sheet["properties"]["title"] for sheet in spreadsheet["sheets"]]
            
            if self.sheet_name not in sheet_names:
                # Создаем новый лист
                request = {
                    "requests": [{
                        "addSheet": {
                            "properties": {
                                "title": self.sheet_name
                            }
                        }
                    }]
                }
                service.spreadsheets().batchUpdate(
                    spreadsheetId=self.spreadsheet_id,
                    body=request
                ).execute()
                logger.info(f"Лист '{self.sheet_name}' создан")
            else:
                logger.info(f"Лист '{self.sheet_name}' уже существует")
                
        except HttpError as e:
            logger.error(f"Ошибка при проверке/создании листа: {e}")
            raise

    def _clear_sheet(self) -> None:
        """Очищает лист перед записью новых данных."""
        service = self._get_service()
        try:
            service.spreadsheets().values().clear(
                spreadsheetId=self.spreadsheet_id,
                range=f"'{self.sheet_name}'",
                body={}
            ).execute()
            logger.info(f"Лист '{self.sheet_name}' очищен")
        except HttpError as e:
            logger.error(f"Ошибка очистки листа: {e}")
            raise

    def _write_batch(self, range_name: str, values: list[list[Any]]) -> None:
        """Записывает пакет данных в таблицу."""
        service = self._get_service()
        try:
            service.spreadsheets().values().update(
                spreadsheetId=self.spreadsheet_id,
                range=range_name,
                valueInputOption="RAW",
                body={"values": values}
            ).execute()
            logger.info(f"Записан пакет в {range_name}: {len(values)} строк")
        except HttpError as e:
            logger.error(f"Ошибка записи пакета в {range_name}: {e}")
            raise

    def sync_to_sheets(self) -> None:
        """
        Синхронизирует данные из БД в Google Таблицу.
        Данные записываются пакетами по SYNC_BATCH_SIZE строк.
        """
        logger.info("Начало синхронизации с Google Sheets")

        # Получаем данные из БД
        chat_topics = self.db.get_unique_chat_topics()
        dates = self.db.get_unique_dates()

        if not chat_topics or not dates:
            logger.info("Нет данных для синхронизации")
            return

        # Формируем заголовки: Дата, ChatId1(TopicId1), ChatId1(TopicId2), ...
        headers = ["Дата"] + [
            self._format_chat_topic(chat_id, topic_id)
            for chat_id, topic_id in chat_topics
        ]

        # Формируем строки данных
        rows: list[list[Any]] = []
        for date in dates:
            row = [date]
            for chat_id, topic_id in chat_topics:
                count = self.db.get_image_count(chat_id, topic_id, date)
                row.append(count if count > 0 else "")
            rows.append(row)

        # Проверяем/создаем лист и очищаем его
        self._ensure_sheet_exists()
        self._clear_sheet()

        # Записываем заголовки
        self._write_batch(
            f"'{self.sheet_name}'!A1",
            [headers]
        )

        # Записываем данные пакетами
        total_rows = len(rows)
        for i in range(0, total_rows, SYNC_BATCH_SIZE):
            batch = rows[i:i + SYNC_BATCH_SIZE]
            start_row = i + 2  # +2 потому что 1 - заголовок, и нумерация с 1
            range_name = f"'{self.sheet_name}'!A{start_row}"
            self._write_batch(range_name, batch)
            logger.info(f"Записано строк: {min(i + SYNC_BATCH_SIZE, total_rows)}/{total_rows}")

        logger.info(f"Синхронизация завершена. Записано {total_rows} строк данных")

