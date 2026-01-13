import logging
from datetime import datetime
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

# Фиксированные типы топиков для столбцов таблицы
# "Продукция" перемещена в конец, после неё идут столбцы реакций
TOPIC_TYPES = [
    "Списание Продуктов",
    "Чистота",
    "Выручка и закупки",
    "Заготовки",
    "Обсуждение",
    "Брендированная упаковка",
    "Продукция",
]

# Дополнительные столбцы для реакций (добавляются после TOPIC_TYPES)
REACTION_COLUMNS = [
    "Лайки",
    "Дизлайки",
]


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

    def _format_date(self, date_str: str) -> str:
        """Конвертирует дату из YYYY-MM-DD в DD.MM.YYYY."""
        try:
            date_obj = datetime.strptime(date_str, "%Y-%m-%d")
            return date_obj.strftime("%d.%m.%Y")
        except ValueError:
            return date_str

    def _ensure_sheet_exists(self) -> None:
        """Создает лист, если он не существует."""
        service = self._get_service()
        try:
            spreadsheet = service.spreadsheets().get(
                spreadsheetId=self.spreadsheet_id
            ).execute()
            
            sheet_names = [sheet["properties"]["title"] for sheet in spreadsheet["sheets"]]
            
            if self.sheet_name not in sheet_names:
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

    def _get_sheet_id(self) -> int | None:
        """Получает ID листа по его названию."""
        service = self._get_service()
        try:
            spreadsheet = service.spreadsheets().get(
                spreadsheetId=self.spreadsheet_id
            ).execute()
            
            for sheet in spreadsheet["sheets"]:
                if sheet["properties"]["title"] == self.sheet_name:
                    return sheet["properties"]["sheetId"]
            return None
        except HttpError as e:
            logger.error(f"Ошибка получения ID листа: {e}")
            return None

    def _auto_resize_columns(self, num_columns: int) -> None:
        """Автоматически подгоняет ширину столбцов под содержимое."""
        sheet_id = self._get_sheet_id()
        if sheet_id is None:
            logger.error("Не удалось получить ID листа для изменения ширины столбцов")
            return
        
        service = self._get_service()
        try:
            request = {
                "requests": [{
                    "autoResizeDimensions": {
                        "dimensions": {
                            "sheetId": sheet_id,
                            "dimension": "COLUMNS",
                            "startIndex": 0,
                            "endIndex": num_columns
                        }
                    }
                }]
            }
            service.spreadsheets().batchUpdate(
                spreadsheetId=self.spreadsheet_id,
                body=request
            ).execute()
            logger.info(f"Ширина {num_columns} столбцов автоматически подогнана")
        except HttpError as e:
            logger.error(f"Ошибка автоподгонки ширины столбцов: {e}")

    def sync_to_sheets(self) -> None:
        """
        Синхронизирует данные из БД в Google Таблицу.
        Структура: Дата | Город | Тип1 | Тип2 | ... | Продукция | Реакции+ | Реакции-
        Для каждой даты несколько строк (по городам).
        Топики с type='Не указан' игнорируются.
        """
        logger.info("Начало синхронизации с Google Sheets")

        # Получаем данные из БД
        dates = self.db.get_unique_dates()
        
        if not dates:
            logger.info("Нет данных для синхронизации")
            return

        # Формируем заголовки: Дата, Город, типы топиков, столбцы реакций
        headers = ["Дата", "Город"] + TOPIC_TYPES + REACTION_COLUMNS

        # Формируем строки данных
        rows: list[list[Any]] = []
        for date in dates:
            # Получаем города с данными за эту дату (только с топиками у которых установлен тип)
            cities = self.db.get_cities_with_data_for_date(date)
            
            formatted_date = self._format_date(date)
            
            for city in cities:
                row = [formatted_date, city]
                # Добавляем количество фото по типам
                for topic_type in TOPIC_TYPES:
                    count = self.db.get_image_count_by_city_type_date(city, topic_type, date)
                    row.append(count)  # 0 если нет данных
                
                # Добавляем реакции (только для топиков "Продукция")
                positive_reactions, negative_reactions = self.db.get_reaction_count_by_city_date(city, date)
                row.append(positive_reactions)
                row.append(negative_reactions)
                
                rows.append(row)

        if not rows:
            logger.info("Нет данных для записи")
            return

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

        # Автоматически подгоняем ширину столбцов
        self._auto_resize_columns(len(headers))

        logger.info(f"Синхронизация завершена. Записано {total_rows} строк данных")
