import os
import sqlite3
from datetime import datetime
from typing import Optional
from zoneinfo import ZoneInfo

from config import DATABASE_PATH, TIMEZONE


class Database:
    def __init__(self, db_path: str = DATABASE_PATH):
        self.db_path = db_path
        self._ensure_directory()
        self._init_db()

    def _ensure_directory(self) -> None:
        """Создает директорию для БД если её нет."""
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)

    def _get_connection(self) -> sqlite3.Connection:
        """Получает соединение с БД."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        """Инициализирует таблицы БД."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            # Таблица активных чатов
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS active_chats (
                    chat_id INTEGER NOT NULL,
                    topic_id INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (chat_id, topic_id)
                )
            """)
            
            # Таблица подсчета изображений
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS image_counts (
                    chat_id INTEGER NOT NULL,
                    topic_id INTEGER NOT NULL DEFAULT 0,
                    date TEXT NOT NULL,
                    count INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (chat_id, topic_id, date)
                )
            """)
            
            conn.commit()

    def _get_current_date(self) -> str:
        """Возвращает текущую дату в формате YYYY-MM-DD по МСК."""
        tz = ZoneInfo(TIMEZONE)
        return datetime.now(tz).strftime("%Y-%m-%d")

    def add_active_chat(self, chat_id: int, topic_id: int = 0) -> bool:
        """
        Добавляет чат в список активных.
        Возвращает True если добавлен, False если уже существует.
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    """
                    INSERT INTO active_chats (chat_id, topic_id, created_at)
                    VALUES (?, ?, ?)
                    """,
                    (chat_id, topic_id, self._get_current_date())
                )
                conn.commit()
                return True
            except sqlite3.IntegrityError:
                return False

    def remove_active_chat(self, chat_id: int, topic_id: int = 0) -> bool:
        """
        Удаляет чат из списка активных.
        Возвращает True если удален, False если не существовал.
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                DELETE FROM active_chats
                WHERE chat_id = ? AND topic_id = ?
                """,
                (chat_id, topic_id)
            )
            conn.commit()
            return cursor.rowcount > 0

    def is_chat_active(self, chat_id: int, topic_id: int = 0) -> bool:
        """Проверяет, активен ли чат."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT 1 FROM active_chats
                WHERE chat_id = ? AND topic_id = ?
                """,
                (chat_id, topic_id)
            )
            return cursor.fetchone() is not None

    def get_all_active_chats(self) -> list[tuple[int, int]]:
        """Возвращает список всех активных чатов (chat_id, topic_id)."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT chat_id, topic_id FROM active_chats")
            return [(row["chat_id"], row["topic_id"]) for row in cursor.fetchall()]

    def increment_image_count(self, chat_id: int, topic_id: int = 0, count: int = 1) -> None:
        """Увеличивает счетчик изображений для чата/топика на текущую дату."""
        date = self._get_current_date()
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO image_counts (chat_id, topic_id, date, count)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(chat_id, topic_id, date) 
                DO UPDATE SET count = count + ?
                """,
                (chat_id, topic_id, date, count, count)
            )
            conn.commit()

    def get_all_image_counts(self) -> list[dict]:
        """
        Возвращает все записи подсчета изображений.
        Формат: [{"chat_id": int, "topic_id": int, "date": str, "count": int}, ...]
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT chat_id, topic_id, date, count
                FROM image_counts
                ORDER BY date, chat_id, topic_id
                """
            )
            return [
                {
                    "chat_id": row["chat_id"],
                    "topic_id": row["topic_id"],
                    "date": row["date"],
                    "count": row["count"]
                }
                for row in cursor.fetchall()
            ]

    def get_unique_chat_topics(self) -> list[tuple[int, int]]:
        """
        Возвращает уникальные комбинации (chat_id, topic_id) из image_counts.
        Отсортировано по chat_id, topic_id.
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT DISTINCT chat_id, topic_id
                FROM image_counts
                ORDER BY chat_id, topic_id
                """
            )
            return [(row["chat_id"], row["topic_id"]) for row in cursor.fetchall()]

    def get_unique_dates(self) -> list[str]:
        """Возвращает уникальные даты из image_counts, отсортированные."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT DISTINCT date
                FROM image_counts
                ORDER BY date
                """
            )
            return [row["date"] for row in cursor.fetchall()]

    def get_image_count(self, chat_id: int, topic_id: int, date: str) -> int:
        """Возвращает количество изображений для конкретного чата/топика/даты."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT count FROM image_counts
                WHERE chat_id = ? AND topic_id = ? AND date = ?
                """,
                (chat_id, topic_id, date)
            )
            row = cursor.fetchone()
            return row["count"] if row else 0

