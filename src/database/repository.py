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
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS active_chats (
                    chat_id INTEGER PRIMARY KEY,
                    created_at TEXT NOT NULL,
                    city TEXT NOT NULL DEFAULT 'Не указан'
                )
            """)
            
            cursor.execute("PRAGMA table_info(active_chats)")
            columns = [col[1] for col in cursor.fetchall()]
            if 'city' not in columns:
                cursor.execute("ALTER TABLE active_chats ADD COLUMN city TEXT NOT NULL DEFAULT 'Не указан'")
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS image_counts (
                    chat_id INTEGER NOT NULL,
                    topic_id INTEGER NOT NULL DEFAULT 0,
                    date TEXT NOT NULL,
                    count INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (chat_id, topic_id, date)
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS chat_titles (
                    chat_id INTEGER PRIMARY KEY,
                    title TEXT NOT NULL
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS topic_titles (
                    chat_id INTEGER NOT NULL,
                    topic_id INTEGER NOT NULL,
                    title TEXT NOT NULL,
                    PRIMARY KEY (chat_id, topic_id)
                )
            """)
            
            conn.commit()

    def _get_current_date(self) -> str:
        tz = ZoneInfo(TIMEZONE)
        return datetime.now(tz).strftime("%Y-%m-%d")

    def add_active_chat(self, chat_id: int) -> bool:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    "INSERT INTO active_chats (chat_id, created_at, city) VALUES (?, ?, 'Не указан')",
                    (chat_id, self._get_current_date())
                )
                conn.commit()
                return True
            except sqlite3.IntegrityError:
                return False

    def remove_active_chat(self, chat_id: int) -> bool:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM active_chats WHERE chat_id = ?", (chat_id,))
            conn.commit()
            return cursor.rowcount > 0

    def is_chat_active(self, chat_id: int) -> bool:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM active_chats WHERE chat_id = ?", (chat_id,))
            return cursor.fetchone() is not None

    def get_all_active_chats(self) -> list[int]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT chat_id FROM active_chats")
            return [row["chat_id"] for row in cursor.fetchall()]

    def set_chat_city(self, chat_id: int, city: str) -> bool:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE active_chats SET city = ? WHERE chat_id = ?", (city, chat_id))
            conn.commit()
            return cursor.rowcount > 0

    def get_chat_city(self, chat_id: int) -> str:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT city FROM active_chats WHERE chat_id = ?", (chat_id,))
            row = cursor.fetchone()
            return row["city"] if row else "Не указан"

    def increment_image_count(self, chat_id: int, topic_id: int = 0, count: int = 1) -> None:
        date = self._get_current_date()
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """INSERT INTO image_counts (chat_id, topic_id, date, count)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(chat_id, topic_id, date) 
                DO UPDATE SET count = count + ?""",
                (chat_id, topic_id, date, count, count)
            )
            conn.commit()

    def get_all_image_counts(self) -> list[dict]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """SELECT chat_id, topic_id, date, count
                FROM image_counts
                ORDER BY date, chat_id, topic_id"""
            )
            return [
                {"chat_id": row["chat_id"], "topic_id": row["topic_id"], 
                 "date": row["date"], "count": row["count"]}
                for row in cursor.fetchall()
            ]

    def get_unique_chat_topics(self) -> list[tuple[int, int]]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """SELECT chat_id, topic_id FROM image_counts
                GROUP BY chat_id, topic_id ORDER BY MIN(rowid)"""
            )
            return [(row["chat_id"], row["topic_id"]) for row in cursor.fetchall()]

    def get_unique_dates(self) -> list[str]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT DISTINCT date FROM image_counts ORDER BY date")
            return [row["date"] for row in cursor.fetchall()]

    def get_unique_cities(self) -> list[str]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT DISTINCT city FROM active_chats ORDER BY city")
            return [row["city"] for row in cursor.fetchall()]

    def get_all_topic_names_from_counts(self) -> list[str]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT DISTINCT ic.chat_id, ic.topic_id FROM image_counts ic")
            topic_names = set()
            for row in cursor.fetchall():
                chat_id = row["chat_id"]
                topic_id = row["topic_id"]
                topic_name = self.get_topic_title(chat_id, topic_id)
                topic_names.add(topic_name)
            return sorted(list(topic_names))

    def get_image_count(self, chat_id: int, topic_id: int, date: str) -> int:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT count FROM image_counts WHERE chat_id = ? AND topic_id = ? AND date = ?",
                (chat_id, topic_id, date)
            )
            row = cursor.fetchone()
            return row["count"] if row else 0

    def get_image_count_by_city_topic_date(self, city: str, topic_name: str, date: str) -> int:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT chat_id FROM active_chats WHERE city = ?", (city,))
            chat_ids = [row["chat_id"] for row in cursor.fetchall()]
            
            if not chat_ids:
                return 0
            
            total = 0
            for chat_id in chat_ids:
                if topic_name == "General":
                    topic_id = 0
                else:
                    cursor.execute(
                        "SELECT topic_id FROM topic_titles WHERE chat_id = ? AND title = ?",
                        (chat_id, topic_name)
                    )
                    row = cursor.fetchone()
                    if row:
                        topic_id = row["topic_id"]
                    else:
                        continue
                
                cursor.execute(
                    "SELECT count FROM image_counts WHERE chat_id = ? AND topic_id = ? AND date = ?",
                    (chat_id, topic_id, date)
                )
                count_row = cursor.fetchone()
                if count_row:
                    total += count_row["count"]
            
            return total

    def get_cities_with_data_for_date(self, date: str) -> list[str]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """SELECT DISTINCT ac.city FROM active_chats ac
                INNER JOIN image_counts ic ON ac.chat_id = ic.chat_id
                WHERE ic.date = ? ORDER BY ac.city""",
                (date,)
            )
            return [row["city"] for row in cursor.fetchall()]

    def update_chat_title(self, chat_id: int, title: str) -> None:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """INSERT INTO chat_titles (chat_id, title) VALUES (?, ?)
                ON CONFLICT(chat_id) DO UPDATE SET title = ?""",
                (chat_id, title, title)
            )
            conn.commit()

    def update_topic_title(self, chat_id: int, topic_id: int, title: str) -> None:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """INSERT INTO topic_titles (chat_id, topic_id, title) VALUES (?, ?, ?)
                ON CONFLICT(chat_id, topic_id) DO UPDATE SET title = ?""",
                (chat_id, topic_id, title, title)
            )
            conn.commit()

    def get_chat_title(self, chat_id: int) -> str:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT title FROM chat_titles WHERE chat_id = ?", (chat_id,))
            row = cursor.fetchone()
            return row["title"] if row else str(chat_id)

    def get_topic_title(self, chat_id: int, topic_id: int) -> str:
        if topic_id == 0:
            return "General"
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT title FROM topic_titles WHERE chat_id = ? AND topic_id = ?",
                (chat_id, topic_id)
            )
            row = cursor.fetchone()
            return row["title"] if row else f"Топик {topic_id}"

    def get_display_name(self, chat_id: int, topic_id: int) -> str:
        chat_title = self.get_chat_title(chat_id)
        topic_title = self.get_topic_title(chat_id, topic_id)
        return f"{chat_title} | {topic_title}"
