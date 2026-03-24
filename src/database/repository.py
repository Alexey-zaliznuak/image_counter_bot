import os
import sqlite3
from datetime import datetime
from typing import Optional
from zoneinfo import ZoneInfo

from config import DATABASE_PATH, TIMEZONE

# Значение в колонке image_counts.messenger; для существующих строк по умолчанию TG.
MESSENGER_TG = "tg"
MESSENGER_MAX = "max"


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
                    messenger TEXT NOT NULL DEFAULT 'tg',
                    count INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (chat_id, topic_id, date, messenger)
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
                    type TEXT NOT NULL DEFAULT 'Не указан',
                    PRIMARY KEY (chat_id, topic_id)
                )
            """)
            
            # Миграция: добавляем столбец type в topic_titles если его нет
            cursor.execute("PRAGMA table_info(topic_titles)")
            columns = [col[1] for col in cursor.fetchall()]
            if 'type' not in columns:
                cursor.execute("ALTER TABLE topic_titles ADD COLUMN type TEXT NOT NULL DEFAULT 'Не указан'")
            
            # Таблица для хранения реакций (👍/👎) в топиках "Продукция"
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS reaction_counts (
                    chat_id INTEGER NOT NULL,
                    topic_id INTEGER NOT NULL DEFAULT 0,
                    date TEXT NOT NULL,
                    positive_count INTEGER NOT NULL DEFAULT 0,
                    negative_count INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (chat_id, topic_id, date)
                )
            """)
            
            # Таблица для хранения связи message_id -> topic_id
            # (нужна для определения топика при получении реакции)
            # message_id TEXT: Telegram (int как строка) и MAX (mid строка)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS message_topics (
                    chat_id INTEGER NOT NULL,
                    message_id TEXT NOT NULL,
                    topic_id INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (chat_id, message_id)
                )
            """)

            self._migrate_message_topics_message_id_to_text(conn)
            self._migrate_image_counts_messenger(conn)

            conn.commit()

    def _migrate_message_topics_message_id_to_text(self, conn: sqlite3.Connection) -> None:
        """Миграция v2: message_id INTEGER -> TEXT (Telegram + MAX)."""
        cursor = conn.cursor()
        cursor.execute("PRAGMA user_version")
        if cursor.fetchone()[0] >= 2:
            return
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='message_topics'"
        )
        if not cursor.fetchone():
            cursor.execute("PRAGMA user_version = 2")
            conn.commit()
            return
        cursor.execute("PRAGMA table_info(message_topics)")
        cols = {row[1]: (row[2] or "").upper() for row in cursor.fetchall()}
        mid_declared = cols.get("message_id", "")
        if mid_declared == "TEXT":
            cursor.execute("PRAGMA user_version = 2")
            conn.commit()
            return
        if mid_declared != "INTEGER":
            cursor.execute("PRAGMA user_version = 2")
            conn.commit()
            return
        cursor.execute("""
            CREATE TABLE message_topics_new (
                chat_id INTEGER NOT NULL,
                message_id TEXT NOT NULL,
                topic_id INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY (chat_id, message_id)
            )
        """)
        cursor.execute("""
            INSERT INTO message_topics_new (chat_id, message_id, topic_id, created_at)
            SELECT chat_id, CAST(message_id AS TEXT), topic_id, created_at
            FROM message_topics
        """)
        cursor.execute("DROP TABLE message_topics")
        cursor.execute("ALTER TABLE message_topics_new RENAME TO message_topics")
        cursor.execute("PRAGMA user_version = 2")
        conn.commit()

    def _migrate_image_counts_messenger(self, conn: sqlite3.Connection) -> None:
        """Миграция v3: колонка messenger и составной PK (старые строки — messenger='tg')."""
        cursor = conn.cursor()
        cursor.execute("PRAGMA user_version")
        if cursor.fetchone()[0] >= 3:
            return
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='image_counts'"
        )
        if not cursor.fetchone():
            cursor.execute("PRAGMA user_version = 3")
            conn.commit()
            return
        cursor.execute("PRAGMA table_info(image_counts)")
        col_names = [row[1] for row in cursor.fetchall()]
        if "messenger" in col_names:
            cursor.execute("PRAGMA user_version = 3")
            conn.commit()
            return
        cursor.execute("""
            CREATE TABLE image_counts_new (
                chat_id INTEGER NOT NULL,
                topic_id INTEGER NOT NULL DEFAULT 0,
                date TEXT NOT NULL,
                messenger TEXT NOT NULL DEFAULT 'tg',
                count INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (chat_id, topic_id, date, messenger)
            )
        """)
        cursor.execute("""
            INSERT INTO image_counts_new (chat_id, topic_id, date, count, messenger)
            SELECT chat_id, topic_id, date, count, 'tg' FROM image_counts
        """)
        cursor.execute("DROP TABLE image_counts")
        cursor.execute("ALTER TABLE image_counts_new RENAME TO image_counts")
        cursor.execute("PRAGMA user_version = 3")
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

    def ensure_active_chat(self, chat_id: int) -> None:
        """Добавляет чат в отслеживаемые, если его ещё нет."""
        if not self.is_chat_active(chat_id):
            self.add_active_chat(chat_id)

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

    def increment_image_count(
        self,
        chat_id: int,
        topic_id: int = 0,
        count: int = 1,
        *,
        messenger: str = MESSENGER_TG,
    ) -> None:
        date = self._get_current_date()
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """INSERT INTO image_counts (chat_id, topic_id, date, messenger, count)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(chat_id, topic_id, date, messenger)
                DO UPDATE SET count = count + ?""",
                (chat_id, topic_id, date, messenger, count, count)
            )
            conn.commit()

    def get_all_image_counts(self) -> list[dict]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """SELECT chat_id, topic_id, date, messenger, count
                FROM image_counts
                ORDER BY date, messenger, chat_id, topic_id"""
            )
            return [
                {
                    "chat_id": row["chat_id"],
                    "topic_id": row["topic_id"],
                    "date": row["date"],
                    "messenger": row["messenger"],
                    "count": row["count"],
                }
                for row in cursor.fetchall()
            ]

    def get_unique_chat_topics(
        self, messenger: str | None = None
    ) -> list[tuple[int, int]]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            if messenger is None:
                cursor.execute(
                    """SELECT chat_id, topic_id FROM image_counts
                    GROUP BY chat_id, topic_id ORDER BY MIN(rowid)"""
                )
            else:
                cursor.execute(
                    """SELECT chat_id, topic_id FROM image_counts
                    WHERE messenger = ?
                    GROUP BY chat_id, topic_id ORDER BY MIN(rowid)""",
                    (messenger,),
                )
            return [(row["chat_id"], row["topic_id"]) for row in cursor.fetchall()]

    def get_unique_dates_for_messenger(self, messenger: str) -> list[str]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """SELECT DISTINCT date FROM image_counts
                WHERE messenger = ? ORDER BY date""",
                (messenger,),
            )
            return [row["date"] for row in cursor.fetchall()]

    def get_unique_cities(self) -> list[str]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT DISTINCT city FROM active_chats ORDER BY city")
            return [row["city"] for row in cursor.fetchall()]

    def get_image_count(
        self,
        chat_id: int,
        topic_id: int,
        date: str,
        *,
        messenger: str = MESSENGER_TG,
    ) -> int:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """SELECT count FROM image_counts
                WHERE chat_id = ? AND topic_id = ? AND date = ? AND messenger = ?""",
                (chat_id, topic_id, date, messenger),
            )
            row = cursor.fetchone()
            return row["count"] if row else 0

    def get_image_count_by_city_type_date(
        self,
        city: str,
        topic_type: str,
        date: str,
        *,
        messenger: str = MESSENGER_TG,
    ) -> int:
        """
        Возвращает сумму изображений для города/типа топика/даты.
        Суммирует по всем чатам с указанным городом и топикам с указанным типом.
        Топики с type='Не указан' игнорируются.
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            # Получаем все чаты с указанным городом
            cursor.execute(
                "SELECT chat_id FROM active_chats WHERE city = ?",
                (city,)
            )
            chat_ids = [row["chat_id"] for row in cursor.fetchall()]
            
            if not chat_ids:
                return 0
            
            total = 0
            for chat_id in chat_ids:
                # Находим все topic_id с указанным типом в этом чате
                cursor.execute(
                    "SELECT topic_id FROM topic_titles WHERE chat_id = ? AND type = ?",
                    (chat_id, topic_type)
                )
                topic_ids = [row["topic_id"] for row in cursor.fetchall()]
                
                for topic_id in topic_ids:
                    cursor.execute(
                        """SELECT count FROM image_counts
                        WHERE chat_id = ? AND topic_id = ? AND date = ? AND messenger = ?""",
                        (chat_id, topic_id, date, messenger),
                    )
                    count_row = cursor.fetchone()
                    if count_row:
                        total += count_row["count"]
            
            return total

    def get_cities_with_data_for_date(
        self, date: str, *, messenger: str = MESSENGER_TG
    ) -> list[str]:
        """
        Возвращает список городов, у которых есть данные за указанную дату.
        Учитывает только топики с установленным типом (не 'Не указан').
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """SELECT DISTINCT ac.city FROM active_chats ac
                INNER JOIN image_counts ic ON ac.chat_id = ic.chat_id
                INNER JOIN topic_titles tt ON ic.chat_id = tt.chat_id AND ic.topic_id = tt.topic_id
                WHERE ic.date = ? AND ic.messenger = ? AND tt.type != 'Не указан'
                ORDER BY ac.city""",
                (date, messenger),
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
                """INSERT INTO topic_titles (chat_id, topic_id, title, type) VALUES (?, ?, ?, 'Не указан')
                ON CONFLICT(chat_id, topic_id) DO UPDATE SET title = ?""",
                (chat_id, topic_id, title, title)
            )
            conn.commit()

    def set_topic_type(self, chat_id: int, topic_id: int, topic_type: str) -> bool:
        """Устанавливает тип для топика. Возвращает True если успешно."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            # Сначала убедимся что запись существует
            cursor.execute(
                """INSERT INTO topic_titles (chat_id, topic_id, title, type) VALUES (?, ?, ?, ?)
                ON CONFLICT(chat_id, topic_id) DO UPDATE SET type = ?""",
                (chat_id, topic_id, f"Топик {topic_id}", topic_type, topic_type)
            )
            conn.commit()
            return True

    def get_topic_type(self, chat_id: int, topic_id: int) -> str:
        """Возвращает тип топика или 'Не указан' если не найден."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT type FROM topic_titles WHERE chat_id = ? AND topic_id = ?",
                (chat_id, topic_id)
            )
            row = cursor.fetchone()
            return row["type"] if row else "Не указан"

    def get_chat_title(self, chat_id: int) -> str:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT title FROM chat_titles WHERE chat_id = ?", (chat_id,))
            row = cursor.fetchone()
            return row["title"] if row else str(chat_id)

    def get_topic_title(self, chat_id: int, topic_id: int) -> str:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT title FROM topic_titles WHERE chat_id = ? AND topic_id = ?",
                (chat_id, topic_id)
            )
            row = cursor.fetchone()
            if row:
                return row["title"]
        if topic_id == 0:
            return "General"
        return f"Топик {topic_id}"

    def get_display_name(self, chat_id: int, topic_id: int) -> str:
        chat_title = self.get_chat_title(chat_id)
        topic_title = self.get_topic_title(chat_id, topic_id)
        return f"{chat_title} | {topic_title}"

    def update_reaction_count(
        self, 
        chat_id: int, 
        topic_id: int, 
        positive_delta: int = 0, 
        negative_delta: int = 0,
        date: Optional[str] = None
    ) -> None:
        """
        Обновляет счётчик реакций (положительных/отрицательных).
        Дельта может быть положительной (добавление) или отрицательной (удаление).
        
        Args:
            date: Дата для записи реакции. Если None - используется текущая дата.
                  Обычно передаётся дата создания сообщения, чтобы реакция
                  засчитывалась на тот день, когда было создано сообщение.
        """
        if date is None:
            date = self._get_current_date()
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """INSERT INTO reaction_counts (chat_id, topic_id, date, positive_count, negative_count)
                VALUES (?, ?, ?, MAX(0, ?), MAX(0, ?))
                ON CONFLICT(chat_id, topic_id, date) 
                DO UPDATE SET 
                    positive_count = MAX(0, positive_count + ?),
                    negative_count = MAX(0, negative_count + ?)""",
                (chat_id, topic_id, date, positive_delta, negative_delta, positive_delta, negative_delta)
            )
            conn.commit()

    def save_message_topic(
        self,
        chat_id: int,
        message_id: int | str,
        topic_id: int,
        *,
        created_at_date: Optional[str] = None,
    ) -> None:
        """Сохраняет связь message_id -> topic_id для последующего определения топика при реакции."""
        date = created_at_date if created_at_date is not None else self._get_current_date()
        mid = str(message_id)
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """INSERT OR REPLACE INTO message_topics (chat_id, message_id, topic_id, created_at)
                VALUES (?, ?, ?, ?)""",
                (chat_id, mid, topic_id, date)
            )
            conn.commit()

    def get_topic_by_message(self, chat_id: int, message_id: int | str) -> Optional[int]:
        """Возвращает topic_id для сообщения или None если не найдено."""
        mid = str(message_id)
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT topic_id FROM message_topics WHERE chat_id = ? AND message_id = ?",
                (chat_id, mid)
            )
            row = cursor.fetchone()
            return row["topic_id"] if row else None

    def get_message_created_date(self, chat_id: int, message_id: int | str) -> Optional[str]:
        """Возвращает дату создания сообщения или None если не найдено."""
        mid = str(message_id)
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT created_at FROM message_topics WHERE chat_id = ? AND message_id = ?",
                (chat_id, mid)
            )
            row = cursor.fetchone()
            return row["created_at"] if row else None

    def get_message_info(self, chat_id: int, message_id: int | str) -> Optional[tuple[int, str]]:
        """
        Возвращает (topic_id, created_at) для сообщения или None если не найдено.
        Оптимизация: один запрос вместо двух.
        """
        mid = str(message_id)
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT topic_id, created_at FROM message_topics WHERE chat_id = ? AND message_id = ?",
                (chat_id, mid)
            )
            row = cursor.fetchone()
            return (row["topic_id"], row["created_at"]) if row else None

    def cleanup_old_message_topics(self, days: int = 7) -> int:
        """Удаляет старые записи из message_topics (старше N дней). Возвращает количество удалённых."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """DELETE FROM message_topics 
                WHERE date(created_at) < date('now', '-' || ? || ' days')""",
                (days,)
            )
            conn.commit()
            return cursor.rowcount

    def get_reaction_count_by_city_date(
        self, city: str, date: str
    ) -> tuple[int, int]:
        """
        Возвращает сумму реакций (positive, negative) для города/даты.
        Учитывает только топики с типом 'Продукция'.
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            # Получаем все чаты с указанным городом
            cursor.execute(
                "SELECT chat_id FROM active_chats WHERE city = ?",
                (city,)
            )
            chat_ids = [row["chat_id"] for row in cursor.fetchall()]
            
            if not chat_ids:
                return (0, 0)
            
            total_positive = 0
            total_negative = 0
            
            for chat_id in chat_ids:
                # Находим все topic_id с типом "Продукция" в этом чате
                cursor.execute(
                    "SELECT topic_id FROM topic_titles WHERE chat_id = ? AND type = ?",
                    (chat_id, "Продукция")
                )
                topic_ids = [row["topic_id"] for row in cursor.fetchall()]
                
                for topic_id in topic_ids:
                    cursor.execute(
                        """SELECT positive_count, negative_count 
                        FROM reaction_counts 
                        WHERE chat_id = ? AND topic_id = ? AND date = ?""",
                        (chat_id, topic_id, date)
                    )
                    row = cursor.fetchone()
                    if row:
                        total_positive += row["positive_count"]
                        total_negative += row["negative_count"]
            
            return (total_positive, total_negative)
