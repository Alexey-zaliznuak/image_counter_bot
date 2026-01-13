import asyncio
import logging

from config import SYNC_INTERVAL_MINUTES
from database import Database
from services.sheets import GoogleSheetsService

logger = logging.getLogger(__name__)

# Интервал очистки старых message_topics (в часах)
CLEANUP_INTERVAL_HOURS = 1
# Возраст записей для удаления (в днях)
CLEANUP_AGE_DAYS = 30


class SyncScheduler:
    def __init__(self, db: Database):
        self.db = db
        self.sheets_service = GoogleSheetsService(db)
        self._sync_task: asyncio.Task | None = None
        self._cleanup_task: asyncio.Task | None = None
        self._running = False

    async def _sync_loop(self) -> None:
        """Основной цикл синхронизации."""
        while self._running:
            try:
                logger.info("Запуск плановой синхронизации...")
                # Запускаем синхронизацию в отдельном потоке, т.к. она блокирующая
                await asyncio.get_event_loop().run_in_executor(
                    None, self.sheets_service.sync_to_sheets
                )
            except Exception as e:
                logger.error(f"Ошибка синхронизации: {e}", exc_info=True)

            # Ждем следующего интервала
            await asyncio.sleep(SYNC_INTERVAL_MINUTES * 60)

    async def _cleanup_loop(self) -> None:
        """Цикл очистки старых записей message_topics."""
        while self._running:
            # Ждём час перед первой очисткой
            await asyncio.sleep(CLEANUP_INTERVAL_HOURS * 3600)
            
            if not self._running:
                break
                
            try:
                logger.info(f"Очистка записей message_topics старше {CLEANUP_AGE_DAYS} дней...")
                deleted = await asyncio.get_event_loop().run_in_executor(
                    None, self.db.cleanup_old_message_topics, CLEANUP_AGE_DAYS
                )
                if deleted > 0:
                    logger.info(f"Удалено {deleted} старых записей message_topics")
            except Exception as e:
                logger.error(f"Ошибка очистки message_topics: {e}", exc_info=True)

    def start(self) -> None:
        """Запускает планировщик синхронизации."""
        if self._running:
            logger.warning("Планировщик уже запущен")
            return

        self._running = True
        self._sync_task = asyncio.create_task(self._sync_loop())
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())
        logger.info(f"Планировщик запущен. Синхронизация: каждые {SYNC_INTERVAL_MINUTES} мин, очистка: каждый {CLEANUP_INTERVAL_HOURS} ч")

    async def stop(self) -> None:
        """Останавливает планировщик."""
        self._running = False
        
        for task in [self._sync_task, self._cleanup_task]:
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        
        logger.info("Планировщик остановлен")

    async def force_sync(self) -> None:
        """Принудительная синхронизация."""
        logger.info("Принудительная синхронизация...")
        await asyncio.get_event_loop().run_in_executor(
            None, self.sheets_service.sync_to_sheets
        )

