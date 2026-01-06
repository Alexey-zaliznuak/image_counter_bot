import asyncio
import logging

from config import SYNC_INTERVAL_MINUTES
from database import Database
from services.sheets import GoogleSheetsService

logger = logging.getLogger(__name__)


class SyncScheduler:
    def __init__(self, db: Database):
        self.db = db
        self.sheets_service = GoogleSheetsService(db)
        self._task: asyncio.Task | None = None
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

    def start(self) -> None:
        """Запускает планировщик синхронизации."""
        if self._running:
            logger.warning("Планировщик уже запущен")
            return

        self._running = True
        self._task = asyncio.create_task(self._sync_loop())
        logger.info(f"Планировщик запущен. Интервал: {SYNC_INTERVAL_MINUTES} минут")

    async def stop(self) -> None:
        """Останавливает планировщик."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("Планировщик остановлен")

    async def force_sync(self) -> None:
        """Принудительная синхронизация."""
        logger.info("Принудительная синхронизация...")
        await asyncio.get_event_loop().run_in_executor(
            None, self.sheets_service.sync_to_sheets
        )

