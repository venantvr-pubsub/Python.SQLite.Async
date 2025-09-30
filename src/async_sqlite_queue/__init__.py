import logging
import sqlite3
import threading
import time
from collections import deque
from typing import Deque, List, Optional, Tuple, Union

__all__ = ["AsyncSQLite"]
logger = logging.getLogger(__name__)


class _DatabaseWorker(threading.Thread):

    def __init__(
            self,
            db_path: str,
            write_queue: Deque,
            stop_event: threading.Event,
            ready_event: threading.Event
    ) -> None:
        super().__init__(name="AsyncSQLiteWorker")
        self.db_path = db_path
        self._write_queue = write_queue
        self._stop_event = stop_event
        self._ready_event = ready_event
        self.daemon = True

    def run(self) -> None:
        conn = None
        try:
            use_uri = self.db_path.startswith("file:")
            conn = sqlite3.connect(self.db_path, timeout=30.0, check_same_thread=False, uri=use_uri)
            if not self.db_path.startswith("file::memory:"):
                conn.execute("PRAGMA journal_mode=WAL;")
                conn.execute("PRAGMA busy_timeout = 30000;")
            logger.info("Database worker is ready.")
            self._ready_event.set()
            while not self._stop_event.is_set():
                try:
                    sql, params = self._write_queue.popleft()
                    if sql is None:
                        break
                    if params is None:
                        conn.executescript(sql)
                    else:
                        conn.execute(sql, params)
                    conn.commit()
                except IndexError:
                    time.sleep(0.01)
                except sqlite3.Error as e:
                    logger.error(f"Database write error: {e}")
        except Exception as e:
            logger.error(f"Fatal error in database worker: {e}", exc_info=True)
        finally:
            if conn:
                conn.close()
            logger.info("Database connection closed.")


class AsyncSQLite:

    def __init__(self, db_path: str = ":memory:"):
        if db_path == ":memory:":
            self.db_path = "file::memory:?cache=shared"
            logger.info("Using shared in-memory database.")
        else:
            self.db_path = db_path
        self._write_queue: Deque[Optional[Tuple[str, tuple]]] = deque()
        self._queue_lock = threading.Lock()
        self._stop_worker_event = threading.Event()
        self._db_ready_event = threading.Event()
        self._worker: Optional[_DatabaseWorker] = None

    def start(self, migration_script_path: Optional[str] = None, check_table: Optional[str] = None) -> None:
        if self._worker is not None and self._worker.is_alive():
            logger.warning("Worker is already running.")
            return
        logger.info(f"Starting DatabaseWorker for '{self.db_path}'...")
        self._stop_worker_event.clear()
        self._worker = _DatabaseWorker(
            self.db_path, self._write_queue, self._stop_worker_event, self._db_ready_event,
        )
        self._worker.start()
        if migration_script_path and check_table:
            if self.wait_for_ready():
                logger.info(f"Checking for table '{check_table}' to decide on migration...")
                try:
                    res = self.execute_read("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (check_table,))
                    if not res:
                        logger.info(f"Table '{check_table}' not found, queuing migration: {migration_script_path}")
                        self.execute_script(migration_script_path)
                    else:
                        logger.info(f"Table '{check_table}' already exists, skipping migration.")
                except Exception as e:
                    logger.error(f"Failed to check or run migration: {e}")
            else:
                logger.error("Worker did not become ready, cannot run migration.")

    def wait_for_ready(self, timeout: float = 10.0) -> bool:
        return self._db_ready_event.wait(timeout=timeout)

    # noinspection PyTypeChecker
    def stop(self, timeout: float = 5.0) -> None:
        if self._worker is None or not self._worker.is_alive():
            logger.info("Worker is not running.")
            return
        logger.info("Shutting down database worker...")
        self._stop_worker_event.set()
        with self._queue_lock:
            self._write_queue.append((None, None))
        self._worker.join(timeout=timeout)
        if self._worker.is_alive():
            logger.error("Database worker failed to shut down in time.")
        self._worker = None
        logger.info("Shutdown complete.")

    def queue_size(self) -> int:
        """Returns the current number of items in the write queue."""
        with self._queue_lock:
            return len(self._write_queue)

    def wait_for_queue_empty(self, timeout: float = 5.0) -> bool:
        """Wait until the write queue is empty or timeout is reached.

        Args:
            timeout: Maximum time to wait in seconds

        Returns:
            True if queue is empty, False if timeout was reached
        """
        start_time = time.time()
        while self.queue_size() > 0:
            if time.time() - start_time > timeout:
                logger.warning(f"Queue did not empty within {timeout}s timeout")
                return False
            time.sleep(0.01)
        return True

    def execute_write(self, sql: str, params: tuple = ()) -> None:
        with self._queue_lock:
            self._write_queue.append((sql, params))

    def _get_read_connection(self) -> sqlite3.Connection:
        use_uri = self.db_path.startswith("file:")
        return sqlite3.connect(self.db_path, uri=use_uri, check_same_thread=False)

    def execute_read(self, sql: str, params: tuple = (), fetch: str = "all") -> Union[List[Tuple], Tuple, None]:
        if not self._db_ready_event.is_set():
            raise ConnectionError("Database is not ready yet. Call wait_for_ready() first.")
        with self._get_read_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, params)
            if fetch == "one":
                return cursor.fetchone()
            return cursor.fetchall()

    # noinspection PyTypeChecker
    def execute_script(self, script_path: str) -> None:
        try:
            with open(script_path) as f:
                script = f.read()
        except FileNotFoundError:
            logger.error(f"Migration script not found: {script_path}")
            raise
        with self._queue_lock:
            self._write_queue.append((script, None))
