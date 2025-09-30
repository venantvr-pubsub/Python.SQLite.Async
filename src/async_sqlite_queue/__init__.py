import logging
import sqlite3
import threading
import time
from collections import deque
from typing import Any, Deque, List, Optional, Tuple

# Exporte publiquement la classe principale pour un import facile
__all__ = ["AsyncSQLite"]

# Standard logging setup for a library
logger = logging.getLogger(__name__)


class _DatabaseWorker(threading.Thread):
    # ... (contenu de la classe _DatabaseWorker inchangé) ...
    """
    Internal worker thread that processes all write operations from a queue.
    """

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
        """Main loop: initializes DB, signals readiness, then processes the queue."""
        conn = None
        try:
            # For shared in-memory databases, we must use uri=True
            use_uri = self.db_path.startswith("file:")
            conn = sqlite3.connect(self.db_path, timeout=30.0, check_same_thread=False, uri=use_uri)

            # Enable WAL mode for file-based databases for better concurrency
            if not self.db_path.startswith("file::memory:"):
                conn.execute("PRAGMA journal_mode=WAL;")
                conn.execute("PRAGMA busy_timeout = 30000;")

            logger.info("Database worker is ready.")
            self._ready_event.set()

            while not self._stop_event.is_set():
                try:
                    # The lock is handled by the main class, so we just pop here
                    sql, params = self._write_queue.popleft()

                    # A None in the queue is the signal to exit gracefully
                    if sql is None:
                        break

                    # Permet d'exécuter des scripts complets
                    if params is None:
                        conn.executescript(sql)
                    else:
                        conn.execute(sql, params)
                    conn.commit()
                except IndexError:
                    # Queue is empty, wait a bit
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
    # ... (contenu de la classe AsyncSQLite presque inchangé) ...
    """
    A thread-safe SQLite3 wrapper that performs write operations in a separate thread.
    """

    def __init__(self, db_path: str = ":memory:"):
        # Handle the special case for shared in-memory DB
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
        """
        Starts the background database worker thread.
        If a migration script is provided, it runs it if the check_table is missing.
        """
        if self._worker is not None and self._worker.is_alive():
            logger.warning("Worker is already running.")
            return

        logger.info(f"Starting DatabaseWorker for '{self.db_path}'...")
        self._stop_worker_event.clear()
        self._worker = _DatabaseWorker(
            self.db_path,
            self._write_queue,
            self._stop_worker_event,
            self._db_ready_event,
        )
        self._worker.start()

        if migration_script_path and check_table:
            if self.wait_for_ready():
                logger.info(f"Checking for table '{check_table}' to decide on migration...")
                try:
                    res = self.execute_read(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{check_table}'")
                    if not res:
                        logger.info(f"Table '{check_table}' not found, queuing migration script: {migration_script_path}")
                        self.execute_script(migration_script_path)
                    else:
                        logger.info(f"Table '{check_table}' already exists, skipping migration.")
                except Exception as e:
                    logger.error(f"Failed to check or run migration: {e}")
            else:
                logger.error("Worker did not become ready, cannot run migration.")

    def wait_for_ready(self, timeout: float = 10.0) -> bool:
        """Waits for the database worker to be initialized."""
        return self._db_ready_event.wait(timeout=timeout)

    def stop(self, timeout: float = 5.0) -> None:
        """Stops the background database worker thread gracefully."""
        if self._worker is None or not self._worker.is_alive():
            logger.info("Worker is not running.")
            return

        logger.info("Shutting down database worker...")
        self._stop_worker_event.set()
        # Add a sentinel value to the queue to unblock the worker if it's waiting on popleft()
        with self._queue_lock:
            self._write_queue.append((None, None))

        self._worker.join(timeout=timeout)
        if self._worker.is_alive():
            logger.error("Database worker failed to shut down in time.")
        self._worker = None
        logger.info("Shutdown complete.")

    def execute_write(self, sql: str, params: tuple = ()) -> None:
        """
        Queues a write operation (INSERT, UPDATE, DELETE, etc.) to be executed
        in the background. This method is non-blocking.
        """
        with self._queue_lock:
            self._write_queue.append((sql, params))

    def _get_read_connection(self) -> sqlite3.Connection:
        """
        Returns a new, read-only connection to the database.
        """
        if self.db_path.startswith("file:"):
            return sqlite3.connect(self.db_path, uri=True, check_same_thread=False)

        db_uri = f"file:{self.db_path}?mode=ro"
        return sqlite3.connect(db_uri, uri=True, check_same_thread=False)

    def execute_read(self, sql: str, params: tuple = (), fetch: str = "all") -> List[Any]:
        """
        Executes a read operation (SELECT) and returns the results immediately.
        This method is blocking.
        """
        if not self._db_ready_event.is_set():
            raise ConnectionError("Database is not ready yet. Call wait_for_ready() first.")

        try:
            with self._get_read_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(sql, params)
                if fetch == "one":
                    return cursor.fetchone()
                return cursor.fetchall()
        except sqlite3.Error as e:
            logger.error(f"Database read error: {e}")
            return []

    def execute_script(self, script_path: str) -> None:
        """
        Executes an entire SQL script file. This is treated as a write operation.
        """
        with open(script_path) as f:
            script = f.read()
        # On utilise `None` comme marqueur pour `executescript` dans le worker
        with self._queue_lock:
            self._write_queue.append((script, None))
