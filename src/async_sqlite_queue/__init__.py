import logging
import sqlite3
import threading
import time
from collections import deque
from typing import Deque, List, Optional, Tuple, Union

__all__ = ["AsyncSQLite"]
logger = logging.getLogger(__name__)


class _DatabaseWorker(threading.Thread):

    def __init__(self, db_path: str, write_queue: Deque, stop_event: threading.Event, ready_event: threading.Event) -> None:
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

            self._ready_event.set()

            while True:
                try:
                    sql, params = self._write_queue.popleft()
                    if sql is None:
                        break

                    # On ne commit manuellement que pour les requêtes uniques.
                    # executescript() gère sa propre transaction et son propre commit.
                    if params is None:
                        conn.executescript(sql)
                    else:
                        conn.execute(sql, params)
                        conn.commit()

                except IndexError:
                    if self._stop_event.is_set():
                        break
                    time.sleep(0.01)
                except sqlite3.Error as e:
                    logger.error(f"Database write error: {e}")
        except Exception as e:
            logger.error(f"Fatal error in database worker: {e}", exc_info=True)
        finally:
            if conn:
                conn.close()


class AsyncSQLite:

    def __init__(self, db_path: str = ":memory:"):
        if db_path == ":memory:":
            self.db_path = "file::memory:?cache=shared"
        else:
            self.db_path = db_path
        self._write_queue: Deque[Optional[Tuple[str, Optional[tuple]]]] = deque()
        self._queue_lock = threading.Lock()
        self._stop_worker_event = threading.Event()
        self._db_ready_event = threading.Event()
        self._worker: Optional[_DatabaseWorker] = None

    def start(self) -> None:
        if self._worker is not None and self._worker.is_alive():
            return
        self._stop_worker_event.clear()
        self._worker = _DatabaseWorker(self.db_path, self._write_queue, self._stop_worker_event, self._db_ready_event)
        self._worker.start()

    def wait_for_ready(self, timeout: float = 10.0) -> bool:
        return self._db_ready_event.wait(timeout=timeout)

    def stop(self, timeout: float = 5.0) -> None:
        if self._worker is None or not self._worker.is_alive():
            return

        start_time = time.time()
        while len(self._write_queue) > 0:
            if time.time() - start_time > timeout:
                logger.warning("Timeout waiting for queue to empty before stopping.")
                break
            time.sleep(0.01)

        with self._queue_lock:
            # noinspection PyTypeChecker
            self._write_queue.append((None, None))

        self._stop_worker_event.set()
        self._worker.join(timeout=timeout)
        if self._worker.is_alive():
            logger.error("Database worker failed to shut down in time.")

        self._worker = None

    def execute_write(self, sql: str, params: Optional[tuple] = ()) -> None:
        with self._queue_lock:
            self._write_queue.append((sql, params))

    def execute_read(self, sql: str, params: tuple = (), fetch: str = "all") -> Union[List[Tuple], Tuple, None]:
        if not self._db_ready_event.is_set():
            raise ConnectionError("Database is not ready yet.")

        use_uri = self.db_path.startswith("file:")
        with sqlite3.connect(self.db_path, uri=use_uri, check_same_thread=False, timeout=10) as conn:
            cursor = conn.cursor()
            cursor.execute(sql, params)
            if fetch == "one":
                return cursor.fetchone()
            return cursor.fetchall()

    def queue_size(self) -> int:
        with self._queue_lock:
            return len(self._write_queue)

    def wait_for_queue_empty(self, timeout: float = 5.0) -> bool:
        start_time = time.time()
        while self.queue_size() > 0:
            if time.time() - start_time > timeout:
                logger.warning(f"Queue did not empty within {timeout}s timeout")
                return False
            time.sleep(0.01)
        return True

    def execute_script(self, script_path: str) -> None:
        try:
            with open(script_path, 'r', encoding='utf-8') as f:
                script = f.read()
        except FileNotFoundError:
            logger.error(f"Script file not found: {script_path}")
            raise

        with self._queue_lock:
            self._write_queue.append((script, None))
