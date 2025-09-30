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

            # --- BOUCLE CORRIGÉE ---
            # La boucle est maintenant infinie et ne s'arrête que lorsque
            # le sentinel 'None' est trouvé dans la file.
            while True:
                try:
                    sql, params = self._write_queue.popleft()

                    if sql is None:  # Sentinel d'arrêt
                        break

                    if params is None:
                        conn.executescript(sql)
                    else:
                        conn.execute(sql, params)
                    conn.commit()
                except IndexError:
                    # Si la file est vide et que l'événement d'arrêt est signalé,
                    # on sort pour éviter une attente infinie.
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
        self._write_queue: Deque[Optional[Tuple[str, tuple]]] = deque()
        self._queue_lock = threading.Lock()
        self._stop_worker_event = threading.Event()
        self._db_ready_event = threading.Event()
        self._worker: Optional[_DatabaseWorker] = None

    # noinspection PyUnusedLocal
    def start(self, migration_script: Optional[str] = None, check_table: Optional[str] = None) -> None:
        if self._worker is not None and self._worker.is_alive():
            return
        self._stop_worker_event.clear()
        self._worker = _DatabaseWorker(self.db_path, self._write_queue, self._stop_worker_event, self._db_ready_event)
        self._worker.start()

        # Note: La logique de migration a été déplacée dans le logger lui-même,
        # mais cette structure de base reste utile.

    def wait_for_ready(self, timeout: float = 10.0) -> bool:
        return self._db_ready_event.wait(timeout=timeout)

    # --- MÉTHODE STOP() CORRIGÉE ---
    # noinspection PyTypeChecker
    def stop(self, timeout: float = 5.0) -> None:
        if self._worker is None or not self._worker.is_alive():
            return

        # 1. Attendre activement que la file se vide.
        # C'est la garantie que toutes les commandes (y compris CREATE TABLE) sont envoyées.
        start_time = time.time()
        while len(self._write_queue) > 0:
            if time.time() - start_time > timeout:
                logger.warning("Timeout waiting for queue to empty before stopping.")
                break
            time.sleep(0.01)

        # 2. Envoyer le sentinel d'arrêt au worker.
        with self._queue_lock:
            self._write_queue.append((None, None))

        # 3. Utiliser l'événement pour réveiller le worker s'il est en attente
        self._stop_worker_event.set()

        # 4. Attendre la fin du thread
        self._worker.join(timeout=timeout)
        if self._worker.is_alive():
            logger.error("Database worker failed to shut down in time.")

        self._worker = None

    def execute_write(self, sql: str, params: Optional[tuple] = ()) -> None:
        with self._queue_lock:
            self._write_queue.append((sql, params))

    def execute_read(self, sql: str, params: tuple = (), fetch: str = "all") -> Union[List[Tuple], Tuple, None]:
        if not self._db_ready_event.is_set():
            raise ConnectionError("Database is not ready.")
        use_uri = self.db_path.startswith("file:")
        with sqlite3.connect(self.db_path, uri=use_uri, check_same_thread=False, timeout=10) as conn:
            cursor = conn.cursor()
            cursor.execute(sql, params)
            if fetch == "one":
                return cursor.fetchone()
            return cursor.fetchall()