import os
import tempfile
import threading

import pytest

from async_sqlite_queue import AsyncSQLite


@pytest.fixture
def temp_db_path():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    yield db_path
    try:
        os.unlink(db_path)
    except FileNotFoundError:
        pass


@pytest.fixture
def db_manager(temp_db_path):
    db = AsyncSQLite(db_path=temp_db_path)
    db.start()
    assert db.wait_for_ready(timeout=5), "La BDD n'a pas pu démarrer à temps."
    yield db
    db.stop()


@pytest.fixture
def in_memory_db_manager():
    db = AsyncSQLite(db_path=":memory:")
    db.start()
    assert db.wait_for_ready(timeout=5)
    yield db
    db.stop()


def test_initialization_file_path(temp_db_path):
    db = AsyncSQLite(temp_db_path)
    assert db.db_path == temp_db_path


def test_initialization_in_memory():
    db = AsyncSQLite(":memory:")
    assert db.db_path == "file::memory:?cache=shared"


def test_lifecycle_start_wait_stop(temp_db_path):
    """Teste le cycle de vie complet : démarrage, attente et arrêt."""
    db = AsyncSQLite(temp_db_path)
    assert db._worker is None

    db.start()
    assert db._worker is not None
    assert db._worker.is_alive()

    # On garde une référence au thread du worker
    worker_thread = db._worker
    assert isinstance(worker_thread, threading.Thread)

    ready = db.wait_for_ready(timeout=5)
    assert ready is True

    db.stop()
    # On vérifie que la référence que nous avons gardée n'est plus en vie.
    assert not worker_thread.is_alive()
    # On peut aussi vérifier que l'objet a bien été nettoyé
    assert db._worker is None


# noinspection PyUnresolvedReferences
def test_write_and_read(db_manager):
    create_sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"
    db_manager.execute_write(create_sql)
    insert_sql = "INSERT INTO users (name) VALUES (?)"
    db_manager.execute_write(insert_sql, ("Alice",))
    db_manager.execute_write(insert_sql, ("Bob",))
    assert db_manager.wait_for_queue_empty(timeout=5)
    read_sql = "SELECT name FROM users ORDER BY name"
    results = db_manager.execute_read(read_sql)
    assert len(results) == 2
    assert results[0][0] == "Alice"
    assert results[1][0] == "Bob"


# noinspection PyUnresolvedReferences
def test_execute_read_fetch_one(db_manager):
    db_manager.execute_write("CREATE TABLE settings (key TEXT, value TEXT)")
    db_manager.execute_write("INSERT INTO settings VALUES (?, ?)", ("theme", "dark"))
    assert db_manager.wait_for_queue_empty(timeout=5)
    result = db_manager.execute_read("SELECT value FROM settings WHERE key=?", ("theme",), fetch="one")
    assert result is not None
    assert result[0] == "dark"


def test_read_before_ready():
    db = AsyncSQLite(":memory:")
    with pytest.raises(ConnectionError, match="Database is not ready yet"):
        db.execute_read("SELECT 1")


# noinspection PyUnresolvedReferences
def test_execute_script(db_manager, tmp_path):
    script_content = """
    CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT);
    INSERT INTO products (name) VALUES ('Laptop');
    INSERT INTO products (name) VALUES ('Mouse');
    """
    script_path = tmp_path / "init.sql"
    script_path.write_text(script_content)
    db_manager.execute_script(str(script_path))
    assert db_manager.wait_for_queue_empty(timeout=5)
    count = db_manager.execute_read("SELECT COUNT(*) FROM products", fetch="one")[0]
    assert count == 2


# noinspection PyUnresolvedReferences
def test_concurrent_writes(db_manager):
    """Teste la robustesse avec plusieurs threads écrivant en même temps."""
    db_manager.execute_write("CREATE TABLE records (id INTEGER PRIMARY KEY, thread_id INTEGER, value INTEGER)")
    assert db_manager.wait_for_queue_empty(timeout=5)
    num_threads = 10
    writes_per_thread = 50
    threads = []

    def writer_task(thread_id):
        # noinspection PyShadowingNames
        for i in range(writes_per_thread):
            db_manager.execute_write(
                "INSERT INTO records (thread_id, value) VALUES (?, ?)",
                (thread_id, i)
            )

    for i in range(num_threads):
        thread = threading.Thread(target=writer_task, args=(i,))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    # Attendre que la file soit vide
    assert db_manager.wait_for_queue_empty(timeout=10), "La file ne s'est pas vidée à temps."

    total_records = db_manager.execute_read("SELECT COUNT(*) FROM records", fetch="one")[0]
    assert total_records == num_threads * writes_per_thread
