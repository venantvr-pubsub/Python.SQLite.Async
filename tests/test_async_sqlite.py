import os
import tempfile
import threading
import time

import pytest

from async_sqlite_queue import AsyncSQLite


# ... (Toutes les fixtures et tous les tests restent inchangés) ...
@pytest.fixture
def temp_db_path():
    """Crée un chemin vers un fichier de base de données temporaire."""
    # NamedTemporaryFile crée et ouvre un fichier ; nous voulons juste le nom.
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    yield db_path
    # Nettoyage après le test
    try:
        os.unlink(db_path)
    except FileNotFoundError:
        pass


@pytest.fixture
def db_manager(temp_db_path):
    """Fixture qui initialise, démarre et arrête proprement le manager de BDD."""
    db = AsyncSQLite(db_path=temp_db_path)
    db.start()
    # Attend que le worker soit prêt avant de lancer le test
    assert db.wait_for_ready(timeout=5), "La BDD n'a pas pu démarrer à temps."
    yield db
    # Le nettoyage se fait après que le test a utilisé la fixture
    db.stop()


@pytest.fixture
def in_memory_db_manager():
    """Fixture pour la base de données en mémoire partagée."""
    db = AsyncSQLite(db_path=":memory:")
    db.start()
    assert db.wait_for_ready(timeout=5)
    yield db
    db.stop()


def test_initialization_file_path(temp_db_path):
    """Vérifie que le chemin du fichier est correctement stocké."""
    db = AsyncSQLite(temp_db_path)
    assert db.db_path == temp_db_path


def test_initialization_in_memory():
    """Vérifie la conversion automatique de ':memory:' en URI partagée."""
    db = AsyncSQLite(":memory:")
    assert db.db_path == "file::memory:?cache=shared"


def test_lifecycle_start_wait_stop(temp_db_path):
    """Teste le cycle de vie complet : démarrage, attente et arrêt."""
    db = AsyncSQLite(temp_db_path)
    assert db._worker is None

    db.start()
    assert db._worker is not None
    assert db._worker.is_alive()

    ready = db.wait_for_ready(timeout=5)
    assert ready is True

    db.stop()
    time.sleep(0.1)  # Laisse le temps au thread de se terminer complètement
    assert not db._worker.is_alive()


def test_write_and_read(db_manager):
    """Teste l'écriture asynchrone et la lecture synchrone."""
    # 1. Créer une table (opération d'écriture)
    create_sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"
    db_manager.execute_write(create_sql)

    # 2. Insérer des données (opération d'écriture)
    insert_sql = "INSERT INTO users (name) VALUES (?)"
    db_manager.execute_write(insert_sql, ("Alice",))
    db_manager.execute_write(insert_sql, ("Bob",))

    # 3. Laisser un court instant au worker pour traiter la file d'attente
    time.sleep(0.1)

    # 4. Lire les données pour vérifier (opération de lecture)
    read_sql = "SELECT name FROM users ORDER BY name"
    results = db_manager.execute_read(read_sql)

    assert len(results) == 2
    assert results[0][0] == "Alice"
    assert results[1][0] == "Bob"


def test_execute_read_fetch_one(db_manager):
    """Teste l'option 'fetch_one' pour la lecture."""
    db_manager.execute_write("CREATE TABLE settings (key TEXT, value TEXT)")
    db_manager.execute_write("INSERT INTO settings VALUES (?, ?)", ("theme", "dark"))
    time.sleep(0.1)

    result = db_manager.execute_read("SELECT value FROM settings WHERE key=?", ("theme",), fetch="one")
    assert result is not None
    assert result[0] == "dark"


def test_read_before_ready():
    """Vérifie qu'une erreur est levée si on lit avant que la BDD soit prête."""
    db = AsyncSQLite(":memory:")
    # Ne pas appeler db.start() ou db.wait_for_ready()
    with pytest.raises(ConnectionError, match="Database is not ready yet"):
        db.execute_read("SELECT 1")


def test_execute_script(db_manager, tmp_path):
    """Teste l'exécution d'un script SQL complet à partir d'un fichier."""
    script_content = """
    CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT);
    INSERT INTO products (name) VALUES ('Laptop');
    INSERT INTO products (name) VALUES ('Mouse');
    """
    script_path = tmp_path / "init.sql"
    script_path.write_text(script_content)

    db_manager.execute_script(str(script_path))
    time.sleep(0.1)

    count = db_manager.execute_read("SELECT COUNT(*) FROM products", fetch="one")[0]
    assert count == 2


def test_concurrent_writes(db_manager):
    """Teste la robustesse avec plusieurs threads écrivant en même temps."""
    db_manager.execute_write("CREATE TABLE records (id INTEGER PRIMARY KEY, thread_id INTEGER, value INTEGER)")
    time.sleep(0.1)  # S'assurer que la table est créée

    num_threads = 10
    writes_per_thread = 50
    threads = []

    def writer_task(thread_id):
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
        thread.join()  # Attendre que tous les threads aient mis leurs écritures en file

    # Laisser au worker le temps de vider la file
    # Ce temps peut dépendre de la performance de la machine
    time.sleep(0.5)

    total_records = db_manager.execute_read("SELECT COUNT(*) FROM records", fetch="one")[0]
    assert total_records == num_threads * writes_per_thread