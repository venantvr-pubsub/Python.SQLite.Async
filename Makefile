# Utilise python3 par défaut. Peut être surchargé (ex: make PYTHON=python3.9 test)
PYTHON=python3

# Cible par défaut, exécutée quand on tape juste `make`
.DEFAULT_GOAL := help

# Empêche make de confondre les cibles avec des fichiers du même nom
.PHONY: help install test format lint build clean

help:
	@echo "Makefile pour le projet async-sqlite-queue"
	@echo ""
	@echo "Cibles disponibles :"
	@echo "  install   - Crée un environnement virtuel et installe les dépendances en mode éditable."
	@echo "  test      - Lance la suite de tests avec pytest."
	@echo "  format    - Formate le code avec Black."
	@echo "  lint      - Vérifie la qualité du code avec Ruff."
	@echo "  build     - Construit les paquets de distribution (wheel et sdist)."
	@echo "  clean     - Supprime les fichiers temporaires et de build."

install:
	@echo "📦 Création de l'environnement virtuel et installation des dépendances..."
	$(PYTHON) -m venv .venv
	@. .venv/bin/activate && pip install --upgrade pip
	@. .venv/bin/activate && pip install -e ".[test,dev]"
	@echo "✅ Installation terminée. Activez l'environnement avec : source .venv/bin/activate"

test:
	@echo "🧪 Lancement des tests..."
	@. .venv/bin/activate && pytest

format:
	@echo "🎨 Formatage du code avec Black..."
	@. .venv/bin/activate && black src tests

lint:
	@echo "🔍 Analyse du code avec Ruff..."
	@. .venv/bin/activate && ruff check src tests

build:
	@echo "🏗️  Construction des paquets de distribution..."
	@. .venv/bin/activate && $(PYTHON) -m build

clean:
	@echo "🧹 Nettoyage du projet..."
	rm -rf .venv build dist *.egg-info .pytest_cache
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true