# Variables
PYTHON := python3
VENV := venv
SRC_DIR := ../sqlite-lab_John-Coogan/mylib
TEST_DIR := ../sqlite-lab_John-Coogan
REQUIREMENTS := requirements.txt

# Phony targets
.PHONY: all venv install test format lint clean

# Default target
all: venv install test format lint

# Create a virtual environment
venv:
	$(PYTHON) -m venv $(VENV)

# Install project dependencies
install: venv
	$(VENV)/bin/pip install --upgrade pip -r  $(REQUIREMENTS)

# Run unit tests
test: 
	$(VENV)/bin/pytest $(TEST_DIR)

# Format code with Black
format:
	$(VENV)/bin/black $(SRC_DIR)

# Lint code with Ruff
lint:
	$(VENV)/bin/ruff check $(SRC_DIR)

# Clean up generated files and virtual environment
clean:
	rm -rf $(VENV) __pycache__ .pytest_cache
