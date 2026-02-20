.PHONY: all install test lint fmt clean

all: lint test

install:
	pip install -e ".[dev]"

test:
	python3 -m pytest

coverage:
	python3 -m pytest --cov=threadify --cov-report=term-missing --cov-report=html

lint:
	ruff check .

fmt:
	ruff format .

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	rm -rf build/ dist/ *.egg-info/ .pytest_cache/ .ruff_cache/
