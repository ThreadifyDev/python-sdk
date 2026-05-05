.PHONY: all install test lint fmt build publish-test publish clean

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

build:
	python -m build

publish-test: build
	python -m twine upload --repository testpypi dist/*

publish: build
	python -m twine upload dist/*

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	rm -rf build/ dist/ *.egg-info/ .pytest_cache/ .ruff_cache/
