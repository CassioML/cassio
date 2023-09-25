SHELL := /bin/bash

.PHONY: all format format-tests format-src test-all test-unit test-integration test-astra-integration test-cassandra-integration build help

all: help

format: format-tests format-src

format-tests:
	cd tests/ && ruff .
	cd tests/ && black . --check
	cd tests/ && mypy .

format-src:
	cd src && ruff .
	cd src && black . --check
	cd src && mypy .

test-all: test-unit test-integration

test-unit:
	pytest tests/unit

test-integration:
	. .testing.env
	pytest tests/integration

test-astra-integration:
	source .testing.env
	TEST_DB_MODE="ASTRA_DB" pytest tests/integration

test-cassandra-integration:
	. .testing.env
	TEST_DB_MODE="LOCAL_CASSANDRA" pytest tests/integration

build:
	rm dist/*
	python setup.py sdist bdist_wheel

help:
	@echo "======================================================================"
	@echo "CassIO make command              purpose"
	@echo "----------------------------------------------------------------------"
	@echo "format                           lint, style and typecheck"
	@echo "  format-tests                     ... on test code"
	@echo "  format-src                       ... on library code"
	@echo "test-all                         run all tests"
	@echo "  test-unit                        ... only unit tests"
	@echo "  test-integration                 ... only integration tests"
	@echo "    test-astra-integration             ... explicitly on Astra"
	@echo "    test-cassandra-integration         ... explicitly on Cassandra"
	@echo "build                            create new 'dist/*', ready for PyPI"
	@echo "======================================================================"
