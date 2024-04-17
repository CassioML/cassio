SHELL := /bin/bash

.PHONY: all format format-fix format-tests format-src test-all test-unit test-integration test-astra-integration test-cassandra-integration build help

all: help

FMT_FLAGS ?= --check

format: format-src format-tests

format-tests:
	poetry run ruff tests
	poetry run isort tests $(FMT_FLAGS)
	poetry run black tests $(FMT_FLAGS)
	poetry run mypy tests

format-src:
	poetry run ruff src
	poetry run isort src $(FMT_FLAGS)
	poetry run black src $(FMT_FLAGS)
	poetry run mypy src

format-fix: FMT_FLAGS=
format-fix: format-src format-tests

test-all: test-unit test-integration

test-unit:
	poetry run pytest tests/unit -vv

test-integration:
	poetry run pytest tests/integration -vv

test-astra-integration:
	TEST_DB_MODE="ASTRA_DB" poetry run pytest tests/integration -vv

test-cassandra-integration:
	TEST_DB_MODE="LOCAL_CASSANDRA" poetry run pytest tests/integration -vv

test-testcontainerscassandra-integration:
	TEST_DB_MODE="TESTCONTAINERS_CASSANDRA" poetry run pytest tests/integration -vv

build:
	rm dist/*
	poetry build

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
	@echo "        test-testcontainerscassandra-integration ... with testcontainers"
	@echo "build                            create new 'dist/*', ready for PyPI"
	@echo "======================================================================"
