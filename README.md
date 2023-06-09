# cassIO

A framework-agnostic Python library to seamlessly integrate Apache Cassandra with ML/LLM/genAI workloads.

**Note**: this is currently an alpha release.

## Users

Installation is as simple as:

```
pip install cassio
```

For example usages and integration with higher-level LLM frameworks
such as LangChain, please visit [cassio.org](https://cassio.org).

## CassIO developers

### Developing

To develop `cassio`, use the `requirements-dev.txt`.

To use the dev version in an integration (e.g. your branch of LangChain),

- `pip install -e .` in this `cassio` repo;
- `pip install -e .` in the LangChain `cassio` branch of [this fork](https://github.com/hemidactylus/langchain/tree/cassio);
- plus any additional requirement files specific to the examples
you're running (such as Jupyter).

### Publishing

```
# (bump version & commit ...)
python setup.py sdist bdist_wheel
twine upload dist/*
# (login to PyPI ...)
```

### Testing

#### Unit testing

You need a virtualenv with the `requirements-dev.txt` installed.

```
pytest tests/unit
```

#### Integration with the DB

You need a virtualenv with the `requirements-dev.txt` installed.

Create the DB connection settings file, `cp TEMPLATE.testing.env .testing.env`
and then edit the properties. You need at least one of either Astra DB or a
Cassandra cluster to use, with vector-search support.

Source with `. .testing.env`.

Launch the tests with

```
pytest tests/integration
```

(you can specify `TEST_DB_MODE` in the env file or override it by prepending
the above command with `TEST_DB_MODE=LOCAL_CASSANDRA` or `ASTRA_DB` for
easy switching).
