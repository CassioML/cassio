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

### Unit testing

In a virtualenv with the `requirements-dev.txt` installed, run:

```
pytest
```

(there's not ... much yet in the way of testing).
