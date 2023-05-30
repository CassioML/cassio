# cassIO

A framework-agnostic Python library to seamlessly integrate Apache Cassandra with ML/LLM/genAI workloads.

**Note**: this is currently an alpha release.

## Users

Step 1: `pip install cassio`.

Step 2: for example usages and integration with higher-level LLM frameworks
such as LangChain, please visit [cassio.org](https://cassio.org).

Step 3: if you need the (experimental) Vector Search capabilities, you'll
have to install custom Cassandra drivers on top of the one shipping with
the package. Check `requirements-dev.txt` to find out how to do it.

## CassIO developers

### Developing

To develop `cassio`, use the `requirements-dev.txt` (which also builds
the experimental vector support for the Python drivers).

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

