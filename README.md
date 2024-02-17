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

### Setup

To develop `cassio`, use the `requirements-dev.txt`.

To use the dev version in an integration (e.g. your branch of LangChain),
`pip install -e .` in this `cassio` repo from within the virtual environment
you are using to develop your integration.

#### Poetry

If the integration is Poetry-based (e.g. LangChain itself), you should get this
in your `pyproject.toml`:

```
cassio = {path = "../../cassio", develop = true}
```

Then you do

```
poetry remove cassio                                      # if necessary
poetry lock --no-update
poetry install -E all --with dev --with test_integration  # or similar, this is for langchain
```

[Inspired from this](https://github.com/orgs/python-poetry/discussions/1135).
You also need a recent Poetry for this to work.

#### Versioning

We are still at `0.*`. Occasional breaking changes are to be expected,
but please think carefully. Later, a stronger versioning model will be adopted.

### Style and typing

Style is enforced through `black`, linting with `ruff`,
and typechecking with `mypy`.
The code should run through `make format` without issues.

### Python version coverage

At the moment we try to run tests under Python3.8 and Python3.10 to try and
catch versions-specific issues
(such as the newer `typing` syntax such as `typeA | typeB`, illegal on 3.8).

### Publishing

- Bump version in setup.py
- Add to `CHANGES.txt`
- Commit the very code that will be built:
- `git tag v<x.y.z>; git push origin v<x.y.z>`

```
rm dist/cassio*
make build
twine upload dist/cassio*  # (login to PyPI ...)
```

### Testing

Please run tests (and add some coverage for new features). This is not
enforced other than to your conscience. Type `make` for the available tests.

To run the full tests (except specific tests targeting Cassandra),
there's `make test-all`.

#### Unit testing

You need a virtualenv with the `requirements-dev.txt` installed.

```
make test-unit
```

#### Integration with the DB

You need a virtualenv with the `requirements-dev.txt` installed.

Ensure the required environment variables are set (see for instance
the provided `TEMPLATE.testing.env`).
You need at least one of either Astra DB or a
Cassandra (5+) cluster to use.

Launch the tests with either of:

```
make test-integration
make test-astra-integration
make test-cassandra-integration
```

The latter two above specify `TEST_DB_MODE` as either `LOCAL_CASSANDRA` or
`ASTRA_DB`. _Ideally you should test with both, since some tests are
skipped in either case._
