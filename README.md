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

To develop `cassio`, we use poetry

```shell
pip install poetry
```

Use poetry to install dependencies

```shell
poetry install
```

#### Use cassio current code in other Poetry base projects

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

- Bump version in pyproject.toml
- Add to `CHANGES.txt`
- Commit the very code that will be built:
- `git tag v<x.y.z>; git push origin v<x.y.z>`

```
make build
poetry publish  # (login to PyPI ...)
```

### Testing

Please run tests (and add some coverage for new features). This is not
enforced other than to your conscience. Type `make` for the available tests.

To run the full tests (except specific tests targeting Cassandra),
there's `make test-all`.

#### Unit testing

```
make test-unit
```

#### Integration with the DB

Ensure the required environment variables are set (see for instance
the provided `TEMPLATE.testing.env`).
You need at least one of either Astra DB or a
Cassandra (5+) cluster to use.

Launch the tests with either of:

```
make test-integration

make test-astra-integration
make test-cassandra-integration
make test-testcontainerscassandra-integration
```

The last three above specify `TEST_DB_MODE` as either `LOCAL_CASSANDRA`, `TESTCONTAINERS_CASSANDRA` or
`ASTRA_DB`. Refer to `TEMPLATE.testing.env` for required environment variables in the specific cases.

_Note: Ideally you should test with both Astra DB and one Cassandra, since some tests are
skipped in either case._
