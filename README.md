# cassIO

A framework-agnostic Python library to seamlessly integrate Apache Cassandra with ML/LLM/genAI workloads.

## Developing

### Developing

To develop `cassio`, use the `requirements-dev.txt`.

To use the dev version in an integration (e.g. your branch of LangChain),

- `pip install -e .` in this `cassio`;
- `pip install -e .` in the LangChain `branch`;
- plus any additional requirement files specific to the examples you're running (such as Jupyter)

## Unit testing

In a virtualenv with the `requirements-dev.txt` installed, run:

`pytest`
