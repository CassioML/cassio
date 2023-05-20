# cassIO

A framework-agnostic Python library to seamlessly integrate Apache Cassandra with ML/LLM/genAI workloads.

## Install in dev mode

`python setup.py -e .`

### Developing

You need the full `langchain` + `cassio` stack, i.e.

- clone `https://github.com/hemidactylus/cassio` and `pip install -e .`;
- clone `https://github.com/hemidactylus/langchain` _in the `cassio` branch_ and `pip install -e .`;


## Unit testing

In a virtualenv with the `requirements-dev.txt` installed, run:

`pytest`
