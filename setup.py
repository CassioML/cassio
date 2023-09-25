from setuptools import setup, find_packages
import pathlib

here = pathlib.Path(__file__).parent.resolve()

setup(
    name="cassio",
    version="0.1.2",
    author="Stefano Lottini",
    author_email="stefano.lottini@datastax.com",
    package_dir={"": "src"},
    package_data={"cassio": ["py.typed"]},
    include_package_data=True,
    packages=find_packages(where="src"),
    entry_points={
        "console_scripts": [
            # Command-line utility to generate init strings
            "cassio-create-init-string=cassio.config.bundle_management:create_init_string_utility",
        ],
    },
    url="https://github.com/hemidactylus/cassio",
    license="LICENSE.txt",
    description=(
        "A framework-agnostic Python library to seamlessly integrate "
        "Apache Cassandra(R) with ML/LLM/genAI workloads."
    ),
    long_description=(here / "README.md").read_text(encoding="utf-8"),
    long_description_content_type="text/markdown",
    install_requires=[
        "numpy>=1.0",
        "cassandra-driver>=3.28.0",
    ],
    python_requires=">=3.8",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        #
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3 :: Only",
        "Topic :: Scientific/Engineering",
        "Topic :: Scientific/Engineering :: Artificial Intelligence",
    ],
    keywords="cassandra, ai, llm, genai, astradb",
)
