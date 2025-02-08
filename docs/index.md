# Daggery

Daggery is a mini-library for defining, validating, and executing graphs of user-defined functions or rules. It supports synchronous and asynchronous graphs, allows custom rules, and provides wrappers for things like logging and timing.

## Installation

## Usage



## Project layout

    daggery/            # The core library.
        dag.py          # Creating and executing synchronous graphs.
        graph.py        # Validating graph descriptions from clients.
        node.py         # Abstract classes to use for defining operations.
        description.py  # Classes to use for defining graph descriptions.
        async_dag.py    # The async equivalent of `dag.py`.
        async_node.py   # The async equivalent of `node.py`.
        utils/          # Utilities for developer use or for inspiration.
    tests/              # Unit tests for the library.
    examples/           # Some example usage patterns.