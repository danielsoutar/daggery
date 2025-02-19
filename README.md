# Daggery

This mini-library exposes a set of types designed for executing directed, acyclic graphs (DAGs) of operations. It supports generating synchronous and asynchronous DAGs, allows custom operations, and provides wrappers for things like logging and timing.

## Getting Started (Installation)

1. Ensure you have `uv` [installed in your system](https://docs.astral.sh/uv/getting-started/installation/).
2. `uv sync`
3. That's it.

For checking out the docs:

```
$ uv run mkdocs serve
```

For running tests:

```
$ uv run pytest
```

## Exposed types

The two types currently exposed are:

* `FunctionDAG`
* `AsyncFunctionDAG`

A `FunctionDAG` represents a DAG of functions, while an `AsyncFunctionDAG` represents a DAG of async functions (wow!). Both can take in a string encoding a linear sequence of operations, using the following format in the example below:

```python
# Note that frozen=True is used for all nodes - and is required by Daggery.
class Foo(Node, frozen=True):
    def transform(self, value: int) -> int: return value * value

class Bar(Node, frozen=True):
    def transform(self, value: int) -> int: return value + 10

class Baz(Node, frozen=True):
    def transform(self, value: int) -> int: return value - 5

custom_op_node_map = {"foo": Foo, "bar": Bar, "baz": Baz}

# The below sequence can be thought of as a function composition.
# i.e. combined = foo . bar . baz, or baz(bar(foo(x)))
sequence = "foo >> bar >> baz"
dag = FunctionDAG.from_string(sequence, custom_op_node_map)
if isinstance(dag, InvalidDAG):
    do_something_with_invalid_dag(dag)
result = dag.transform(42)
result
# 1769
```

More generally both accept a DAG description, using a topologically-sorted sequence of desired operations and a sequence of argument mappings for operations with multiple inputs:

```python
class AddNode(Node, frozen=True):
    def transform(self, value: float) -> float:
        return value + 1

class MultiplyNode(Node, frozen=True):
    def transform(self, value: float) -> float:
        return value * 2

class ExpNode(Node, frozen=True):
    def transform(self, base: float, exponent: float) -> float:
        return base**exponent

mock_op_node_map = {
    "add": AddNode,
    "mul": MultiplyNode,
    "exp": ExpNode,
}

ops = OperationSequence(
    ops=(
        Operation(
            name="add0", op_name="add", children=("add1", "mul0")
        ),
        Operation(
            name="add1", op_name="add", children=("exp0",)
        ),
        Operation(
            name="mul0", op_name="mul", children=("exp0",)
        ),
        Operation(
            name="exp0", op_name="exp"
        ),
    )
)
# Only need to provide mappings when arguments are ambiguous (i.e. >1 input).
# In this example, the first argument comes from `add1`, the second from `mul0`.
mapping = ArgumentMapping(op_name="exp0", inputs=("add1", "mul0"))

dag = FunctionDAG.from_dag_description(
    DAGDescription(operations=ops, argument_mappings=(mapping,)),
    custom_op_node_map,
)
if isinstance(dag, InvalidDAG):
    do_something_with_invalid_dag(dag)
result = dag.transform(1)
# 81
```

## The Daggery Philosophy

This library adheres to the following mantras:

### Latest and greatest developer tools used (correctly) wherever possible

`uv`, `mypy`, and `ruff` are all examples. Warnings are fixed immediately.

### Everything is a value, including errors - code should be exception-free

Daggery code aims to never raise Exceptions, and provides utilities for user-defined `Node`s to avoid doing so.

### Immutability is first-class.

This encourages many things like local reasoning, safety, efficiency, and testability. Additionally it also encourages state to be decoupled and encoded explicitly, further aiding these aims.

### Leverage structure and validated types - the earlier this is done, the greater the benefits.

Structure (such as sortedness and uniqueness) gives leverage and constraints provide freedom to optimise for subsequent code. Immutability is also structure and is treated accordingly.

### Interfaces should be simple and composable. Avoid hacky gimmicks and unmaintainable approaches like multiple inheritance.

Simple code is unlikely to go wrong. Composable abstractions are scalable.

--------

## TODO:

- [X] Add HTTP client decorator to `Node.transform`.
- [X] Confirm graph substitution works with nested DAGs inside Operations.
- [X] Add examples.
- [X] Add unit tests for the above.
- [X] Add `nullable_[async_]dag` and `throwable_[async_]dag` wrappers.
- [X] Migrate to `uv`.
- [X] Add docstrings/doc pages
- [ ] Tidy up/standardise terminology.
    - [ ] `Node` vs `transform` vs `Operation`.
    - [X] Different types of `Node`s.
    - [X] `OperationSequence -> OperationSequence`?
    - [X] `ArgumentMapping -> ArgumentMappingSequence`?
    - [X] `FunctionDAG -> OperationDAG`? `FunctionDAG -> DAG`?
    - [X] Tidy up graph.py with naming and data structures as well as better validation.
    - [X] Get the example with a FastAPI service working.
- [ ] Showcase to others.
- [ ] ???
- [ ] Profit!
