# Daggery

This mini-library exposes a set of types designed for executing graphs of operations. It supports generating synchronous and asynchronous graphs, allows custom rules, and provides wrappers for things like logging and timing.

## Exposed types

The two types currently exposed are:

* `FunctionDAG`
* `AsyncFunctionDAG`

A `FunctionDAG` represents a graph of functions, while an `AsyncFunctionDAG` represents a graph of async functions (wow!). Both can take in a graph description via a string encoding a linear sequence of operations, using the following format in the example below:

```python
class Foo(Node):
    def transform(self, value: int) -> int: return value * value

class Bar(Node):
    def transform(self, value: int) -> int: return value + 10

class Baz(Node):
    def transform(self, value: int) -> int: return value - 5

custom_node_map = {"foo": Foo, "bar": Bar, "baz": Baz}

# The below sequence can be thought of as a function composition.
# i.e. combined = foo . bar . baz, or baz(bar(foo(x)))
sequence = "foo >> bar >> baz"
dag = FunctionDAG.from_string(sequence, custom_node_map)
result = dag.transform(42)
result
# 1769
```

More generally both accept a graph description, using a topologically-sorted list of desired operations naming supported rules, and a list of argument mappings for operations with multiple inputs:

```python
# Assume corresponding nodes for `add`, `mul` (multiply) and `exp` (exponentiate).
# Assume these operations wait for some abitrary duration.
ops = OperationList(
        items=[
            Operation(
                name="add0", rule="add", children=["add1", "mul0"]
            ),
            Operation(
                name="add1", rule="add", children=["exp0"]
            ),
            Operation(
                name="mul0", rule="mul", children=["exp0"]
            ),
            Operation(
                name="exp0", rule="exp", children=[]
            ),
        ]
    )
# Only need to provide mappings when arguments are ambiguous.
mappings: list[ArgumentMappingMetadata] = [
    ArgumentMappingMetadata(
        node_name="exp0", inputs=["add1", "mul0"]  # first arg comes from `add1`, second from `mul0`.
    ),
]

dag = AsyncFunctionDAG.from_node_list(ops, mappings, custom_node_map)
result = await dag.transform(1)
# 81
```

## The Daggery Philosophy

This library adheres to the following mantras:

### Latest and greatest developer tools used wherever possible

`uv`, `mypy`, and `ruff`, as examples.

### Everything is a value, including errors - code should be exception-free

Daggery code aims to never raise Exceptions, and provides utilities for user-defined `Node`s to avoid doing so.

### Immutability is first-class.

This encourages many things like locality, safety, efficiency, and testability. Additionally it also encourages state to be decoupled and encoded explicitly, further aiding these aims.

### Leverage structure and validated types - the earlier this is done, the greater the benefits.

Structure (such as sortedness and uniqueness) gives leverage and constraints provide freedom to optimise for subsequent code. Immutability is also structure and is treated accordingly.

### Interfaces should be simple and composable. Avoid hacky gimmicks and unmaintainable approaches like multiple inheritance.

Simple code is unlikely to go wrong. Composable abstractions are scalable.

--------

## TODO:

- [ ] Add HTTP client decorator to `Node.transform`.
- [ ] Confirm graph substitution works with nested DAGs inside Operations.
- [ ] Add examples.
- [ ] Add unit tests for the above.
- [ ] Tidy up/standardise terminology and add docstrings/doc pages.
- [ ] Migrate to `uv`.
- [ ] Showcase to others.
- [ ] ???
- [ ] Profit!

## Usage

To run the service locally, run the following command:

```
poetry run uvicorn adjustment.main:app --reload
```

The service will listen on http://127.0.0.1:8000/adjustment - the provided client in client.py can be used to test the service.