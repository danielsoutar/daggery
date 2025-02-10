# Daggery

Daggery is a mini-library for defining, validating, and executing DAGs of user-defined functions. It supports synchronous and asynchronous DAGs, allows custom operations, and provides wrappers for things like logging and timing.

## Installation

### TODO

## Usage

For synchronous DAGs, use `FunctionDAG` like so - the following example shows a linear sequence of operations encoded using a string:

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
result = dag.transform(42)
result
# 1769
```

The `AsyncFunctionDAG` class uses a similar interface.

By contrast, both can also take in a more general `DAGDescription` encoding more complex graphs:

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

## Project layout

    daggery/            # The core library.
    tests/              # Unit tests for the library.
    examples/           # Some example usage patterns.