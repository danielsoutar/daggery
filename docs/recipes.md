# Recipes for using Daggery

Daggery has a few characteristics and features that are worth pointing out, for more sophisticated usage:

## Nested DAGs

One common issue with graphs is *substitution*.

For example, you may want to provide a high-level set of operations that clients (be it your own code, other programmers or users) can compose together. This grants controlled flexibility while managing complexity.

One obvious problem is that clients may want high-level and *complex* operations. It may make sense to model these as sub-graphs. However, you want to enable separation of concerns, and as a service provider may not want to leak the internal implementation to clients.

In Daggery, this is supported via composition (the below snippet is from the substitution example):

```python
class FooExternal(Node, frozen=True):
    def transform(self, value):
        # Create a diamond graph.
        names = ["foo_internal", "qux", "quux", "combined"]
        op_names = ["foo_internal", "qux", "quux", "combined"]
        all_children: list[tuple] = [...]
        dag = FunctionDAG.throwable_from_dag_description(
            dag_description=DAGDescription(
                operations=OperationSequence(ops=...),
                argument_mappings=...,
            ),
            # Assume these nodes are internally defined in your service.
            custom_op_node_map={
                "foo_internal": FooHeadInternal,
                "qux": FooQuxInternal,
                "quux": FooQuuxInternal,
                "combined": FooCombinedInternal,
            },
        )
```

In this example we simply spark off another DAG within a Node. This also plays well in the asynchronous case. The advantage of this is that clients can use and understand a DAG in terms of nodes like `FooExternal`, without needing to know the internal implementation of nodes like `FooQuxInternal`.

A limitation of this is that graphs cannot currently be *reordered* or *optimised* by Daggery - only substitution of an operation/node with a sub-graph is supported. The former is a viable feature in the short-term, the latter is harder and would be more involved. This is because Daggery by design doesn't know the internals of the graph. However: if a sequence of optimisations was passed to Daggery along with a graph description, it is possible Daggery could act as a miniature graph-compiler in the future.

## Checking batching (async DAGs only)

In Daggery currently, the policy for evaluating graphs asynchronously is via *batches*.

Batches of tasks are sparked off and jointly waited on (for the implementation of this, see `async_dag.py`). The advantage of this approach is that it is simple and low-overhead, but the main trade-off is that Daggery only ensures valid batching - not that the batching is fast!

For seeing how nodes are batched, the `.nodes` field of an `AsyncFunctionDAG` contains a tuple of tuples (this graph derives from the 'free insertable node' example in the tests):

```python
dag.nodes
# (
#   ...('add0')...,
#   ...('sin0', 'add1', 'mul0')...,
#   ...('add2', 'mul1')...,
#   ...('add3', 'mul2')...,
#   ...('add4', 'mul3')...,
#   ...('max0')...,
```

In this example the batches are (tied) optimal - due to the connections, we can't group contiguous nodes better than this. By contrast however, we could have a pathological input like this (and note that in both cases the input is topologically sorted if we flattened the sequence):

```python
dag.nodes
# (
#   ...('add0')...,
#   ...('add1')...,
#   ...('add2')...,
#   ...('add3')...,
#   ...('add4')...,
#   ...('mul0')...,
#   ...('mul1')...,
#   ...('mul2')...,
#   ...('mul3', 'sin0')...,
#   ...('max0')...,
```

The sequential nature of the computation is implied by the larger number of batches and that they are size 1. Given the branching there must be at least one batch with more than 1 node, but assuming equal time per node, we would nearly double the amount of time if this was an asynchronous graph.

Of course, this says nothing about how long each node takes. But seeing how these batches are laid out and figuring out timings could be helpful in determining good performance with batching.

## `nullable` and `throwable` DAG construction

If preferred, graph construction can be nullable, or throw exceptions, rather than returning the error as a value:

```python
# Requires an isinstance check.
dag = FunctionDAG.from_dag_description(...)
if isinstance(dag, FunctionDAG):
    do_something_with_dag(dag)
else:
    do_something_with_invalid_dag(dag)
# Can be used in an assignment expression, i.e. the walrus operator.
if dag := FunctionDAG.nullable_from_dag_description(...):
    do_something_with_dag(dag)
else:
    do_something_with_invalid_dag(dag)
# Requires no extra checks, but throws exceptions instead.
dag = FunctionDAG.throwable_from_dag_description(...)
```

## Decorators for Nodes (`logged`, `timed`, etc)

Although Nodes can be arbitrary functions, a fair question might be how to integrate things like logging, timing, tracing, as well as other functionality.

Basic as they may be, Daggery provides a few starter decorators as inspiration for developers (these are lifted from Daggery's utilties):

```python
def logged(logger):
    def decorator(method):
        @wraps(method)
        def wrapper(self, *args, **kwargs):
            logger.info(f"{self.name}:")
            logger.info(f"  args: {args}")
            logger.info(f"  kwargs: {kwargs}")
            output = method(self, *args, **kwargs)
            logger.info(f"  Output: {output}")
            return output
        return wrapper
    return decorator
```

This exemplifies a basic logging decorator that can be attached to the transform of a Node:

```python

class Foo(Node, frozen=True):
    @logged(logger)
    def transform(self, value: int) -> int:
        ...
```

There are more interesting examples, however. We can use dependency injection to decouple things like HTTP clients from functions, making them more testable:

```python
def http_client(base_url):
    def client(ep, pl):
        return requests.post(base_url + ep, json=pl)

    def decorator(method):
        @wraps(method)
        def wrapper(self, *args, **kwargs):
            return method(self, *args, client, **kwargs)
        return wrapper
    return decorator
```

Again, basic - but potentially inspiration for clever tricks!

The final one is an example of how Daggery supports exception-free code:

```python
def bypass(error_types, logger):
    def decorator(method):
        @wraps(method)
        def wrapper(self, *args, **kwargs):
            # kwargs are not propagated by Operations, so
            # just checking args is sufficient.
            if any(isinstance(arg, error_types) for arg in args):
                logger.info(f"{self.name} bypassed.")
                # If multiple errors, return the first one.
                return next(filter(lambda a: isinstance(a, error_types), args))
            return method(self, *args, **kwargs)
        return wrapper
    return decorator
```

In this decorator we can skip, or 'bypass' a Node's transform entirely if one of the inputs matches an error type we accept, and the first error is returned. This enables Nodes to be decoupled from each other in having to know anything about errors.

Deeper integration with these decorators could be a viable option in the future with injected contexts, or more besides!
