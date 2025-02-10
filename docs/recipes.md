# Recipes for using Daggery

## Nested DAGs

TODO

## Checking batching (async DAGs only)

TODO

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

TODO

