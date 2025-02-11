from daggery.dag import FunctionDAG
from daggery.description import (
    ArgumentMapping,
    DAGDescription,
    Operation,
    OperationSequence,
)
from daggery.node import Node
from daggery.prevalidate import InvalidDAG


class FooHeadInternal(Node, frozen=True):
    def transform(self, value):
        return value


class FooQuxInternal(Node, frozen=True):
    def transform(self, value):
        return value


class FooQuuxInternal(Node, frozen=True):
    def transform(self, value):
        return value


class FooCombinedInternal(Node, frozen=True):
    def transform(self, a, b):
        return a * b


class FooExternal(Node, frozen=True):
    def transform(self, value):
        names = ["foo_internal", "qux", "quux", "combined"]
        op_names = ["foo_internal", "qux", "quux", "combined"]
        all_children: list[tuple] = [
            ("qux", "quux"),
            ("combined",),
            ("combined",),
            (),
        ]
        dag = FunctionDAG.from_dag_description(
            dag_description=DAGDescription(
                operations=OperationSequence(
                    ops=tuple(
                        Operation(name=name, op_name=op_name, children=children)
                        for name, op_name, children in zip(
                            names, op_names, all_children
                        )
                    )
                ),
                argument_mappings=(
                    ArgumentMapping(op_name="combined", inputs=("qux", "quux")),
                ),
            ),
            custom_op_node_map={
                "foo_internal": FooHeadInternal,
                "qux": FooQuxInternal,
                "quux": FooQuuxInternal,
                "combined": FooCombinedInternal,
            },
        )
        if isinstance(dag, InvalidDAG):
            return 0
        else:
            return dag.transform(value)


class BarExternal(Node, frozen=True):
    def transform(self, value):
        return value + 10


class BazExternal(Node, frozen=True):
    def transform(self, value):
        return value - 5


def construct_dag() -> FunctionDAG | None:
    """
    This code demonstrates the usage of nested DAGs.

    Because the DAG contains a nested DAG, the `transform` method of `FooExternal`
    is effectively substituted for a diamond-shaped graph of `Foo***Internal` nodes.

    This can be a useful pattern for decoupling a high-level set of Operations
    provided to your users, and internally to model those Operations as complex
    directed graphs.

    It goes without saying that this plays well with AsyncDAGs, and is especially
    useful in those scenarios. Daggery DAGs do not rely on state, so these graphs,
    nested or otherwise, are also thread-safe.
    """
    return FunctionDAG.nullable_from_dag_description(
        dag_description=DAGDescription(
            operations=OperationSequence(
                ops=(
                    Operation(name="foo", op_name="foo", children=("bar",)),
                    Operation(name="bar", op_name="bar", children=("baz",)),
                    Operation(name="baz", op_name="baz"),
                )
            )
        ),
        custom_op_node_map={
            "foo": FooExternal,
            "bar": BarExternal,
            "baz": BazExternal,
        },
    )


def main():
    if dag := construct_dag():
        print(dag.transform(42))
    else:
        print("Error when constructing DAG.")


if __name__ == "__main__":
    main()
