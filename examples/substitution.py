from daggery.dag import FunctionDAG
from daggery.description import ArgumentMappingMetadata, Operation, OperationList
from daggery.graph import InvalidGraph
from daggery.node import Node


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
        rules = ["foo_internal", "qux", "quux", "combined"]
        all_children = [["qux", "quux"], ["combined"], ["combined"], []]
        dag = FunctionDAG.from_node_list(
            graph_description=OperationList(
                items=[
                    Operation(name=name, rule=rule, children=children)
                    for name, rule, children in zip(names, rules, all_children)
                ]
            ),
            argument_mappings=[
                ArgumentMappingMetadata(node_name="combined", inputs=["qux", "quux"])
            ],
            custom_node_map={
                "foo_internal": FooHeadInternal,
                "qux": FooQuxInternal,
                "quux": FooQuuxInternal,
                "combined": FooCombinedInternal,
            },
        )
        if isinstance(dag, InvalidGraph):
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
    dag = FunctionDAG.from_node_list(
        graph_description=OperationList(
            items=[
                Operation(name="foo", rule="foo", children=["bar"]),
                Operation(name="bar", rule="bar", children=["baz"]),
                Operation(name="baz", rule="baz", children=[]),
            ]
        ),
        argument_mappings=[],
        custom_node_map={
            "foo": FooExternal,
            "bar": BarExternal,
            "baz": BazExternal,
        },
    )
    # Casting to None allows us to use the walrus operator in calling code.
    # TODO: Consider whether this comparison should be supported in error types.
    return None if isinstance(dag, InvalidGraph) else dag


def main():
    if dag := construct_dag():
        print(dag.transform(42))
    else:
        print("Error when constructing DAG.")


if __name__ == "__main__":
    main()
