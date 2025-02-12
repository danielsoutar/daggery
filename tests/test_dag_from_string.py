import pytest

from daggery.dag import DAGNode, FunctionDAG, InvalidDAG
from daggery.node import Node


class Foo(Node, frozen=True):
    def transform(self, value: int) -> int:
        return value * value


class Bar(Node, frozen=True):
    def transform(self, value: int) -> int:
        return value + 10


class Baz(Node, frozen=True):
    def transform(self, value: int) -> int:
        return value - 5


# The ignore is used to silence mypy - in this case we want to demonstrate
# an unsupported Node is blocked by Daggery.
# As a bonus, the default is true anyway.
class UnfrozenFoo(Node, frozen=False):  # type: ignore
    def transform(self, value: int) -> int:
        return value * value


mock_op_node_map: dict[str, type[Node]] = {
    "foo": Foo,
    "bar": Bar,
    "baz": Baz,
    "unfrozen-foo": UnfrozenFoo,
}


def test_single_node():
    dag = FunctionDAG.from_string(
        "foo",
        custom_op_node_map=mock_op_node_map,
    )
    assert isinstance(dag, FunctionDAG)

    # Create expected instance
    expected_head = DAGNode(
        naked_node=Foo(name="foo0", children=()),
        input_nodes=("__INPUT__",),
    )
    assert dag.nodes == (expected_head,)


def test_unfrozen_node_fails():
    dag = FunctionDAG.from_string(
        "unfrozen-foo",
        custom_op_node_map=mock_op_node_map,
    )
    assert isinstance(dag, InvalidDAG)

    assert "Mutable node found in DAG" in dag.message


def test_multiple_nodes():
    dag = FunctionDAG.from_string(
        "foo >> bar >> baz",
        custom_op_node_map=mock_op_node_map,
    )
    assert isinstance(dag, FunctionDAG)

    node3 = DAGNode(naked_node=Baz(name="baz0", children=()), input_nodes=("bar0",))
    node2 = DAGNode(
        naked_node=Bar(name="bar0", children=("baz0",)), input_nodes=("foo0",)
    )
    node1 = DAGNode(
        naked_node=Foo(name="foo0", children=("bar0",)),
        input_nodes=("__INPUT__",),
    )

    assert dag.nodes == (node1, node2, node3)

    result = dag.transform(42)
    expected_result = 1769

    assert expected_result == result


def test_multiple_nodes_of_same_type():
    dag = FunctionDAG.from_string(
        "foo >> foo >> foo",
        custom_op_node_map=mock_op_node_map,
    )
    assert isinstance(dag, FunctionDAG)

    node3 = DAGNode(naked_node=Foo(name="foo2", children=()), input_nodes=("foo1",))
    node2 = DAGNode(
        naked_node=Foo(name="foo1", children=("foo2",)), input_nodes=("foo0",)
    )
    node1 = DAGNode(
        naked_node=Foo(name="foo0", children=("foo1",)),
        input_nodes=("__INPUT__",),
    )

    assert dag.nodes == (node1, node2, node3)


def test_from_invalid_string():
    result = FunctionDAG.from_string(
        "foo >> invalid >> baz",
        custom_op_node_map=mock_op_node_map,
    )
    assert isinstance(result, InvalidDAG)
    assert (
        "Invalid internal node class found in prevalidated DAG: invalid"
        in result.message
    )


def test_empty_string():
    result = FunctionDAG.from_string(
        "",
        custom_op_node_map=mock_op_node_map,
    )
    assert isinstance(result, InvalidDAG)
    assert "DAG string is empty and therefore invalid" == result.message


def test_whitespace_only_string():
    result = FunctionDAG.from_string(
        "   ",
        custom_op_node_map=mock_op_node_map,
    )
    assert isinstance(result, InvalidDAG)
    assert "DAG string is empty and therefore invalid" == result.message


def test_cannot_create_abstract_node():
    with pytest.raises(TypeError, match="Can't instantiate abstract class Node"):
        Node()  # type: ignore
