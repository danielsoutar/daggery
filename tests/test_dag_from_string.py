import pytest
from pydantic import ConfigDict

from daggery.dag import AnnotatedNode, FunctionDAG, InvalidGraph
from daggery.node import Node


class Foo(Node):
    model_config = ConfigDict(extra="forbid", frozen=True)

    def transform(self, value: int) -> int:
        return value * value


class Bar(Node):
    model_config = ConfigDict(extra="forbid", frozen=True)

    def transform(self, value: int) -> int:
        return value + 10


class Baz(Node):
    model_config = ConfigDict(extra="forbid", frozen=True)

    def transform(self, value: int) -> int:
        return value - 5


custom_node_map: dict[str, type[Node]] = {"foo": Foo, "bar": Bar, "baz": Baz}


def test_single_node():
    dag = FunctionDAG.from_string(
        "foo",
        custom_node_map=custom_node_map,
    )
    assert isinstance(dag, FunctionDAG)

    # Create expected instance
    expected_head = AnnotatedNode(
        naked_node=Foo(name="foo0", children=()),
        input_nodes=("__INPUT__",),
    )
    assert dag.nodes == (expected_head,)


def test_multiple_nodes():
    dag = FunctionDAG.from_string(
        "foo >> bar >> baz",
        custom_node_map=custom_node_map,
    )
    assert isinstance(dag, FunctionDAG)

    node3 = AnnotatedNode(
        naked_node=Baz(name="baz0", children=()), input_nodes=("bar0",)
    )
    node2 = AnnotatedNode(
        naked_node=Bar(name="bar0", children=(node3.naked_node,)), input_nodes=("foo0",)
    )
    node1 = AnnotatedNode(
        naked_node=Foo(name="foo0", children=(node2.naked_node,)),
        input_nodes=("__INPUT__",),
    )

    assert dag.nodes == (node1, node2, node3)

    result = dag.transform(42)
    expected_result = 1769

    assert expected_result == result


def test_multiple_nodes_of_same_type():
    dag = FunctionDAG.from_string(
        "foo >> foo >> foo",
        custom_node_map=custom_node_map,
    )
    assert isinstance(dag, FunctionDAG)

    node3 = AnnotatedNode(
        naked_node=Foo(name="foo2", children=()), input_nodes=("foo1",)
    )
    node2 = AnnotatedNode(
        naked_node=Foo(name="foo1", children=(node3.naked_node,)), input_nodes=("foo0",)
    )
    node1 = AnnotatedNode(
        naked_node=Foo(name="foo0", children=(node2.naked_node,)),
        input_nodes=("__INPUT__",),
    )

    assert dag.nodes == (node1, node2, node3)


def test_from_invalid_string():
    result = FunctionDAG.from_string(
        "foo >> invalid >> baz",
        custom_node_map=custom_node_map,
    )
    assert isinstance(result, InvalidGraph)
    assert "Invalid rule found in unvalidated DAG: invalid" in result.message


def test_empty_string():
    result = FunctionDAG.from_string(
        "",
        custom_node_map=custom_node_map,
    )
    assert isinstance(result, InvalidGraph)
    assert "DAG string is empty and therefore invalid" == result.message


def test_whitespace_only_string():
    result = FunctionDAG.from_string(
        "   ",
        custom_node_map=custom_node_map,
    )
    assert isinstance(result, InvalidGraph)
    assert "DAG string is empty and therefore invalid" == result.message


def test_cannot_create_abstract_node():
    with pytest.raises(TypeError, match="Can't instantiate abstract class Node"):
        Node()  # type: ignore
