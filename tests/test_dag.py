import pytest
from pydantic import ConfigDict

from adjustment.dag import DAG, InvalidGraph, UnvalidatedDAG, node_map
from adjustment.node import Bar, Baz, Foo, Node, Quux, Qux
from adjustment.parse import UnvalidatedNode


class AddNode(Node):
    model_config = ConfigDict(extra="forbid", frozen=True)

    def transform(self, value: float) -> float:
        return value + 1


class MultiplyNode(Node):
    model_config = ConfigDict(extra="forbid", frozen=True)

    def transform(self, value: float) -> float:
        return value * 2


def test_single_node():
    dag = DAG.from_input("foo")
    assert isinstance(dag, DAG)

    # Create expected instance
    expected_head = Foo(name="foo0", children=[])

    # Compare actual DAG with expected instance
    assert dag.head == expected_head
    assert dag.head.children == []


def test_multiple_nodes():
    dag = DAG.from_input("foo >> bar >> baz")
    assert isinstance(dag, DAG)

    # Create expected instances using back-to-front construction
    expected_third = Baz(name="baz0", children=[])
    expected_second = Bar(name="bar0", children=[expected_third])
    expected_head = Foo(name="foo0", children=[expected_second])

    # Compare actual DAG with expected instances
    assert dag.head == expected_head
    assert dag.head.children == [expected_second]
    assert dag.head.children[0].children == [expected_third]
    assert dag.head.children[0].children[0].children == []


def test_from_invalid_string():
    result = DAG.from_input("foo >> invalid >> baz")
    assert isinstance(result, InvalidGraph)
    assert result.message == "Invalid rule found in unvalidated DAG: invalid"


def test_node_map():
    assert "foo" in node_map
    assert "bar" in node_map
    assert "baz" in node_map
    assert "qux" in node_map
    assert "quux" in node_map
    assert node_map["foo"] is Foo
    assert node_map["bar"] is Bar
    assert node_map["baz"] is Baz
    assert node_map["qux"] is Qux
    assert node_map["quux"] is Quux


def test_cannot_create_abstract_node():
    with pytest.raises(TypeError, match="Can't instantiate abstract class Node"):
        Node()  # type: ignore


def test_factory_blocks_cycles():
    test_node = {"name": "foo0", "rule": "foo", "children": ["foo0"]}

    # Attempt to create a DAG
    result = DAG.from_input(input_data=[test_node])
    assert isinstance(result, InvalidGraph)
    assert "Input is not topologically sorted" in result.message
