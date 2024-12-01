import pytest

from adjustment.graph import DAG, from_string, node_map
from adjustment.node import Node


class AddNode(Node):
    def transform(self, value: int) -> int:
        return value + 1


class MultiplyNode(Node):
    def transform(self, value: int) -> int:
        return value * 2


def test_from_valid_string():
    dag_string = "foo >> bar >> baz"
    dag = from_string(dag_string)
    assert isinstance(dag, DAG)
    assert dag.head == node_map["foo"]
    assert dag.head.child == node_map["bar"]
    assert dag.head.child.child == node_map["baz"]
    assert dag.head.child.child.child is None


def test_from_invalid_string():
    dag_string = "foo >> invalid >> baz"
    with pytest.raises(ValueError, match="Invalid node name encountered"):
        from_string(dag_string)


def test_node_map():
    assert "foo" in node_map
    assert "bar" in node_map
    assert "baz" in node_map
    assert "qux" in node_map
    assert "quux" in node_map
    assert isinstance(node_map["foo"], node_map["foo"].__class__)
    assert isinstance(node_map["bar"], node_map["bar"].__class__)
    assert isinstance(node_map["baz"], node_map["baz"].__class__)
    assert isinstance(node_map["qux"], node_map["qux"].__class__)
    assert isinstance(node_map["quux"], node_map["quux"].__class__)


def test_multiple_operators():
    # Create nodes
    node1 = AddNode()
    node2 = MultiplyNode()
    node3 = AddNode()

    # Link nodes
    node1.child = node2
    node2.child = node3

    # Create DAG
    dag = DAG(head=node1)

    # Test transformation
    result = dag.transform(3)

    # (3 + 1) * 2 + 1 = 9
    assert result == 9
