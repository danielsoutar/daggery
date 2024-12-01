import pytest

from adjustment.graph import DAG, from_string, node_map
from adjustment.node import Bar, Baz, Foo, Node, Quux, Qux


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

    # Create expected instances
    expected_head = node_map["foo"]()
    expected_second = node_map["bar"]()
    expected_third = node_map["baz"]()
    expected_head.child = expected_second
    expected_second.child = expected_third

    # Compare actual DAG with expected instances
    assert dag.head == expected_head
    assert dag.head.child == expected_second
    assert dag.head.child.child == expected_third
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
    assert isinstance(node_map["foo"], Foo.__class__)
    assert isinstance(node_map["bar"], Bar.__class__)
    assert isinstance(node_map["baz"], Baz.__class__)
    assert isinstance(node_map["qux"], Qux.__class__)
    assert isinstance(node_map["quux"], Quux.__class__)


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
