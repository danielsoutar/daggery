import pytest
from pydantic import ConfigDict

from adjustment.dag import DAG, from_string, node_map
from adjustment.node import Bar, Baz, Foo, Node, Quux, Qux


class AddNode(Node):
    model_config = ConfigDict(extra="forbid", frozen=True)

    def transform(self, value: int) -> int:
        return value + 1


class MultiplyNode(Node):
    model_config = ConfigDict(extra="forbid", frozen=True)

    def transform(self, value: int) -> int:
        return value * 2


def test_from_valid_string():
    dag_string = "foo >> bar >> baz"
    dag = from_string(dag_string)
    assert isinstance(dag, DAG)

    # Create expected instances using back-to-front construction
    expected_third = node_map["baz"]()
    expected_second = node_map["bar"](child=expected_third)
    expected_head = node_map["foo"](child=expected_second)

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


def test_cannot_create_abstract_node():
    with pytest.raises(TypeError, match="Can't instantiate abstract class Node"):
        Node()


def test_multiple_operators():
    # Create nodes back-to-front.
    node3 = AddNode()
    node2 = MultiplyNode(child=node3)
    node1 = AddNode(child=node2)

    # Create DAG
    dag = DAG(head=node1)

    # Test transformation
    result = dag.transform(3)

    # (3 + 1) * 2 + 1 = 9
    assert result == 9
