import pytest
from pydantic import ConfigDict

from adjustment.dag import DAG, node_map
from adjustment.node import Bar, Baz, Foo, Node, Quux, Qux


class AddNode(Node):
    model_config = ConfigDict(extra="forbid", frozen=True)

    def transform(self, value: float) -> float:
        return value + 1


class MultiplyNode(Node):
    model_config = ConfigDict(extra="forbid", frozen=True)

    def transform(self, value: float) -> float:
        return value * 2


def test_single_node():
    dag_string = "foo"
    dag = DAG(dag_string)
    assert isinstance(dag, DAG)

    # Create expected instance
    expected_head = Foo(child=None)

    # Compare actual DAG with expected instance
    assert dag.head == expected_head
    assert dag.head.child is None


def test_multiple_nodes():
    dag_string = "foo >> bar >> baz"
    dag = DAG(dag_string)
    assert isinstance(dag, DAG)

    # Create expected instances using back-to-front construction
    expected_third = Baz(child=None)
    expected_second = Bar(child=expected_third)
    expected_head = Foo(child=expected_second)

    # Compare actual DAG with expected instances
    assert dag.head == expected_head
    assert dag.head.child == expected_second
    assert dag.head.child.child == expected_third
    assert dag.head.child.child.child is None


def test_from_invalid_string():
    dag_string = "foo >> invalid >> baz"
    with pytest.raises(ValueError, match="Invalid node name encountered"):
        DAG(dag_string)


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
