import pytest
from pydantic import ConfigDict

from adjustment.node import Bar, Baz, Foo, Node, Quux, Qux
from adjustment.sequence import FunctionSequence, InvalidSequence, node_map


class AddNode(Node):
    model_config = ConfigDict(extra="forbid", frozen=True)

    def transform(self, value: float) -> float:
        return value + 1


class MultiplyNode(Node):
    model_config = ConfigDict(extra="forbid", frozen=True)

    def transform(self, value: float) -> float:
        return value * 2


def test_single_node():
    dag = FunctionSequence.from_string("foo")
    assert isinstance(dag, FunctionSequence)

    # Create expected instance
    expected_head = Foo(name="foo0", children=())

    # Compare actual FunctionSequence with expected instance
    assert dag.head == expected_head
    assert dag.head.children == ()


def test_multiple_nodes():
    dag = FunctionSequence.from_string("foo >> bar >> baz")
    assert isinstance(dag, FunctionSequence)

    # Create expected instances using back-to-front construction
    expected_third = Baz(name="baz0", children=())
    expected_second = Bar(name="bar0", children=(expected_third,))
    expected_head = Foo(name="foo0", children=(expected_second,))

    # Compare actual FunctionSequence with expected instances
    assert dag.head == expected_head
    assert dag.head.children == (expected_second,)
    assert dag.head.children[0].children == (expected_third,)
    assert dag.head.children[0].children[0].children == ()


def test_multiple_nodes_of_same_type():
    dag = FunctionSequence.from_string("foo >> foo >> foo")
    assert isinstance(dag, FunctionSequence)

    # Create expected instances using back-to-front construction
    expected_third = Foo(name="foo2", children=())
    expected_second = Foo(name="foo1", children=(expected_third,))
    expected_head = Foo(name="foo0", children=(expected_second,))

    # Compare actual FunctionSequence with expected instances
    assert dag.head == expected_head
    assert dag.head.children == (expected_second,)
    assert dag.head.children[0].children == (expected_third,)
    assert dag.head.children[0].children[0].children == ()


def test_from_invalid_string():
    result = FunctionSequence.from_string("foo >> invalid >> baz")
    assert isinstance(result, InvalidSequence)
    assert (
        result.message == "Invalid rule found in unvalidated FunctionSequence: invalid"
    )


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


# def test_factory_blocks_cycles():
#     test_node = {"name": "foo0", "rule": "foo", "children": ["foo0"]}

#     # Attempt to create a FunctionSequence
#     result = FunctionSequence.from_node_list(input_data=[test_node])
#     assert isinstance(result, InvalidGraph)
#     assert "Input is not topologically sorted" in result.message
