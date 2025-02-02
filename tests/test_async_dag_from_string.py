import pytest

from adjustment.async_dag import AsyncAnnotatedNode, AsyncFunctionDAG
from adjustment.async_node import AsyncFoo, AsyncPing
from adjustment.graph import AsyncNode, InvalidGraph, async_node_map


def test_single_node():
    dag = AsyncFunctionDAG.from_string("foo")
    assert isinstance(dag, AsyncFunctionDAG)

    # Create expected instance
    expected_head = AsyncAnnotatedNode(
        naked_node=AsyncFoo(name="foo0", children=()),
        input_nodes=("__INPUT__",),
    )
    assert dag.nodes == ((expected_head,),)


def test_multiple_nodes():
    dag = AsyncFunctionDAG.from_string("foo >> ping")
    assert isinstance(dag, AsyncFunctionDAG)

    node2 = AsyncAnnotatedNode(
        naked_node=AsyncPing(name="ping0", children=()), input_nodes=("foo0",)
    )
    node1 = AsyncAnnotatedNode(
        naked_node=AsyncFoo(name="foo0", children=(node2.naked_node,)),
        input_nodes=("__INPUT__",),
    )

    assert dag.nodes == ((node1,), (node2,))


def test_multiple_nodes_of_same_type():
    dag = AsyncFunctionDAG.from_string("foo >> foo >> foo")
    assert isinstance(dag, AsyncFunctionDAG)

    node3 = AsyncAnnotatedNode(
        naked_node=AsyncFoo(name="foo2", children=()), input_nodes=("foo1",)
    )
    node2 = AsyncAnnotatedNode(
        naked_node=AsyncFoo(name="foo1", children=(node3.naked_node,)),
        input_nodes=("foo0",),
    )
    node1 = AsyncAnnotatedNode(
        naked_node=AsyncFoo(name="foo0", children=(node2.naked_node,)),
        input_nodes=("__INPUT__",),
    )

    assert dag.nodes == ((node1,), (node2,), (node3,))


def test_from_invalid_string():
    result = AsyncFunctionDAG.from_string("foo >> invalid >> baz")
    assert isinstance(result, InvalidGraph)
    assert "Invalid rule found in unvalidated DAG: invalid" in result.message


def test_empty_string():
    result = AsyncFunctionDAG.from_string("")
    assert isinstance(result, InvalidGraph)
    assert "DAG string is empty and therefore invalid" == result.message


def test_whitespace_only_string():
    result = AsyncFunctionDAG.from_string("   ")
    assert isinstance(result, InvalidGraph)
    assert "DAG string is empty and therefore invalid" == result.message


def test_async_node_map():
    assert "foo" in async_node_map
    assert "ping" in async_node_map
    assert async_node_map["foo"] is AsyncFoo
    assert async_node_map["ping"] is AsyncPing


def test_cannot_create_abstract_async_node():
    with pytest.raises(TypeError, match="Can't instantiate abstract class AsyncNode"):
        AsyncNode()  # type: ignore
