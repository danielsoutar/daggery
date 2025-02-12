import asyncio
from typing import Dict

import pytest

from daggery.async_dag import AsyncDAGNode, AsyncFunctionDAG
from daggery.async_node import AsyncNode
from daggery.prevalidate import InvalidDAG


class AsyncFoo(AsyncNode, frozen=True):
    async def transform(self, value: int) -> int:
        await asyncio.sleep(0.1)
        return value * value


# The ignore is used to silence mypy - in this case we want to demonstrate
# an unsupported Node is blocked by Daggery.
# As a bonus, the default is true anyway.
class UnfrozenFoo(AsyncNode, frozen=False):  # type: ignore
    async def transform(self, value: int) -> int:
        await asyncio.sleep(0.1)
        return value * value


class AsyncPing(AsyncNode, frozen=True):
    async def transform(self, count: int) -> int:
        proc = await asyncio.create_subprocess_exec(
            "ping",
            "-c",
            str(count),
            "8.8.8.8",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        return await proc.wait()


mock_op_node_map: Dict[str, type[AsyncNode]] = {
    "foo": AsyncFoo,
    "unfrozen-foo": UnfrozenFoo,
    "ping": AsyncPing,
}


async def test_single_node():
    dag = AsyncFunctionDAG.from_string("foo", custom_op_node_map=mock_op_node_map)
    assert isinstance(dag, AsyncFunctionDAG)

    # Create expected instance
    expected_head = AsyncDAGNode(
        naked_node=AsyncFoo(name="foo0", children=()),
        input_nodes=("__INPUT__",),
    )
    assert dag.nodes == ((expected_head,),)


def test_unfrozen_node_fails():
    dag = AsyncFunctionDAG.from_string(
        "unfrozen-foo",
        custom_op_node_map=mock_op_node_map,
    )
    assert isinstance(dag, InvalidDAG)
    assert "Mutable node found in DAG" in dag.message


async def test_multiple_nodes():
    dag = AsyncFunctionDAG.from_string(
        "foo >> ping",
        custom_op_node_map=mock_op_node_map,
    )
    assert isinstance(dag, AsyncFunctionDAG)

    node2 = AsyncDAGNode(
        naked_node=AsyncPing(name="ping0", children=()), input_nodes=("foo0",)
    )
    node1 = AsyncDAGNode(
        naked_node=AsyncFoo(name="foo0", children=("ping0",)),
        input_nodes=("__INPUT__",),
    )

    assert dag.nodes == ((node1,), (node2,))


async def test_multiple_nodes_of_same_type():
    dag = AsyncFunctionDAG.from_string(
        "foo >> foo >> foo",
        custom_op_node_map=mock_op_node_map,
    )
    assert isinstance(dag, AsyncFunctionDAG)

    node3 = AsyncDAGNode(
        naked_node=AsyncFoo(name="foo2", children=()), input_nodes=("foo1",)
    )
    node2 = AsyncDAGNode(
        naked_node=AsyncFoo(name="foo1", children=("foo2",)),
        input_nodes=("foo0",),
    )
    node1 = AsyncDAGNode(
        naked_node=AsyncFoo(name="foo0", children=("foo1",)),
        input_nodes=("__INPUT__",),
    )

    assert dag.nodes == ((node1,), (node2,), (node3,))


def test_from_invalid_string():
    result = AsyncFunctionDAG.from_string(
        "foo >> invalid >> baz",
        custom_op_node_map=mock_op_node_map,
    )
    assert isinstance(result, InvalidDAG)
    assert (
        "Invalid internal node class found in prevalidated DAG: invalid"
        in result.message
    )


def test_empty_string():
    result = AsyncFunctionDAG.from_string("", custom_op_node_map={})
    assert isinstance(result, InvalidDAG)
    assert "DAG string is empty and therefore invalid" == result.message


def test_whitespace_only_string():
    result = AsyncFunctionDAG.from_string("   ", custom_op_node_map={})
    assert isinstance(result, InvalidDAG)
    assert "DAG string is empty and therefore invalid" == result.message


def test_cannot_create_abstract_async_node():
    with pytest.raises(TypeError, match="Can't instantiate abstract class AsyncNode"):
        AsyncNode()  # type: ignore
