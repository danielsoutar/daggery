import asyncio
from typing import Dict

import pytest
from pydantic import ConfigDict

from daggery.async_dag import AsyncAnnotatedNode, AsyncFunctionDAG
from daggery.async_node import AsyncNode
from daggery.graph import InvalidGraph


class AsyncFoo(AsyncNode):
    model_config = ConfigDict(extra="forbid", frozen=True)

    async def transform(self, value: int) -> int:
        await asyncio.sleep(1)
        return value * value


class AsyncPing(AsyncNode):
    model_config = ConfigDict(extra="forbid", frozen=True)

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


mock_node_map: Dict[str, type[AsyncNode]] = {"foo": AsyncFoo, "ping": AsyncPing}


async def test_single_node():
    dag = AsyncFunctionDAG.from_string("foo", custom_node_map=mock_node_map)
    assert isinstance(dag, AsyncFunctionDAG)

    # Create expected instance
    expected_head = AsyncAnnotatedNode(
        naked_node=AsyncFoo(name="foo0", children=()),
        input_nodes=("__INPUT__",),
    )
    assert dag.nodes == ((expected_head,),)


async def test_multiple_nodes():
    dag = AsyncFunctionDAG.from_string(
        "foo >> ping",
        custom_node_map=mock_node_map,
    )
    assert isinstance(dag, AsyncFunctionDAG)

    node2 = AsyncAnnotatedNode(
        naked_node=AsyncPing(name="ping0", children=()), input_nodes=("foo0",)
    )
    node1 = AsyncAnnotatedNode(
        naked_node=AsyncFoo(name="foo0", children=(node2.naked_node,)),
        input_nodes=("__INPUT__",),
    )

    assert dag.nodes == ((node1,), (node2,))


async def test_multiple_nodes_of_same_type():
    dag = AsyncFunctionDAG.from_string(
        "foo >> foo >> foo",
        custom_node_map=mock_node_map,
    )
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
    result = AsyncFunctionDAG.from_string(
        "foo >> invalid >> baz",
        custom_node_map=mock_node_map,
    )
    assert isinstance(result, InvalidGraph)
    assert "Invalid rule found in unvalidated DAG: invalid" in result.message


def test_empty_string():
    result = AsyncFunctionDAG.from_string("", custom_node_map={})
    assert isinstance(result, InvalidGraph)
    assert "DAG string is empty and therefore invalid" == result.message


def test_whitespace_only_string():
    result = AsyncFunctionDAG.from_string("   ", custom_node_map={})
    assert isinstance(result, InvalidGraph)
    assert "DAG string is empty and therefore invalid" == result.message


def test_cannot_create_abstract_async_node():
    with pytest.raises(TypeError, match="Can't instantiate abstract class AsyncNode"):
        AsyncNode()  # type: ignore
