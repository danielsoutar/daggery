import asyncio

import pytest
from pydantic import ConfigDict

from daggery.async_dag import AsyncAnnotatedNode, AsyncFunctionDAG
from daggery.async_node import AsyncNode
from daggery.request import ArgumentMappingMetadata, Operation, OperationList


class AddAsyncNode(AsyncNode):
    model_config = ConfigDict(extra="forbid", frozen=True)

    async def transform(self, value: float) -> float:
        await asyncio.sleep(1)
        return value + 1


class MultiplyAsyncNode(AsyncNode):
    model_config = ConfigDict(extra="forbid", frozen=True)

    async def transform(self, value: float) -> float:
        await asyncio.sleep(2)
        return value * 2


class ExpAsyncNode(AsyncNode):
    model_config = ConfigDict(extra="forbid", frozen=True)

    async def transform(self, base: float, exponent: float) -> float:
        await asyncio.sleep(2)
        return base**exponent


mock_node_map = {
    "add": AddAsyncNode,
    "mul": MultiplyAsyncNode,
    "exp": ExpAsyncNode,
}


@pytest.mark.asyncio
async def test_single_node():
    ops = OperationList(items=[Operation(name="add", rule="add", children=[])])
    mappings: list[ArgumentMappingMetadata] = []
    dag = AsyncFunctionDAG.from_node_list(
        graph_description=ops,
        argument_mappings=mappings,
        custom_node_map=mock_node_map,
    )
    assert isinstance(dag, AsyncFunctionDAG)

    expected_head = AsyncAnnotatedNode(
        naked_node=AddAsyncNode(name="add", children=()),
        input_nodes=("__INPUT__",),
    )

    assert dag.nodes == ((expected_head,),)
    actual_output = await dag.transform(1)
    expected_output = 2
    assert actual_output == expected_output


@pytest.mark.asyncio
async def test_diamond_structure():
    ops = OperationList(
        items=[
            Operation(name="add0", rule="add", children=["add1", "mul0"]),
            Operation(name="add1", rule="add", children=["exp0"]),
            Operation(name="mul0", rule="mul", children=["exp0"]),
            Operation(name="exp0", rule="exp", children=[]),
        ]
    )
    mappings: list[ArgumentMappingMetadata] = [
        ArgumentMappingMetadata(node_name="exp0", inputs=["add1", "mul0"]),
    ]
    dag = AsyncFunctionDAG.from_node_list(
        graph_description=ops,
        argument_mappings=mappings,
        custom_node_map=mock_node_map,
    )
    # The mathematical operation performed is (noting node definitions above):
    # > exp(add(add(1)), multiply(add(1)))
    # = 81
    assert isinstance(dag, AsyncFunctionDAG)
    actual_output = await dag.transform(1)
    expected_output = 81
    assert actual_output == expected_output


@pytest.mark.asyncio
async def test_split_level_structure():
    ops = OperationList(
        items=[
            Operation(name="add0", rule="add", children=["exp0", "mul0", "add1"]),
            Operation(name="add1", rule="add", children=["add2"]),
            Operation(name="mul0", rule="mul", children=["exp0"]),
            Operation(name="add2", rule="add", children=["exp1"]),
            Operation(name="exp0", rule="exp", children=["exp1"]),
            Operation(name="exp1", rule="exp", children=[]),
        ]
    )
    #  ----- add0 -----
    #  |      |       |
    #  |     mul0    add1
    # exp0 ---|       |
    #  |             add2
    #  |------|-------|
    #        exp1
    mappings: list[ArgumentMappingMetadata] = [
        ArgumentMappingMetadata(node_name="exp0", inputs=["add0", "mul0"]),
        ArgumentMappingMetadata(node_name="exp1", inputs=["exp0", "add2"]),
    ]
    dag = AsyncFunctionDAG.from_node_list(
        graph_description=ops,
        argument_mappings=mappings,
        custom_node_map=mock_node_map,
    )
    assert isinstance(dag, AsyncFunctionDAG)
    actual_output = await dag.transform(1)
    expected_output = (2**4) ** 4
    assert actual_output == expected_output
