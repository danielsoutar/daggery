import asyncio

import pytest

from daggery.async_dag import AsyncDAGNode, AsyncFunctionDAG
from daggery.async_node import AsyncNode
from daggery.description import (
    ArgumentMapping,
    DAGDescription,
    Operation,
    OperationSequence,
)
from daggery.prevalidate import InvalidDAG


class AddAsyncNode(AsyncNode, frozen=True):
    async def transform(self, value: float) -> float:
        await asyncio.sleep(0.1)
        return value + 1


class MultiplyAsyncNode(AsyncNode, frozen=True):
    async def transform(self, value: float) -> float:
        await asyncio.sleep(0.2)
        return value * 2


class ExpAsyncNode(AsyncNode, frozen=True):
    async def transform(self, base: float, exponent: float) -> float:
        await asyncio.sleep(0.2)
        return base**exponent


mock_op_node_map = {
    "add": AddAsyncNode,
    "mul": MultiplyAsyncNode,
    "exp": ExpAsyncNode,
}


@pytest.mark.asyncio
async def test_single_node():
    ops = OperationSequence(ops=(Operation(name="add", op_name="add"),))
    dag = AsyncFunctionDAG.from_dag_description(
        dag_description=DAGDescription(operations=ops),
        custom_op_node_map=mock_op_node_map,
    )
    assert isinstance(dag, AsyncFunctionDAG)

    expected_head = AsyncDAGNode(
        naked_node=AddAsyncNode(name="add", children=()),
        input_nodes=("__INPUT__",),
    )

    assert dag.nodes == ((expected_head,),)
    actual_output = await dag.transform(1)
    expected_output = 2
    assert actual_output == expected_output


@pytest.mark.asyncio
async def test_diamond_structure():
    ops = OperationSequence(
        ops=(
            Operation(name="add0", op_name="add", children=("add1", "mul0")),
            Operation(name="add1", op_name="add", children=("exp0",)),
            Operation(name="mul0", op_name="mul", children=("exp0",)),
            Operation(name="exp0", op_name="exp"),
        )
    )
    mappings = (ArgumentMapping(op_name="exp0", inputs=("add1", "mul0")),)
    dag = AsyncFunctionDAG.from_dag_description(
        dag_description=DAGDescription(operations=ops, argument_mappings=mappings),
        custom_op_node_map=mock_op_node_map,
    )
    # The mathematical operation performed is (noting node definitions above):
    # > exp(add(add(1)), multiply(add(1)))
    # = 81
    assert isinstance(dag, AsyncFunctionDAG)
    actual_output = await dag.transform(1)
    expected_output = 81
    assert actual_output == expected_output


def test_from_invalid_dag_description():
    result = AsyncFunctionDAG.from_dag_description(
        dag_description=DAGDescription(
            operations=OperationSequence(
                ops=(
                    Operation(name="head-foo", op_name="foo"),
                    Operation(name="another-head-foo", op_name="foo"),
                )
            ),
            argument_mappings=(ArgumentMapping(op_name="head-foo"),),
        ),
        custom_op_node_map=mock_op_node_map,
    )
    assert isinstance(result, InvalidDAG)
    assert "Input has >1 root node:" in result.message


@pytest.mark.asyncio
async def test_split_level_structure():
    ops = OperationSequence(
        ops=(
            Operation(name="add0", op_name="add", children=("exp0", "mul0", "add1")),
            Operation(name="add1", op_name="add", children=("add2",)),
            Operation(name="mul0", op_name="mul", children=("exp0",)),
            Operation(name="add2", op_name="add", children=("exp1",)),
            Operation(name="exp0", op_name="exp", children=("exp1",)),
            Operation(name="exp1", op_name="exp"),
        )
    )
    #  ----- add0 -----
    #  |      |       |
    #  |     mul0    add1
    # exp0 ---|       |
    #  |             add2
    #  |------|-------|
    #        exp1
    mappings = (
        ArgumentMapping(op_name="exp0", inputs=("add0", "mul0")),
        ArgumentMapping(op_name="exp1", inputs=("exp0", "add2")),
    )
    dag = AsyncFunctionDAG.from_dag_description(
        dag_description=DAGDescription(operations=ops, argument_mappings=mappings),
        custom_op_node_map=mock_op_node_map,
    )
    assert isinstance(dag, AsyncFunctionDAG)
    actual_output = await dag.transform(1)
    expected_output = (2**4) ** 4
    assert actual_output == expected_output
