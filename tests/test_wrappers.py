import asyncio

import pytest

from daggery.async_dag import AsyncFunctionDAG
from daggery.async_node import AsyncNode
from daggery.dag import FunctionDAG
from daggery.description import DAGDescription, Operation, OperationSequence
from daggery.node import Node


class Foo(Node, frozen=True):
    def transform(self, value: int) -> int:
        return value * value


class AsyncFoo(AsyncNode, frozen=True):
    async def transform(self, value: int) -> int:
        await asyncio.sleep(0.1)
        return value * value


mock_op_node_map: dict[str, type[Node]] = {"foo": Foo}
custom_async_op_node_map: dict[str, type[AsyncNode]] = {"foo": AsyncFoo}


def test_function_dag_nullable_from_string():
    dag_description = "foo"

    # Positive case
    dag = FunctionDAG.nullable_from_string(dag_description, mock_op_node_map)
    assert isinstance(dag, FunctionDAG)

    # Negative case
    invalid_dag_description = "invalid"
    dag = FunctionDAG.nullable_from_string(invalid_dag_description, mock_op_node_map)
    assert dag is None


def test_function_dag_throwable_from_string():
    dag_description = "foo"

    # Positive case
    dag = FunctionDAG.throwable_from_string(dag_description, mock_op_node_map)
    assert isinstance(dag, FunctionDAG)

    # Negative case
    invalid_dag_description = "invalid"
    with pytest.raises(ValueError):
        FunctionDAG.throwable_from_string(invalid_dag_description, mock_op_node_map)


def test_async_function_dag_nullable_from_string():
    dag_description = "foo"

    # Positive case
    dag = AsyncFunctionDAG.nullable_from_string(
        dag_description, custom_async_op_node_map
    )
    assert isinstance(dag, AsyncFunctionDAG)

    # Negative case
    invalid_dag_description = "invalid"
    dag = AsyncFunctionDAG.nullable_from_string(
        invalid_dag_description, custom_async_op_node_map
    )
    assert dag is None


def test_async_function_dag_throwable_from_string():
    dag_description = "foo"

    # Positive case
    dag = AsyncFunctionDAG.throwable_from_string(
        dag_description, custom_async_op_node_map
    )
    assert isinstance(dag, AsyncFunctionDAG)

    # Negative case
    invalid_dag_description = "invalid"
    with pytest.raises(Exception):
        AsyncFunctionDAG.throwable_from_string(
            invalid_dag_description, custom_async_op_node_map
        )


def test_function_dag_nullable_from_dag_description():
    operations = OperationSequence(ops=(Operation(name="foo", op_name="foo"),))

    # Positive case
    dag = FunctionDAG.nullable_from_dag_description(
        DAGDescription(operations=operations), mock_op_node_map
    )
    assert isinstance(dag, FunctionDAG)

    # Negative case
    invalid_operations = OperationSequence(
        ops=(Operation(name="foo", op_name="invalid"),)
    )
    dag = FunctionDAG.nullable_from_dag_description(
        DAGDescription(operations=invalid_operations), mock_op_node_map
    )
    assert dag is None


def test_function_dag_throwable_from_dag_description():
    operations = OperationSequence(ops=(Operation(name="foo", op_name="foo"),))

    # Positive case
    dag = FunctionDAG.throwable_from_dag_description(
        DAGDescription(operations=operations), mock_op_node_map
    )
    assert isinstance(dag, FunctionDAG)

    # Negative case
    invalid_operations = OperationSequence(
        ops=(Operation(name="foo", op_name="invalid"),)
    )
    with pytest.raises(ValueError):
        FunctionDAG.throwable_from_dag_description(
            DAGDescription(operations=invalid_operations), mock_op_node_map
        )


def test_async_function_dag_nullable_from_dag_description():
    operations = OperationSequence(ops=(Operation(name="foo", op_name="foo"),))

    # Positive case
    dag = AsyncFunctionDAG.nullable_from_dag_description(
        DAGDescription(operations=operations), custom_async_op_node_map
    )
    assert isinstance(dag, AsyncFunctionDAG)

    # Negative case
    invalid_operations = OperationSequence(
        ops=(Operation(name="foo", op_name="invalid"),)
    )
    dag = AsyncFunctionDAG.nullable_from_dag_description(
        DAGDescription(operations=invalid_operations), custom_async_op_node_map
    )
    assert dag is None


def test_async_function_dag_throwable_from_dag_description():
    operations = OperationSequence(ops=(Operation(name="foo", op_name="foo"),))

    # Positive case
    dag = AsyncFunctionDAG.throwable_from_dag_description(
        DAGDescription(operations=operations), custom_async_op_node_map
    )
    assert isinstance(dag, AsyncFunctionDAG)

    # Negative case
    invalid_operations = OperationSequence(
        ops=(Operation(name="foo", op_name="invalid"),)
    )
    with pytest.raises(Exception):
        AsyncFunctionDAG.throwable_from_dag_description(
            DAGDescription(operations=invalid_operations), custom_async_op_node_map
        )
