import asyncio

import pytest
from pydantic import ConfigDict

from daggery.async_dag import AsyncFunctionDAG
from daggery.async_node import AsyncNode
from daggery.dag import FunctionDAG
from daggery.description import Operation, OperationList
from daggery.node import Node


class Foo(Node):
    model_config = ConfigDict(frozen=True)

    def transform(self, value: int) -> int:
        return value * value


class AsyncFoo(AsyncNode):
    model_config = ConfigDict(frozen=True)

    async def transform(self, value: int) -> int:
        await asyncio.sleep(0.1)
        return value * value


custom_node_map: dict[str, type[Node]] = {"foo": Foo}
custom_async_node_map: dict[str, type[AsyncNode]] = {"foo": AsyncFoo}


def test_function_dag_nullable_from_string():
    graph_description = "foo"

    # Positive case
    dag = FunctionDAG.nullable_from_string(graph_description, custom_node_map)
    assert isinstance(dag, FunctionDAG)

    # Negative case
    invalid_graph_description = "invalid"
    dag = FunctionDAG.nullable_from_string(invalid_graph_description, custom_node_map)
    assert dag is None


def test_function_dag_throwable_from_string():
    graph_description = "foo"

    # Positive case
    dag = FunctionDAG.throwable_from_string(graph_description, custom_node_map)
    assert isinstance(dag, FunctionDAG)

    # Negative case
    invalid_graph_description = "invalid"
    with pytest.raises(ValueError):
        FunctionDAG.throwable_from_string(invalid_graph_description, custom_node_map)


def test_async_function_dag_nullable_from_string():
    graph_description = "foo"

    # Positive case
    dag = AsyncFunctionDAG.nullable_from_string(
        graph_description, custom_async_node_map
    )
    assert isinstance(dag, AsyncFunctionDAG)

    # Negative case
    invalid_graph_description = "invalid"
    dag = AsyncFunctionDAG.nullable_from_string(
        invalid_graph_description, custom_async_node_map
    )
    assert dag is None


def test_async_function_dag_throwable_from_string():
    graph_description = "foo"

    # Positive case
    dag = AsyncFunctionDAG.throwable_from_string(
        graph_description, custom_async_node_map
    )
    assert isinstance(dag, AsyncFunctionDAG)

    # Negative case
    invalid_graph_description = "invalid"
    with pytest.raises(Exception):
        AsyncFunctionDAG.throwable_from_string(
            invalid_graph_description, custom_async_node_map
        )


def test_function_dag_nullable_from_node_list():
    operations = OperationList(items=[Operation(name="foo", rule="foo", children=[])])

    # Positive case
    dag = FunctionDAG.nullable_from_node_list(operations, [], custom_node_map)
    assert isinstance(dag, FunctionDAG)

    # Negative case
    invalid_operations = OperationList(
        items=[Operation(name="foo", rule="invalid", children=[])]
    )
    dag = FunctionDAG.nullable_from_node_list(invalid_operations, [], custom_node_map)
    assert dag is None


def test_function_dag_throwable_from_node_list():
    operations = OperationList(items=[Operation(name="foo", rule="foo", children=[])])

    # Positive case
    dag = FunctionDAG.throwable_from_node_list(operations, [], custom_node_map)
    assert isinstance(dag, FunctionDAG)

    # Negative case
    invalid_operations = OperationList(
        items=[Operation(name="foo", rule="invalid", children=[])]
    )
    with pytest.raises(ValueError):
        FunctionDAG.throwable_from_node_list(invalid_operations, [], custom_node_map)


def test_async_function_dag_nullable_from_node_list():
    operations = OperationList(items=[Operation(name="foo", rule="foo", children=[])])

    # Positive case
    dag = AsyncFunctionDAG.nullable_from_node_list(
        operations, [], custom_async_node_map
    )
    assert isinstance(dag, AsyncFunctionDAG)

    # Negative case
    invalid_operations = OperationList(
        items=[Operation(name="foo", rule="invalid", children=[])]
    )
    dag = AsyncFunctionDAG.nullable_from_node_list(
        invalid_operations, [], custom_async_node_map
    )
    assert dag is None


def test_async_function_dag_throwable_from_node_list():
    operations = OperationList(items=[Operation(name="foo", rule="foo", children=[])])

    # Positive case
    dag = AsyncFunctionDAG.throwable_from_node_list(
        operations, [], custom_async_node_map
    )
    assert isinstance(dag, AsyncFunctionDAG)

    # Negative case
    invalid_operations = OperationList(
        items=[Operation(name="foo", rule="invalid", children=[])]
    )
    with pytest.raises(Exception):
        AsyncFunctionDAG.throwable_from_node_list(
            invalid_operations, [], custom_async_node_map
        )
