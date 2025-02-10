import asyncio
import math

import pytest

from daggery.async_dag import AsyncFunctionDAG
from daggery.async_node import AsyncNode
from daggery.description import (
    ArgumentMapping,
    DAGDescription,
    Operation,
    OperationSequence,
)


class AddAsyncNode(AsyncNode, frozen=True):
    async def transform(self, value: float) -> float:
        await asyncio.sleep(0.05)
        return value + 1


class MultiplyAsyncNode(AsyncNode, frozen=True):
    async def transform(self, value: float) -> float:
        await asyncio.sleep(0.1)
        return value * 2


class ExpAsyncNode(AsyncNode, frozen=True):
    async def transform(self, base: float, exponent: float) -> float:
        await asyncio.sleep(0.1)
        return base**exponent


class SineAsyncNode(AsyncNode, frozen=True):
    async def transform(self, value: float) -> float:
        await asyncio.sleep(0.05)
        return math.sin(value)


class MaxAsyncNode(AsyncNode, frozen=True):
    # Handily, nodes support arbitrarily-sized arguments. And as
    # it turns out, type hinting supports variadic single-type
    # arguments too!
    async def transform(self, *args: float) -> float:
        await asyncio.sleep(0.05)
        return max(args)


class SumAsyncNode(AsyncNode, frozen=True):
    # Handily, nodes support arbitrarily-sized arguments. And as
    # it turns out, type hinting supports variadic single-type
    # arguments too!
    async def transform(self, *args: int) -> int:
        await asyncio.sleep(0.05)
        return sum(args)


class MutableHeadNode(AsyncNode, frozen=True):
    async def transform(self, value: list) -> list:
        await asyncio.sleep(0.05)
        return value


class MutableANode(AsyncNode, frozen=True):
    async def transform(self, value: list) -> list:
        await asyncio.sleep(0.01)
        value.append("A")
        return value


class MutableBNode(AsyncNode, frozen=True):
    async def transform(self, value: list) -> list:
        await asyncio.sleep(0.03)
        value.append("B")
        return value


class MutableCNode(AsyncNode, frozen=True):
    async def transform(self, value: list) -> list:
        await asyncio.sleep(0.05)
        value.append("C")
        return value


class MutableTailNode(AsyncNode, frozen=True):
    async def transform(self, *lists: list) -> list:
        await asyncio.sleep(0.05)
        return [v for sublist in lists for v in sublist]


mock_op_node_map = {
    "add": AddAsyncNode,
    "mul": MultiplyAsyncNode,
    "exp": ExpAsyncNode,
    "sin": SineAsyncNode,
    "max": MaxAsyncNode,
    "sum": SumAsyncNode,
    "mut_head": MutableHeadNode,
    "mut_a": MutableANode,
    "mut_b": MutableBNode,
    "mut_c": MutableCNode,
    "mut_tail": MutableTailNode,
}


@pytest.mark.asyncio
async def test_free_node_insertable_anywhere():
    # We have a fixed graph with simple connectivity, and one 'free' node
    # that takes the head node as input. By inserting this node in every
    # possible position in the list, we show that all topological sortings
    # are supported.
    num_batches = 6
    adds = ["add" + str(i) for i in range(1, num_batches - 1)]
    muls = ["mul" + str(i) for i in range(num_batches - 2)]
    # By interleaving the `add` and `mul` nodes we can batch nodes together.
    # Otherwise we have a largely sequential computation.
    # Based on the above node timings this roughly halves the time taken.
    names = ["add0"] + [v for pair in zip(adds, muls) for v in pair] + ["max0"]
    rules = [name[:3] for name in names]
    child_as = [["add" + str(i)] for i in range(2, num_batches - 1)]
    child_ms = [["mul" + str(i)] for i in range(1, num_batches - 2)]
    interleaved_children = [v for pair in zip(child_as, child_ms) for v in pair]
    all_children = [
        ["sin0", "add1", "mul0"],
        *interleaved_children,
        ["max0"],
        ["max0"],
        [],
    ]
    for offset in range(1, len(names) - 1):
        full_names = names[:offset] + ["sin0"] + names[offset:]
        full_rules = rules[:offset] + ["sin"] + rules[offset:]
        full_all_children = all_children[:offset] + [["max0"]] + all_children[offset:]
        ops = OperationSequence(
            ops=tuple(
                Operation(name=name, op_name=rule, children=tuple(children))
                for name, rule, children in zip(
                    full_names, full_rules, full_all_children
                )
            )
        )
        mappings = (
            ArgumentMapping(
                op_name="max0",
                inputs=(
                    "sin0",
                    "add" + str(num_batches - 2),
                    "mul" + str(num_batches - 3),
                ),
            ),
        )
        dag = AsyncFunctionDAG.from_dag_description(
            DAGDescription(operations=ops, argument_mappings=mappings),
            custom_op_node_map=mock_op_node_map,
        )
        assert isinstance(dag, AsyncFunctionDAG)
        expected_num_batches = num_batches
        assert expected_num_batches == len(dag.nodes)
        # Ensure that sin0 is in its expected batch and position.
        # We unroll the tuple of tuples into a flat list and check
        # sin0 is at the same offset.
        unrolled_nodes = [node for batch in dag.nodes for node in batch]
        expected_pos = offset
        assert "sin0" == unrolled_nodes[expected_pos].naked_node.name

        actual_output = await dag.transform(1)
        # The multiply 'branch' will be the largest.
        expected_output = 2 ** (num_batches - 1)
        assert actual_output == expected_output


@pytest.mark.asyncio
async def test_transitive_closure_graph():
    names = ["sum" + str(i) for i in range(6)]
    rules = ["sum"] * len(names)
    all_children = [names[i + 1 :] for i in range(len(names))]
    all_parents = [names[:i] for i in range(len(names))]
    ops = OperationSequence(
        ops=tuple(
            Operation(name=name, op_name=rule, children=tuple(children))
            for name, rule, children in zip(names, rules, all_children)
        )
    )
    # Each node retrieves values from all of its ancestors, though the
    # ordering doesn't matter in this case since addition is commutative.
    mappings = tuple(
        ArgumentMapping(op_name=name, inputs=tuple(parents))
        for name, parents in zip(names, all_parents)
    )
    dag = AsyncFunctionDAG.from_dag_description(
        DAGDescription(operations=ops, argument_mappings=mappings),
        custom_op_node_map=mock_op_node_map,
    )
    assert isinstance(dag, AsyncFunctionDAG)
    # This graph has every node pointing to all of its successor nodes.
    # Despite the extensive connectivity pattern, this effectively makes
    # the graph a linear sequence (in terms of evaluation order) and so
    # we expect each batch to be of size 1.
    assert all(len(batch) == 1 for batch in dag.nodes)

    # If the graph were linear you'd get an identity (sum(1) == 1).
    # However, each node feeds into all of its successors.
    # So the graph would be:
    # sum(1) -> sum(1, 1) -> sum(1, 2, 1) -> ... -> sum(1..N, 1)
    # We have 6 nodes, so the result should be sum(1..5, 1) = 15 + 1 = 16.
    actual_output = await dag.transform(1)
    expected_output = 16
    assert expected_output == actual_output


@pytest.mark.asyncio
async def test_mutable_arguments_are_dangerous():
    ops = OperationSequence(
        ops=(
            Operation(
                name="mut_head",
                op_name="mut_head",
                children=("mut_a", "mut_b", "mut_c"),
            ),
            Operation(name="mut_a", op_name="mut_a", children=("mut_tail",)),
            Operation(name="mut_b", op_name="mut_b", children=("mut_tail",)),
            Operation(name="mut_c", op_name="mut_c", children=("mut_tail",)),
            Operation(name="mut_tail", op_name="mut_tail"),
        )
    )
    mappings = (
        ArgumentMapping(op_name="mut_tail", inputs=("mut_a", "mut_b", "mut_c")),
    )
    dag = AsyncFunctionDAG.from_dag_description(
        DAGDescription(operations=ops, argument_mappings=mappings),
        custom_op_node_map=mock_op_node_map,
    )
    assert isinstance(dag, AsyncFunctionDAG)
    actual_output = await dag.transform([])
    # The list is *mutated* across nodes unexpectedly - mutability implies state,
    # and state should be sequential! DO NOT USE BRANCHING FOR STATEFUL NODES.
    expected_output = ["A", "B", "C"] * 3
    assert expected_output == actual_output
