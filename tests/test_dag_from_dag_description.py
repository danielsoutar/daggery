from daggery.dag import DAGNode, FunctionDAG
from daggery.description import (
    ArgumentMapping,
    DAGDescription,
    Operation,
    OperationSequence,
)
from daggery.node import Node
from daggery.prevalidate import InvalidDAG, PrevalidatedDAG


class AddNode(Node, frozen=True):
    def transform(self, value: float) -> float:
        return value + 1


class MultiplyNode(Node, frozen=True):
    def transform(self, value: float) -> float:
        return value * 2


class ExpNode(Node, frozen=True):
    def transform(self, base: float, exponent: float) -> float:
        return base**exponent


mock_op_node_map = {
    "add": AddNode,
    "mul": MultiplyNode,
    "exp": ExpNode,
}


def test_single_node():
    ops = OperationSequence(ops=(Operation(name="add", op_name="add"),))
    dag = FunctionDAG.from_dag_description(
        dag_description=DAGDescription(operations=ops),
        custom_op_node_map=mock_op_node_map,
    )
    assert isinstance(dag, FunctionDAG)

    expected_head = DAGNode(
        naked_node=AddNode(name="add", children=()),
        input_nodes=("__INPUT__",),
    )

    assert dag.nodes == (expected_head,)


def test_diamond_structure():
    ops = OperationSequence(
        ops=(
            Operation(name="add0", op_name="add", children=("add1", "mul0")),
            Operation(name="add1", op_name="add", children=("exp0",)),
            Operation(name="mul0", op_name="mul", children=("exp0",)),
            Operation(name="exp0", op_name="exp"),
        )
    )
    mappings = (ArgumentMapping(op_name="exp0", inputs=("add1", "mul0")),)
    dag = FunctionDAG.from_dag_description(
        DAGDescription(operations=ops, argument_mappings=mappings),
        custom_op_node_map=mock_op_node_map,
    )
    # The mathematical operation performed is (noting node definitions above):
    # > exp(add(add(1)), multiply(add(1)))
    # = 81
    assert isinstance(dag, FunctionDAG)
    actual_output = dag.transform(1)
    expected_output = 81
    assert actual_output == expected_output


def test_from_invalid_dag_description():
    result = FunctionDAG.from_dag_description(
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


def test_split_level_structure():
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
    prevalidated_dag = PrevalidatedDAG.from_dag_description(
        DAGDescription(operations=ops, argument_mappings=mappings)
    )
    assert isinstance(prevalidated_dag, PrevalidatedDAG)
    dag = FunctionDAG.from_prevalidated_dag(
        prevalidated_dag, custom_op_node_map=mock_op_node_map
    )
    assert isinstance(dag, FunctionDAG)
    actual_output = dag.transform(1)
    expected_output = (2**4) ** 4
    assert actual_output == expected_output
