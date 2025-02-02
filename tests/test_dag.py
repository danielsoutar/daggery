from pydantic import ConfigDict

from adjustment.dag import AnnotatedNode, FunctionDAG
from adjustment.graph import PrevalidatedDAG
from adjustment.node import Node
from adjustment.request import ArgumentMappingMetadata, Operation, OperationList


class AddNode(Node):
    model_config = ConfigDict(extra="forbid", frozen=True)

    def transform(self, value: float) -> float:
        return value + 1


class MultiplyNode(Node):
    model_config = ConfigDict(extra="forbid", frozen=True)

    def transform(self, value: float) -> float:
        return value * 2


class ExpNode(Node):
    model_config = ConfigDict(extra="forbid", frozen=True)

    def transform(self, base: float, exponent: float) -> float:
        return base**exponent


mock_node_map = {
    "exp": ExpNode,
    "add": AddNode,
    "multiply": MultiplyNode,
}


def test_single_node():
    ops = OperationList(items=[Operation(name="add", rule="add", children=[])])
    mappings: list[ArgumentMappingMetadata] = []
    prevalidated_dag = PrevalidatedDAG.from_node_list(ops, mappings)
    assert isinstance(prevalidated_dag, PrevalidatedDAG)
    dag = FunctionDAG.from_prevalidated_dag(prevalidated_dag, node_map=mock_node_map)
    assert isinstance(dag, FunctionDAG)

    expected_head = AnnotatedNode(
        naked_node=AddNode(name="add", children=()),
        input_values=(),
        output_value="add",
    )

    assert dag.nodes == (expected_head,)


def test_diamond_structure():
    ops = OperationList(
        items=[
            Operation(name="add0", rule="add", children=["add1", "multiply"]),
            Operation(name="add1", rule="add", children=["exp"]),
            Operation(name="multiply", rule="multiply", children=["exp"]),
            Operation(name="exp", rule="exp", children=[]),
        ]
    )
    mappings: list[ArgumentMappingMetadata] = [
        ArgumentMappingMetadata(node_name="exp", inputs=["add1", "multiply"]),
    ]
    prevalidated_dag = PrevalidatedDAG.from_node_list(ops, mappings)
    assert isinstance(prevalidated_dag, PrevalidatedDAG)
    dag = FunctionDAG.from_prevalidated_dag(prevalidated_dag, node_map=mock_node_map)
    # The mathematical operation performed is (noting node definitions above):
    # > exp(add(add(1)), multiply(add(1)))
    # = 81
    assert isinstance(dag, FunctionDAG)
    actual_output = dag.transform(1)
    expected_output = 81
    assert actual_output == expected_output


def test_split_level_structure():
    ops = OperationList(
        items=[
            Operation(name="add0", rule="add", children=["exp0", "multiply", "add1"]),
            Operation(name="add1", rule="add", children=["add2"]),
            Operation(name="multiply", rule="multiply", children=["exp0"]),
            Operation(name="add2", rule="add", children=["exp1"]),
            Operation(name="exp0", rule="exp", children=["exp1"]),
            Operation(name="exp1", rule="exp", children=[]),
        ]
    )
    #  ----- add0 -----
    #  |      |       |
    #  |   multiply  add1
    # exp0 ---|       |
    #  |             add2
    #  |------|-------|
    #        exp1
    mappings: list[ArgumentMappingMetadata] = [
        ArgumentMappingMetadata(node_name="exp0", inputs=["add0", "multiply"]),
        ArgumentMappingMetadata(node_name="exp1", inputs=["exp0", "add2"]),
    ]
    prevalidated_dag = PrevalidatedDAG.from_node_list(ops, mappings)
    assert isinstance(prevalidated_dag, PrevalidatedDAG)
    dag = FunctionDAG.from_prevalidated_dag(prevalidated_dag, node_map=mock_node_map)
    assert isinstance(dag, FunctionDAG)
    actual_output = dag.transform(1)
    expected_output = (2**4) ** 4
    assert actual_output == expected_output
