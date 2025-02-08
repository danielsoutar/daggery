from pydantic import ConfigDict

from daggery.dag import DAGNode, FunctionDAG
from daggery.graph import PrevalidatedDAG
from daggery.node import Node
from daggery.request import ArgumentMappingMetadata, Operation, OperationList


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
    "add": AddNode,
    "mul": MultiplyNode,
    "exp": ExpNode,
}


def test_single_node():
    ops = OperationList(items=[Operation(name="add", rule="add", children=[])])
    mappings: list[ArgumentMappingMetadata] = []
    dag = FunctionDAG.from_node_list(
        graph_description=ops,
        argument_mappings=mappings,
        custom_node_map=mock_node_map,
    )
    assert isinstance(dag, FunctionDAG)

    expected_head = DAGNode(
        naked_node=AddNode(name="add", children=()),
        input_nodes=("__INPUT__",),
    )

    assert dag.nodes == (expected_head,)


def test_diamond_structure():
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
    dag = FunctionDAG.from_node_list(
        graph_description=ops,
        argument_mappings=mappings,
        custom_node_map=mock_node_map,
    )
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
    prevalidated_dag = PrevalidatedDAG.from_node_list(ops, mappings)
    assert isinstance(prevalidated_dag, PrevalidatedDAG)
    dag = FunctionDAG.from_prevalidated_dag(
        prevalidated_dag, custom_node_map=mock_node_map
    )
    assert isinstance(dag, FunctionDAG)
    actual_output = dag.transform(1)
    expected_output = (2**4) ** 4
    assert actual_output == expected_output
