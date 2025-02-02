from adjustment.graph import (
    EmptyDAG,
    InvalidGraph,
    PrevalidatedDAG,
    PrevalidatedNode,
)
from adjustment.request import ArgumentMappingMetadata, Operation, OperationList


def test_prevalidated_dag_from_string_single_node():
    dag_string = "foo"
    expected_output = PrevalidatedDAG(
        nodes=[
            PrevalidatedNode(
                name="foo0",
                rule="foo",
                children=[],
                input_values=[],
                output_value="foo0",
            ),
        ]
    )
    assert PrevalidatedDAG.from_string(dag_string) == expected_output


def test_prevalidated_dag_from_string_multiple_nodes():
    dag_string = "foo >> bar >> baz"
    expected_output = PrevalidatedDAG(
        nodes=[
            PrevalidatedNode(
                name="foo0",
                rule="foo",
                children=["bar0"],
                input_values=[],
                output_value="foo0",
            ),
            PrevalidatedNode(
                name="bar0",
                rule="bar",
                children=["baz0"],
                input_values=["foo0"],
                output_value="bar0",
            ),
            PrevalidatedNode(
                name="baz0",
                rule="baz",
                children=[],
                input_values=["bar0"],
                output_value="baz0",
            ),
        ]
    )
    assert PrevalidatedDAG.from_string(dag_string) == expected_output


def test_prevalidated_dag_from_string_empty_string():
    dag_string = ""
    expected_output = EmptyDAG(message="DAG string is empty and therefore invalid")
    assert PrevalidatedDAG.from_string(dag_string) == expected_output


def test_prevalidated_dag_from_node_list_single_node():
    operations = OperationList(items=[Operation(name="foo", rule="foo", children=[])])
    argument_mappings = [ArgumentMappingMetadata(node_name="foo", inputs=[])]
    expected_output = PrevalidatedDAG(
        nodes=[
            PrevalidatedNode(
                name="foo",
                rule="foo",
                children=[],
                input_values=[],
                output_value="foo",
            ),
        ]
    )
    assert (
        PrevalidatedDAG.from_node_list(operations, argument_mappings) == expected_output
    )


def test_prevalidated_dag_from_node_list_multiple_nodes_multiple_heads():
    operations = OperationList(
        items=[
            Operation(name="foo", rule="foo", children=["baz"]),
            Operation(name="bar", rule="bar", children=["baz"]),
            Operation(name="baz", rule="baz", children=[]),
        ]
    )
    argument_mappings = [
        ArgumentMappingMetadata(node_name="baz", inputs=["foo", "bar"]),
    ]
    actual = PrevalidatedDAG.from_node_list(operations, argument_mappings)
    assert isinstance(actual, InvalidGraph)


def test_prevalidated_dag_from_node_list_multiple_nodes_multiple_tails():
    operations = OperationList(
        items=[
            Operation(name="foo", rule="foo", children=["bar", "baz"]),
            Operation(name="bar", rule="bar", children=[]),
            Operation(name="baz", rule="baz", children=[]),
        ]
    )
    argument_mappings: list[ArgumentMappingMetadata] = []
    actual = PrevalidatedDAG.from_node_list(operations, argument_mappings)
    assert isinstance(actual, InvalidGraph)


def test_prevalidated_dag_from_node_list_multiple_nodes_duplicate_names():
    operations = OperationList(
        items=[
            Operation(name="foo", rule="foo", children=["foo"]),
            Operation(name="foo", rule="foo", children=["bar"]),
            Operation(name="bar", rule="bar", children=[]),
        ]
    )
    argument_mappings: list[ArgumentMappingMetadata] = []
    actual = PrevalidatedDAG.from_node_list(operations, argument_mappings)
    assert isinstance(actual, InvalidGraph)


def test_prevalidated_dag_from_node_list_multiple_nodes_no_mappings_given():
    operations = OperationList(
        items=[
            Operation(name="foo", rule="foo", children=["bar"]),
            Operation(name="bar", rule="bar", children=["baz"]),
            Operation(name="baz", rule="baz", children=[]),
        ]
    )
    # Because we have a linear sequence, mappings are unambigious. So we don't
    # need to specify them.
    argument_mappings: list[ArgumentMappingMetadata] = []
    expected_output = PrevalidatedDAG(
        nodes=[
            PrevalidatedNode(
                name="foo",
                rule="foo",
                children=["bar"],
                input_values=[],
                output_value="foo",
            ),
            PrevalidatedNode(
                name="bar",
                rule="bar",
                children=["baz"],
                input_values=["foo"],
                output_value="bar",
            ),
            PrevalidatedNode(
                name="baz",
                rule="baz",
                children=[],
                input_values=["bar"],
                output_value="baz",
            ),
        ]
    )
    assert (
        PrevalidatedDAG.from_node_list(operations, argument_mappings) == expected_output
    )


def test_prevalidated_dag_from_node_list_multiple_nodes_invalid_mappings_given():
    operations = OperationList(
        items=[
            Operation(name="foo", rule="foo", children=["bar"]),
            Operation(name="bar", rule="bar", children=["baz"]),
            Operation(name="baz", rule="baz", children=[]),
        ]
    )
    # Although specifying redundant mappings isn't an error, specifying
    # invalid mappings is. So we catch that case here.
    argument_mappings = [ArgumentMappingMetadata(node_name="bar", inputs=[])]
    actual = PrevalidatedDAG.from_node_list(operations, argument_mappings)
    assert isinstance(actual, InvalidGraph)


def test_prevalidated_dag_from_node_list_multiple_nodes_some_mappings_given():
    operations = OperationList(
        items=[
            Operation(name="foo", rule="foo", children=["bar"]),
            Operation(name="bar", rule="bar", children=["baz"]),
            Operation(name="baz", rule="baz", children=[]),
        ]
    )
    # We don't need to specify all mappings, instead we can just specify those
    # we think are ambigious. In this case there aren't any, but over-specifying
    # is not treated as an error.
    argument_mappings = [
        ArgumentMappingMetadata(node_name="bar", inputs=["foo"]),
        ArgumentMappingMetadata(node_name="baz", inputs=["bar"]),
    ]
    expected_output = PrevalidatedDAG(
        nodes=[
            PrevalidatedNode(
                name="foo",
                rule="foo",
                children=["bar"],
                input_values=[],
                output_value="foo",
            ),
            PrevalidatedNode(
                name="bar",
                rule="bar",
                children=["baz"],
                input_values=["foo"],
                output_value="bar",
            ),
            PrevalidatedNode(
                name="baz",
                rule="baz",
                children=[],
                input_values=["bar"],
                output_value="baz",
            ),
        ]
    )
    assert (
        PrevalidatedDAG.from_node_list(operations, argument_mappings) == expected_output
    )


def test_prevalidated_dag_from_node_list_multiple_nodes_all_mappings_given():
    operations = OperationList(
        items=[
            Operation(name="foo", rule="foo", children=["bar"]),
            Operation(name="bar", rule="bar", children=["baz"]),
            Operation(name="baz", rule="baz", children=[]),
        ]
    )
    # We don't need to specify all mappings, instead we can just specify those
    # we think are ambigious. In this case there aren't any, but over-specifying
    # is not treated as an error.
    argument_mappings = [
        ArgumentMappingMetadata(node_name="foo", inputs=[]),
        ArgumentMappingMetadata(node_name="bar", inputs=["foo"]),
        ArgumentMappingMetadata(node_name="baz", inputs=["bar"]),
    ]
    expected_output = PrevalidatedDAG(
        nodes=[
            PrevalidatedNode(
                name="foo",
                rule="foo",
                children=["bar"],
                input_values=[],
                output_value="foo",
            ),
            PrevalidatedNode(
                name="bar",
                rule="bar",
                children=["baz"],
                input_values=["foo"],
                output_value="bar",
            ),
            PrevalidatedNode(
                name="baz",
                rule="baz",
                children=[],
                input_values=["bar"],
                output_value="baz",
            ),
        ]
    )
    assert (
        PrevalidatedDAG.from_node_list(operations, argument_mappings) == expected_output
    )


def test_prevalidated_dag_from_node_list_diamond_structure_no_mappings_given():
    operations = OperationList(
        items=[
            Operation(name="foo", rule="foo", children=["bar", "baz"]),
            Operation(name="bar", rule="bar", children=["qux"]),
            Operation(name="baz", rule="baz", children=["qux"]),
            Operation(name="qux", rule="qux", children=[]),
        ]
    )
    argument_mappings: list[ArgumentMappingMetadata] = []
    actual = PrevalidatedDAG.from_node_list(operations, argument_mappings)
    assert isinstance(actual, InvalidGraph)


def test_prevalidated_dag_from_node_list_diamond_structure_all_mappings_given():
    operations = OperationList(
        items=[
            Operation(name="foo", rule="foo", children=["bar", "baz"]),
            Operation(name="bar", rule="bar", children=["qux"]),
            Operation(name="baz", rule="baz", children=["qux"]),
            Operation(name="qux", rule="qux", children=[]),
        ]
    )
    argument_mappings = [
        ArgumentMappingMetadata(node_name="foo", inputs=[]),
        ArgumentMappingMetadata(node_name="bar", inputs=["foo"]),
        ArgumentMappingMetadata(node_name="baz", inputs=["foo"]),
        ArgumentMappingMetadata(node_name="qux", inputs=["bar", "baz"]),
    ]
    expected_output = PrevalidatedDAG(
        nodes=[
            PrevalidatedNode(
                name="foo",
                rule="foo",
                children=["bar", "baz"],
                input_values=[],
                output_value="foo",
            ),
            PrevalidatedNode(
                name="bar",
                rule="bar",
                children=["qux"],
                input_values=["foo"],
                output_value="bar",
            ),
            PrevalidatedNode(
                name="baz",
                rule="baz",
                children=["qux"],
                input_values=["foo"],
                output_value="baz",
            ),
            PrevalidatedNode(
                name="qux",
                rule="qux",
                children=[],
                input_values=["bar", "baz"],
                output_value="qux",
            ),
        ]
    )
    actual = PrevalidatedDAG.from_node_list(operations, argument_mappings)
    assert expected_output == actual


def test_prevalidated_dag_from_node_list_diamond_structure_only_required_mappings_given():
    operations = OperationList(
        items=[
            Operation(name="foo", rule="foo", children=["bar", "baz"]),
            Operation(name="bar", rule="bar", children=["qux"]),
            Operation(name="baz", rule="baz", children=["qux"]),
            Operation(name="qux", rule="qux", children=[]),
        ]
    )
    argument_mappings = [
        ArgumentMappingMetadata(node_name="qux", inputs=["bar", "baz"]),
    ]
    expected_output = PrevalidatedDAG(
        nodes=[
            PrevalidatedNode(
                name="foo",
                rule="foo",
                children=["bar", "baz"],
                input_values=[],
                output_value="foo",
            ),
            PrevalidatedNode(
                name="bar",
                rule="bar",
                children=["qux"],
                input_values=["foo"],
                output_value="bar",
            ),
            PrevalidatedNode(
                name="baz",
                rule="baz",
                children=["qux"],
                input_values=["foo"],
                output_value="baz",
            ),
            PrevalidatedNode(
                name="qux",
                rule="qux",
                children=[],
                input_values=["bar", "baz"],
                output_value="qux",
            ),
        ]
    )
    actual = PrevalidatedDAG.from_node_list(operations, argument_mappings)
    assert expected_output == actual
