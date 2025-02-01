from adjustment.graph import (
    EmptyDAG,
    InvalidGraph,
    UnvalidatedDAG,
    UnvalidatedNode,
)
from adjustment.request import ArgumentMappingMetadata, Operation, OperationList


def test_unvalidated_dag_from_string_single_node():
    dag_string = "foo"
    expected_output = UnvalidatedDAG(
        nodes=[
            UnvalidatedNode(
                name="foo0",
                rule="foo",
                children=[],
                input_values=[],
                output_value="",
            ),
        ]
    )
    assert UnvalidatedDAG.from_string(dag_string) == expected_output


def test_unvalidated_dag_from_string_multiple_nodes():
    dag_string = "foo >> bar >> baz"
    expected_output = UnvalidatedDAG(
        nodes=[
            UnvalidatedNode(
                name="foo0", rule="foo", children=["bar0"], output_value=""
            ),
            UnvalidatedNode(
                name="bar0",
                rule="bar",
                children=["baz0"],
                output_value="",
            ),
            UnvalidatedNode(name="baz0", rule="baz", children=[], output_value=""),
        ]
    )
    assert UnvalidatedDAG.from_string(dag_string) == expected_output


def test_unvalidated_dag_from_string_empty_string():
    dag_string = ""
    expected_output = EmptyDAG(message="DAG string is empty and therefore invalid")
    assert UnvalidatedDAG.from_string(dag_string) == expected_output


def test_unvalidated_dag_from_node_list_single_node():
    operations = OperationList(items=[Operation(name="foo", rule="foo", children=[])])
    argument_mappings = [ArgumentMappingMetadata(node_name="foo", inputs=[])]
    expected_output = UnvalidatedDAG(
        nodes=[
            UnvalidatedNode(
                name="foo",
                rule="foo",
                children=[],
                input_values=[],
                output_value="foo",
            ),
        ]
    )
    assert (
        UnvalidatedDAG.from_node_list(operations, argument_mappings) == expected_output
    )


def test_unvalidated_dag_from_node_list_multiple_nodes_multiple_heads():
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
    actual = UnvalidatedDAG.from_node_list(operations, argument_mappings)
    assert isinstance(actual, InvalidGraph)


def test_unvalidated_dag_from_node_list_multiple_nodes_multiple_tails():
    operations = OperationList(
        items=[
            Operation(name="foo", rule="foo", children=["bar", "baz"]),
            Operation(name="bar", rule="bar", children=[]),
            Operation(name="baz", rule="baz", children=[]),
        ]
    )
    argument_mappings: list[ArgumentMappingMetadata] = []
    actual = UnvalidatedDAG.from_node_list(operations, argument_mappings)
    assert isinstance(actual, InvalidGraph)


def test_unvalidated_dag_from_node_list_multiple_nodes_no_mappings_given():
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
    expected_output = UnvalidatedDAG(
        nodes=[
            UnvalidatedNode(
                name="foo",
                rule="foo",
                children=["bar"],
                input_values=[],
                output_value="foo",
            ),
            UnvalidatedNode(
                name="bar",
                rule="bar",
                children=["baz"],
                input_values=["foo"],
                output_value="bar",
            ),
            UnvalidatedNode(
                name="baz",
                rule="baz",
                children=[],
                input_values=["bar"],
                output_value="baz",
            ),
        ]
    )
    assert (
        UnvalidatedDAG.from_node_list(operations, argument_mappings) == expected_output
    )


def test_unvalidated_dag_from_node_list_multiple_nodes_invalid_mappings_given():
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
    actual = UnvalidatedDAG.from_node_list(operations, argument_mappings)
    assert isinstance(actual, InvalidGraph)


def test_unvalidated_dag_from_node_list_multiple_nodes_some_mappings_given():
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
    expected_output = UnvalidatedDAG(
        nodes=[
            UnvalidatedNode(
                name="foo",
                rule="foo",
                children=["bar"],
                input_values=[],
                output_value="foo",
            ),
            UnvalidatedNode(
                name="bar",
                rule="bar",
                children=["baz"],
                input_values=["foo"],
                output_value="bar",
            ),
            UnvalidatedNode(
                name="baz",
                rule="baz",
                children=[],
                input_values=["bar"],
                output_value="baz",
            ),
        ]
    )
    assert (
        UnvalidatedDAG.from_node_list(operations, argument_mappings) == expected_output
    )


def test_unvalidated_dag_from_node_list_multiple_nodes_all_mappings_given():
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
    expected_output = UnvalidatedDAG(
        nodes=[
            UnvalidatedNode(
                name="foo",
                rule="foo",
                children=["bar"],
                input_values=[],
                output_value="foo",
            ),
            UnvalidatedNode(
                name="bar",
                rule="bar",
                children=["baz"],
                input_values=["foo"],
                output_value="bar",
            ),
            UnvalidatedNode(
                name="baz",
                rule="baz",
                children=[],
                input_values=["bar"],
                output_value="baz",
            ),
        ]
    )
    assert (
        UnvalidatedDAG.from_node_list(operations, argument_mappings) == expected_output
    )


def test_unvalidated_dag_from_node_list_diamond_structure_invalid_mappings_missing():
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
        ArgumentMappingMetadata(node_name="bar", inputs=[]),
        ArgumentMappingMetadata(node_name="baz", inputs=[]),
        ArgumentMappingMetadata(node_name="qux", inputs=[]),
    ]
    actual = UnvalidatedDAG.from_node_list(operations, argument_mappings)
    assert isinstance(actual, InvalidGraph)


def test_unvalidated_dag_from_node_list_diamond_structure_all_mappings_given():
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
    expected_output = UnvalidatedDAG(
        nodes=[
            UnvalidatedNode(
                name="foo",
                rule="foo",
                children=["bar", "baz"],
                input_values=[],
                output_value="foo",
            ),
            UnvalidatedNode(
                name="bar",
                rule="bar",
                children=["qux"],
                input_values=["foo"],
                output_value="bar",
            ),
            UnvalidatedNode(
                name="baz",
                rule="baz",
                children=["qux"],
                input_values=["foo"],
                output_value="baz",
            ),
            UnvalidatedNode(
                name="qux",
                rule="qux",
                children=[],
                input_values=["bar", "baz"],
                output_value="qux",
            ),
        ]
    )
    actual = UnvalidatedDAG.from_node_list(operations, argument_mappings)
    assert expected_output == actual
