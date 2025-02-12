import pytest
from pydantic import ValidationError

from daggery.description import (
    ArgumentMapping,
    DAGDescription,
    Operation,
    OperationSequence,
)
from daggery.prevalidate import (
    EmptyDAG,
    InvalidDAG,
    PrevalidatedDAG,
    PrevalidatedNode,
)


def test_prevalidated_dag_from_string_single_node():
    dag_string = "foo"
    expected_output = PrevalidatedDAG(
        nodes=(PrevalidatedNode(name="foo0", node_class="foo"),)
    )
    assert PrevalidatedDAG.from_string(dag_string) == expected_output


def test_prevalidated_dag_from_string_multiple_nodes():
    dag_string = "foo >> bar >> baz"
    expected_output = PrevalidatedDAG(
        nodes=(
            PrevalidatedNode(
                name="foo0",
                node_class="foo",
                children=("bar0",),
            ),
            PrevalidatedNode(
                name="bar0",
                node_class="bar",
                children=("baz0",),
                input_nodes=("foo0",),
            ),
            PrevalidatedNode(
                name="baz0",
                node_class="baz",
                input_nodes=("bar0",),
            ),
        )
    )
    assert PrevalidatedDAG.from_string(dag_string) == expected_output


def test_prevalidated_dag_from_string_empty_string():
    dag_string = ""
    expected_output = EmptyDAG(message="DAG string is empty and therefore invalid")
    assert PrevalidatedDAG.from_string(dag_string) == expected_output


def test_prevalidated_dag_from_dag_description_single_node():
    operations = OperationSequence(ops=(Operation(name="foo", op_name="foo"),))
    expected_output = PrevalidatedDAG(
        nodes=(PrevalidatedNode(name="foo", node_class="foo"),)
    )
    assert (
        PrevalidatedDAG.from_dag_description(DAGDescription(operations=operations))
        == expected_output
    )


def test_prevalidated_dag_from_dag_description_multiple_nodes_multiple_heads():
    operations = OperationSequence(
        ops=(
            Operation(name="foo", op_name="foo", children=("baz",)),
            Operation(name="bar", op_name="bar", children=("baz",)),
            Operation(name="baz", op_name="baz"),
        )
    )
    mappings = (ArgumentMapping(op_name="baz", inputs=("foo", "bar")),)
    actual = PrevalidatedDAG.from_dag_description(
        DAGDescription(operations=operations, argument_mappings=mappings)
    )
    assert isinstance(actual, InvalidDAG)


def test_prevalidated_dag_from_dag_description_multiple_nodes_multiple_tails():
    operations = OperationSequence(
        ops=(
            Operation(name="foo", op_name="foo", children=("bar", "baz")),
            Operation(name="bar", op_name="bar"),
            Operation(name="baz", op_name="baz"),
        )
    )
    actual = PrevalidatedDAG.from_dag_description(DAGDescription(operations=operations))
    assert isinstance(actual, InvalidDAG)


def test_prevalidated_dag_from_dag_description_multiple_nodes_no_mappings_given():
    operations = OperationSequence(
        ops=(
            Operation(name="foo", op_name="foo", children=("bar",)),
            Operation(name="bar", op_name="bar", children=("baz",)),
            Operation(name="baz", op_name="baz"),
        )
    )
    # Because we have a linear sequence, mappings are unambigious. So we don't
    # need to specify them.
    expected_output = PrevalidatedDAG(
        nodes=(
            PrevalidatedNode(
                name="foo",
                node_class="foo",
                children=("bar",),
            ),
            PrevalidatedNode(
                name="bar",
                node_class="bar",
                children=("baz",),
                input_nodes=("foo",),
            ),
            PrevalidatedNode(
                name="baz",
                node_class="baz",
                input_nodes=("bar",),
            ),
        )
    )
    assert (
        PrevalidatedDAG.from_dag_description(DAGDescription(operations=operations))
        == expected_output
    )


def test_prevalidated_dag_from_dag_description_multiple_nodes_invalid_mappings_given():
    operations = OperationSequence(
        ops=(
            Operation(name="foo", op_name="foo", children=("bar",)),
            Operation(name="bar", op_name="bar", children=("baz",)),
            Operation(name="baz", op_name="baz"),
        )
    )
    # Although specifying redundant mappings isn't an error, specifying
    # invalid mappings is. So we catch that case here.
    mappings = (ArgumentMapping(op_name="bar"),)
    actual = PrevalidatedDAG.from_dag_description(
        DAGDescription(operations=operations, argument_mappings=mappings)
    )
    assert isinstance(actual, InvalidDAG)


def test_prevalidated_dag_from_dag_description_multiple_nodes_some_mappings_given():
    operations = OperationSequence(
        ops=(
            Operation(name="foo", op_name="foo", children=("bar",)),
            Operation(name="bar", op_name="bar", children=("baz",)),
            Operation(name="baz", op_name="baz"),
        )
    )
    # We don't need to specify all mappings, instead we can just specify those
    # we think are ambigious. In this case there aren't any, but over-specifying
    # is not treated as an error.
    mappings = (
        ArgumentMapping(op_name="bar", inputs=("foo",)),
        ArgumentMapping(op_name="baz", inputs=("bar",)),
    )
    expected_output = PrevalidatedDAG(
        nodes=(
            PrevalidatedNode(
                name="foo",
                node_class="foo",
                children=("bar",),
            ),
            PrevalidatedNode(
                name="bar",
                node_class="bar",
                children=("baz",),
                input_nodes=("foo",),
            ),
            PrevalidatedNode(
                name="baz",
                node_class="baz",
                input_nodes=("bar",),
            ),
        )
    )
    assert (
        PrevalidatedDAG.from_dag_description(
            DAGDescription(operations=operations, argument_mappings=mappings)
        )
        == expected_output
    )


def test_prevalidated_dag_from_dag_description_multiple_nodes_all_mappings_given():
    operations = OperationSequence(
        ops=(
            Operation(name="foo", op_name="foo", children=("bar",)),
            Operation(name="bar", op_name="bar", children=("baz",)),
            Operation(name="baz", op_name="baz", children=()),
        )
    )
    # We don't need to specify all mappings, instead we can just specify those
    # we think are ambigious. In this case there aren't any, but over-specifying
    # is not treated as an error.
    mappings = (
        ArgumentMapping(op_name="foo", inputs=()),
        ArgumentMapping(op_name="bar", inputs=("foo",)),
        ArgumentMapping(op_name="baz", inputs=("bar",)),
    )
    expected_output = PrevalidatedDAG(
        nodes=(
            PrevalidatedNode(
                name="foo",
                node_class="foo",
                children=("bar",),
            ),
            PrevalidatedNode(
                name="bar",
                node_class="bar",
                children=("baz",),
                input_nodes=("foo",),
            ),
            PrevalidatedNode(
                name="baz",
                node_class="baz",
                input_nodes=("bar",),
            ),
        )
    )
    assert (
        PrevalidatedDAG.from_dag_description(
            DAGDescription(operations=operations, argument_mappings=mappings)
        )
        == expected_output
    )


def test_prevalidated_dag_from_dag_description_diamond_structure_no_mappings_given():
    operations = OperationSequence(
        ops=(
            Operation(name="foo", op_name="foo", children=("bar", "baz")),
            Operation(name="bar", op_name="bar", children=("qux",)),
            Operation(name="baz", op_name="baz", children=("qux",)),
            Operation(name="qux", op_name="qux"),
        )
    )
    actual = PrevalidatedDAG.from_dag_description(DAGDescription(operations=operations))
    assert isinstance(actual, InvalidDAG)


def test_prevalidated_dag_from_dag_description_diamond_structure_all_mappings_given():
    operations = OperationSequence(
        ops=(
            Operation(name="foo", op_name="foo", children=("bar", "baz")),
            Operation(name="bar", op_name="bar", children=("qux",)),
            Operation(name="baz", op_name="baz", children=("qux",)),
            Operation(name="qux", op_name="qux"),
        )
    )
    mappings = (
        ArgumentMapping(op_name="foo"),
        ArgumentMapping(op_name="bar", inputs=("foo",)),
        ArgumentMapping(op_name="baz", inputs=("foo",)),
        ArgumentMapping(op_name="qux", inputs=("bar", "baz")),
    )
    expected_output = PrevalidatedDAG(
        nodes=(
            PrevalidatedNode(
                name="foo",
                node_class="foo",
                children=(
                    "bar",
                    "baz",
                ),
            ),
            PrevalidatedNode(
                name="bar",
                node_class="bar",
                children=("qux",),
                input_nodes=("foo",),
            ),
            PrevalidatedNode(
                name="baz",
                node_class="baz",
                children=("qux",),
                input_nodes=("foo",),
            ),
            PrevalidatedNode(
                name="qux",
                node_class="qux",
                input_nodes=(
                    "bar",
                    "baz",
                ),
            ),
        )
    )
    actual = PrevalidatedDAG.from_dag_description(
        DAGDescription(operations=operations, argument_mappings=mappings)
    )
    assert expected_output == actual


def test_prevalidated_dag_from_dag_description_diamond_structure_only_required_mappings_given():
    operations = OperationSequence(
        ops=(
            Operation(name="foo", op_name="foo", children=("bar", "baz")),
            Operation(name="bar", op_name="bar", children=("qux",)),
            Operation(name="baz", op_name="baz", children=("qux",)),
            Operation(name="qux", op_name="qux"),
        )
    )
    mappings = (ArgumentMapping(op_name="qux", inputs=("bar", "baz")),)
    expected_output = PrevalidatedDAG(
        nodes=(
            PrevalidatedNode(
                name="foo",
                node_class="foo",
                children=(
                    "bar",
                    "baz",
                ),
            ),
            PrevalidatedNode(
                name="bar",
                node_class="bar",
                children=("qux",),
                input_nodes=("foo",),
            ),
            PrevalidatedNode(
                name="baz",
                node_class="baz",
                children=("qux",),
                input_nodes=("foo",),
            ),
            PrevalidatedNode(
                name="qux",
                node_class="qux",
                input_nodes=(
                    "bar",
                    "baz",
                ),
            ),
        )
    )
    actual = PrevalidatedDAG.from_dag_description(
        DAGDescription(operations=operations, argument_mappings=mappings)
    )
    assert expected_output == actual


def test_prevalidated_node_has_non_empty_name_and_node_class():
    with pytest.raises(ValidationError) as exc_info:
        PrevalidatedNode(name="", node_class="my_op")
    assert "PrevalidatedNode must have a name" in str(exc_info.value)
    with pytest.raises(ValidationError) as exc_info:
        PrevalidatedNode(name="some_name", node_class="")
    assert "PrevalidatedNode must have a node classname" in str(exc_info.value)


def test_prevalidated_node_ensures_unique_children():
    with pytest.raises(ValidationError) as exc_info:
        PrevalidatedNode(
            name="some_name",
            node_class="my_op",
            children=("child0", "child1", "child1"),
        )
    assert "PrevalidatedNode must have unique children" in str(exc_info.value)


def test_prevalidated_node_ensures_unique_input_nodes():
    with pytest.raises(ValidationError) as exc_info:
        PrevalidatedNode(
            name="some_name",
            node_class="my_op",
            input_nodes=("child0", "child1", "child1"),
        )
    assert "PrevalidatedNode must have unique input nodes" in str(exc_info.value)


def test_prevalidated_dag_fails_if_empty():
    with pytest.raises(ValidationError) as exc_info:
        PrevalidatedDAG(nodes=())
    assert "PrevalidatedDAG must contain at least one node" in str(exc_info.value)


def test_prevalidated_dag_not_topologically_sorted():
    operations = OperationSequence(
        ops=(
            Operation(name="foo", op_name="foo", children=("bar",)),
            Operation(name="bar", op_name="bar", children=("baz",)),
            Operation(name="baz", op_name="baz", children=("foo",)),
        )
    )
    mappings = ()
    actual = PrevalidatedDAG.from_dag_description(
        DAGDescription(operations=operations, argument_mappings=mappings)
    )
    assert isinstance(actual, InvalidDAG)
    assert "Input is not topologically sorted" in actual.message
