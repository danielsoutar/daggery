import pytest
from pydantic import ValidationError

from daggery.description import (
    ArgumentMapping,
    DAGDescription,
    Operation,
    OperationSequence,
)


def test_operation_with_empty_name():
    with pytest.raises(ValidationError, match="An Operation must have a name"):
        Operation(name="", op_name="foo")


def test_operation_with_empty_op_name():
    with pytest.raises(ValidationError, match="An Operation must have an op_name"):
        Operation(name="foo", op_name="")


def test_operation_with_duplicate_children():
    with pytest.raises(
        ValidationError, match="An Operation cannot have duplicate children"
    ):
        Operation(name="foo", op_name="foo", children=("child1", "child1"))


def test_operation_with_unique_children():
    op = Operation(name="foo", op_name="foo", children=("child1", "child2"))
    assert op.children == ("child1", "child2")


def test_argument_mapping():
    mapping = ArgumentMapping(op_name="foo", inputs=("input1", "input2"))
    assert mapping.op_name == "foo"
    assert mapping.inputs == ("input1", "input2")


def test_argument_mapping_must_name_operation():
    with pytest.raises(
        ValidationError, match="An ArgumentMapping must name an operation"
    ):
        ArgumentMapping(op_name="")


def test_operation_sequence_not_empty():
    with pytest.raises(ValidationError, match="An OperationSequence cannot be empty"):
        OperationSequence(ops=())


def test_operation_sequence_has_duplicates():
    with pytest.raises(
        ValidationError,
        match="An OperationSequence cannot contain duplicate operations: ",
    ):
        OperationSequence(
            ops=(
                Operation(name="foo", op_name="foo"),
                Operation(name="foo", op_name="foo"),
            )
        )


def test_operation_list_with_ops():
    op_list = OperationSequence(ops=(Operation(name="foo", op_name="foo"),))
    assert len(op_list.ops) == 1


def test_dag_description_with_duplicate_argument_mappings():
    with pytest.raises(
        ValidationError,
        match="Duplicate mappings for the same operation are not allowed:",
    ):
        DAGDescription(
            operations=OperationSequence(ops=(Operation(name="foo", op_name="foo"),)),
            argument_mappings=(
                ArgumentMapping(op_name="foo"),
                ArgumentMapping(op_name="foo"),
            ),
        )


def test_dag_description_with_argument_mappings_naming_invalid_operations():
    with pytest.raises(
        ValidationError,
        match="ArgumentMappings must all reference ops in operations:",
    ):
        DAGDescription(
            operations=OperationSequence(ops=(Operation(name="foo", op_name="foo"),)),
            argument_mappings=(
                ArgumentMapping(op_name="foo"),
                ArgumentMapping(op_name="not-foo"),
            ),
        )


def test_dag_description_with_valid_operations():
    ops = OperationSequence(ops=(Operation(name="foo", op_name="foo"),))
    desc = DAGDescription(operations=ops)
    assert desc.operations == ops


def test_dag_description_with_valid_argument_mappings():
    ops = OperationSequence(ops=(Operation(name="foo", op_name="foo"),))
    mappings = (ArgumentMapping(op_name="foo"),)
    desc = DAGDescription(operations=ops, argument_mappings=mappings)
    assert desc.operations == ops
    assert desc.argument_mappings == mappings
