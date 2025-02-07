import pytest
from pydantic import ValidationError

from daggery.request import (
    AdjustmentRequest,
    ArgumentMappingMetadata,
    Operation,
    OperationList,
)


def test_operation_with_empty_name_or_rule():
    with pytest.raises(ValidationError, match="An Operation must have a name"):
        Operation(name="", rule="foo", children=[])
    with pytest.raises(ValidationError, match="An Operation must have a rule"):
        Operation(name="foo", rule="", children=[])


def test_operation_with_duplicate_children():
    with pytest.raises(
        ValidationError, match="An Operation cannot have duplicate children"
    ):
        Operation(name="foo", rule="foo", children=["child1", "child1"])


def test_operation_with_unique_children():
    op = Operation(name="foo", rule="foo", children=["child1", "child2"])
    assert op.children == ["child1", "child2"]


def test_argument_mapping_metadata():
    mapping = ArgumentMappingMetadata(node_name="foo", inputs=["input1", "input2"])
    assert mapping.node_name == "foo"
    assert mapping.inputs == ["input1", "input2"]


def test_operation_list_not_empty():
    with pytest.raises(ValidationError, match="An OperationList cannot be empty"):
        OperationList(items=[])


def test_operation_list_with_items():
    op_list = OperationList(items=[Operation(name="foo", rule="foo")])
    assert len(op_list.items) == 1


def test_adjustment_request_operations_not_empty():
    with pytest.raises(ValidationError, match="operations must not be empty"):
        AdjustmentRequest(name="req1", value=1, operations="", argument_mappings=[])


def test_adjustment_request_with_argument_mappings_and_string_operations():
    with pytest.raises(
        ValidationError,
        match="argument_mappings does not need to be set if operations is a string",
    ):
        AdjustmentRequest(
            name="req1",
            value=1,
            operations="foo",
            argument_mappings=[
                ArgumentMappingMetadata(node_name="foo", inputs=["input1"])
            ],
        )


def test_adjustment_request_with_duplicate_argument_mappings():
    with pytest.raises(
        ValidationError,
        match="argument_mappings cannot contain duplicate mappings for the same node",
    ):
        AdjustmentRequest(
            name="req1",
            value=1,
            operations=OperationList(items=[Operation(name="foo", rule="foo")]),
            argument_mappings=[
                ArgumentMappingMetadata(node_name="foo", inputs=["input1"]),
                ArgumentMappingMetadata(node_name="foo", inputs=["input2"]),
            ],
        )


def test_adjustment_request_with_valid_operations():
    request = AdjustmentRequest(
        name="req1", value=1, operations="foo", argument_mappings=[]
    )
    assert request.operations == "foo"


def test_adjustment_request_with_valid_argument_mappings():
    request = AdjustmentRequest(
        name="req1",
        value=1,
        operations=OperationList(items=[Operation(name="foo", rule="foo")]),
        argument_mappings=[ArgumentMappingMetadata(node_name="foo", inputs=["input1"])],
    )
    assert len(request.argument_mappings) == 1
