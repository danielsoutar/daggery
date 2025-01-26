from typing import List

from pydantic import BaseModel, model_validator


class Operation(BaseModel):
    name: str
    rule: str
    children: List[str] = []


class ArgumentMappingMetadata(BaseModel):
    node_name: str
    inputs: List[str] = []
    outputs: List[str] = []


class AdjustmentRequest(BaseModel):
    name: str
    value: int
    operations: str | List[Operation]
    argument_mappings: List[ArgumentMappingMetadata]

    @model_validator(mode="after")
    def operations_not_empty(self):
        if isinstance(self.operations, str) and self.operations.strip() == "":
            raise ValueError("operations must not be empty")
        if isinstance(self.operations, list) and len(self.operations) == 0:
            raise ValueError("operations must not be empty")
        return self

    @model_validator(mode="after")
    def argument_mappings_valid(self):
        # If `operations` encodes a graph including nodes with multiple inputs or
        # multiple outputs, argument_mappings should be set to disambiguate these cases.
        # Otherwise argument_mappings can be empty.
        # For simplicity however we use a type check and the server can perform stronger
        # validation. argument_mappings can be empty if not needed.
        operations_guaranteed_linear = isinstance(self.operations, str)
        operations_not_guaranteed_linear = not operations_guaranteed_linear
        argument_mappings_set = len(self.argument_mappings) > 0
        if operations_guaranteed_linear and argument_mappings_set:
            raise ValueError(
                "argument_mappings does not need to be set if operations is a string"
            )
        if operations_not_guaranteed_linear and not argument_mappings_set:
            raise ValueError(
                "argument_mappings must be set if operations is a graph of operations"
            )
        return self
