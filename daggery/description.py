from typing import Tuple

from pydantic import BaseModel, model_validator


class Operation(BaseModel, frozen=True):
    # A descriptive name for this specific operation. It must be unique.
    name: str
    # The name of the operation to perform.
    op_name: str
    # The names of dependent operations.
    children: Tuple[str, ...] = ()

    @model_validator(mode="after")
    def name_and_op_name_not_empty(self):
        if self.name == "":
            raise ValueError("An Operation must have a name")
        if self.op_name == "":
            raise ValueError("An Operation must have an op_name")
        return self

    @model_validator(mode="after")
    def children_are_unique(self):
        if len(self.children) != len(set(self.children)):
            raise ValueError("An Operation cannot have duplicate children")
        return self


class ArgumentMapping(BaseModel, frozen=True):
    op_name: str
    # Named arguments as inputs to the specified operation. These should be in the
    # same order as the op's arguments. For example, if an op has two ordered
    # arguments, `base` and `exponent`, then the input values should be
    # ("name_of_op_with_base", "name_of_op_with_exponent").
    inputs: Tuple[str, ...] = ()
    # Operations always have one output, so no need to name them.
    # TODO: Consider 'assigning' outputs as opposed to the current approach of
    # always broadcasting. Decide at what level of abstraction this would be
    # useful.

    @model_validator(mode="after")
    def name_not_empty(self):
        if self.op_name.strip() == "":
            raise ValueError("An ArgumentMapping must name an operation")
        # No need to check inputs are not empty, as this is technically
        # allowed if the head is specified, albeit redundant.
        return self


class OperationSequence(BaseModel, frozen=True):
    ops: Tuple[Operation, ...]

    @model_validator(mode="after")
    def ops_not_empty(self):
        if len(self.ops) == 0:
            raise ValueError("An OperationSequence cannot be empty")
        return self

    @model_validator(mode="after")
    def ops_unique(self):
        op_names = [op.name for op in self.ops]
        if len(self.ops) != len(set(op_names)):
            raise ValueError(
                f"An OperationSequence cannot contain duplicate operations: {self.ops}"
            )
        return self


class DAGDescription(BaseModel):
    operations: OperationSequence
    argument_mappings: Tuple[ArgumentMapping, ...] = ()

    @model_validator(mode="after")
    def argument_mappings_valid(self):
        # Argument mappings can be empty (e.g. the DAG is a linear sequence),
        # but any mapping must correctly reference the given operations.
        # Block duplicate mappings for the same op - this avoids ambiguous cases
        # and realistically would be an error.
        if len(self.argument_mappings) != len(set(self.argument_mappings)):
            raise ValueError(
                "Duplicate mappings for the same operation are not allowed: "
                f"{self.argument_mappings}"
            )
        op_names = set([op.name for op in self.operations.ops])
        mapping_op_names = set([mp.op_name for mp in self.argument_mappings])
        mapping_child_names = set(
            [child for mp in self.argument_mappings for child in mp.inputs]
        )
        if (mapping_op_names | mapping_child_names).difference(op_names):
            raise ValueError(
                "ArgumentMappings must all reference ops in operations: "
                f"{self.argument_mappings}"
            )
        return self
