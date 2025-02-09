from typing import List

from pydantic import BaseModel, ConfigDict, model_validator


class Operation(BaseModel):
    name: str
    rule: str
    children: List[str] = []

    @model_validator(mode="after")
    def name_and_rule_not_empty(self):
        if self.name == "":
            raise ValueError("An Operation must have a name")
        if self.rule == "":
            raise ValueError("An Operation must have a rule")
        return self

    @model_validator(mode="after")
    def children_are_unique(self):
        if len(self.children) != len(set(self.children)):
            raise ValueError("An Operation cannot have duplicate children")
        return self


class ArgumentMappingMetadata(BaseModel):
    # The name of the node. It should be unique.
    node_name: str
    # The named arguments as inputs to the given node. These should be in the
    # same order as the node's arguments. For example, if a node has two ordered
    # arguments, `base` and `exponent`, then the input values should be
    # ["name_of_node_with_base", "name_of_node_with_exponent"].
    inputs: List[str] = []
    # Nodes always have one output, so no need to name them.
    # TODO: Consider 'assigning' outputs as opposed to the current approach of
    # always broadcasting. Decide at what level of abstraction this would be
    # useful.

    @model_validator(mode="after")
    def name_not_empty(self):
        if self.node_name.strip() == "":
            raise ValueError("An ArgumentMappingMetadata must name a node")
        # No need to check inputs are not empty, as this is technically
        # allowed if the head is specified, albeit redundant.
        return self


class OperationList(BaseModel):
    """
    A non-empty immutable list of Operations, encoding a graph in node list
    form.
    """

    model_config = ConfigDict(frozen=True)

    items: List[Operation]

    @model_validator(mode="after")
    def operations_not_empty(self):
        if len(self.items) == 0:
            raise ValueError("An OperationList cannot be empty")
        return self


class AdjustmentRequest(BaseModel):
    name: str
    value: int
    operations: str | OperationList
    argument_mappings: List[ArgumentMappingMetadata]

    @model_validator(mode="after")
    def operations_not_empty(self):
        if isinstance(self.operations, str) and self.operations.strip() == "":
            raise ValueError("operations must not be empty")
        return self

    @model_validator(mode="after")
    def argument_mappings_valid(self):
        # If `operations` encodes a graph including nodes with multiple inputs or
        # multiple outputs, argument_mappings should be set to disambiguate these cases.
        # Otherwise argument_mappings can be empty.
        operations_guaranteed_linear = isinstance(self.operations, str)
        argument_mappings_set = len(self.argument_mappings) > 0
        distinct_argument_mappings = set(
            [mapping.node_name for mapping in self.argument_mappings]
        )
        argument_mappings_not_distinct = len(self.argument_mappings) != len(
            distinct_argument_mappings
        )
        if operations_guaranteed_linear and argument_mappings_set:
            raise ValueError(
                "argument_mappings does not need to be set if operations is a string"
            )
        if argument_mappings_not_distinct:
            raise ValueError(
                "argument_mappings cannot contain duplicate mappings for the same node"
            )

        return self
