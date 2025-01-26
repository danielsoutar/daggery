from typing import Any, Union

from .graph import (
    AbstractFunctionGraph,
    EmptyDAG,
    InvalidSequence,
    UnvalidatedDAG,
    node_map,
)
from .node import Node
from .utils import logger_factory

logger = logger_factory(__name__)


class FunctionSequence(AbstractFunctionGraph):
    head: Node

    def __init__(self, head: Node):
        # Set the head of the FunctionSequence.
        super().__init__(head=head)

    # We separate the creation of the sequence from the init method since this allows
    # returning instances of InvalidGraph, making this code exception-free.
    @classmethod
    def from_unvalidated_dag(
        cls, unvalidated_sequence: UnvalidatedDAG
    ) -> Union["FunctionSequence", InvalidSequence]:
        node_names = [node.name for node in unvalidated_sequence.nodes]
        if len(node_names) != len(set(node_names)):
            return InvalidSequence(
                message=f"Non-unique node names found in FunctionSequence: {node_names}"
            )

        node_rules = [node.rule for node in unvalidated_sequence.nodes]
        for rule in node_rules:
            if rule not in node_map.keys():
                return InvalidSequence(
                    message=f"Invalid rule found in unvalidated FunctionSequence: {rule}"
                )

        reversed_nodes = reversed(unvalidated_sequence.nodes)
        unvalidated_tail = next(reversed_nodes)
        if len(unvalidated_tail.children) != 0:
            return InvalidSequence(
                message=f"Tail with children found in unvalidated FunctionSequence: {unvalidated_tail}"
            )
        tail_class = node_map[unvalidated_tail.rule]
        tail = tail_class(name=unvalidated_tail.name, children=tuple())
        head = tail

        # Creating immutable nodes back-to-front guarantees an immutable FunctionSequence.
        for unvalidated_node in reversed_nodes:
            name = unvalidated_node.name
            child_nodes = unvalidated_node.children

            if len(child_nodes) != 1:
                return InvalidSequence(
                    message=(
                        "Node with 0 or >1 children found in unvalidated "
                        f"FunctionSequence: {unvalidated_node}"
                    )
                )
            if child_nodes[0] != head.name:
                return InvalidSequence(
                    message=(
                        "Node with invalid child found in unvalidated "
                        f"FunctionSequence: {unvalidated_node} (node), "
                        f"{head} (expected child)"
                    )
                )

            node_class = node_map[unvalidated_node.rule]
            head = node_class(name=name, children=(head,))

        return cls(head=head)

    @classmethod
    def from_string(
        cls, sequence_string: str
    ) -> Union["FunctionSequence", InvalidSequence]:
        unvalidated_sequence = UnvalidatedDAG.from_string(sequence_string)
        if isinstance(unvalidated_sequence, EmptyDAG):
            return InvalidSequence(message=unvalidated_sequence.message)
        return cls.from_unvalidated_dag(unvalidated_sequence)

    def transform(self, value: Any) -> Any:
        current_node: Node | None = self.head
        while current_node is not None:
            # Log the node name and intermediate result.
            logger.info(
                f"Node: {current_node.__class__.__name__}, "
                f"Intermediate Result: {value}"
            )
            value = current_node.transform(value)
            current_node = (
                current_node.children[0] if current_node.children != () else None
            )
        return value

    # TODO: Fill this in.
    def serialise(self) -> str:
        return str(self.head)
