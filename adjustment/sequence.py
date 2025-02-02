from typing import Any, Union

from pydantic import BaseModel

from .graph import (
    EmptyDAG,
    InvalidSequence,
    PrevalidatedDAG,
    node_map,
)
from .node import Node
from .utils import logger_factory

logger = logger_factory(__name__)


class FunctionSequence(BaseModel):
    head: Node

    def __init__(self, head: Node):
        # Set the head of the FunctionSequence.
        super().__init__(head=head)

    # We separate the creation of the sequence from the init method since this allows
    # returning instances of InvalidGraph, making this code exception-free.
    @classmethod
    def from_prevalidated_dag(
        cls, prevalidated_sequence: PrevalidatedDAG
    ) -> Union["FunctionSequence", InvalidSequence]:
        node_rules = [node.rule for node in prevalidated_sequence.nodes]
        for rule in node_rules:
            if rule not in node_map.keys():
                return InvalidSequence(
                    message=f"Invalid rule found in FunctionSequence: {rule}"
                )

        reversed_nodes = reversed(prevalidated_sequence.nodes)
        prevalidated_tail = next(reversed_nodes)
        tail_class = node_map[prevalidated_tail.rule]
        tail = tail_class(name=prevalidated_tail.name, children=tuple())
        head = tail

        # Creating immutable nodes back-to-front guarantees an immutable Sequence.
        for prevalidated_node in reversed_nodes:
            name = prevalidated_node.name
            child_nodes = prevalidated_node.children

            if len(child_nodes) > 1:
                return InvalidSequence(
                    message=(
                        "Node with >1 children found in "
                        f"FunctionSequence: {prevalidated_node}"
                    )
                )

            node_class = node_map[prevalidated_node.rule]
            head = node_class(name=name, children=(head,))

        return cls(head=head)

    @classmethod
    def from_string(
        cls, sequence_string: str
    ) -> Union["FunctionSequence", InvalidSequence]:
        prevalidated_sequence = PrevalidatedDAG.from_string(sequence_string)
        if isinstance(prevalidated_sequence, EmptyDAG):
            return InvalidSequence(message=prevalidated_sequence.message)
        return cls.from_prevalidated_dag(prevalidated_sequence)

    def transform(self, value: Any) -> Any:
        current_node: Node | None = self.head
        while current_node is not None:
            value = current_node.transform(value)
            self._pretty_log_node(current_node, value)
            children = current_node.children or (None,)
            current_node = children[0]
        return value

    def _pretty_log_node(self, current_node: Node, value: Any) -> None:
        logger.info(f"Node: {current_node.name}:")
        logger.info(f"  Output(s): {value}")

    # TODO: Fill this in.
    def serialise(self) -> str:
        return str(self.head)
