from abc import ABC, abstractmethod
from collections import defaultdict
from typing import Dict, List, Union

from pydantic import BaseModel, model_validator

from .node import Bar, Baz, Foo, Node, Quux, Qux
from .request import ArgumentMappingMetadata, OperationList
from .utils import logger_factory

logger = logger_factory(__name__)


node_map: Dict[str, type[Node]] = {
    "foo": Foo,
    "bar": Bar,
    "baz": Baz,
    "qux": Qux,
    "quux": Quux,
}


# Abstract type for a consistent interface working with function graphs.
class AbstractFunctionGraph(BaseModel, ABC):
    head: Node

    @abstractmethod
    def transform(self, value):
        pass

    @abstractmethod
    def serialise(self):
        pass


class EmptyDAG(BaseModel):
    message: str


class InvalidGraph(BaseModel):
    message: str


class InvalidSequence(BaseModel):
    message: str


class UnvalidatedNode(BaseModel):
    # The name of this node. It must be unique.
    name: str
    # The rule of this node.
    rule: str
    # The names of the nodes that depend on this node.
    children: List[str] = []
    # The names of the arguments that this node depends on. These must be unique.
    input_values: List[str] = []
    # The name of the argument that this node produces. This must be unique.
    output_value: str


class UnvalidatedDAG(BaseModel):
    """
    This represents an unvalidated DAG, assumed to be in topologically-sorted
    order.
    """

    nodes: List[UnvalidatedNode]

    @model_validator(mode="after")
    def dag_not_empty(self):
        if len(self.nodes) == 0:
            raise ValueError("UnvalidatedDAG must contain at least one node")
        return self

    @classmethod
    def from_string(cls, dag_string: str) -> Union["UnvalidatedDAG", EmptyDAG]:
        dag_string = dag_string.strip()
        if dag_string == "":
            return EmptyDAG(message="DAG string is empty and therefore invalid")

        rule_names = list(map(str.strip, dag_string.split(">>")))
        nodes = []
        seen_names = {rule: 0 for rule in rule_names}

        for i, rule_name in enumerate(rule_names[:-1]):
            # This indexing ensures each node has a unique name.
            parent, child = rule_name, rule_names[i + 1]
            parent_name = parent + str(seen_names[parent])
            seen_names[parent] += 1
            child_name = child + str(seen_names[child])

            nodes.append(
                UnvalidatedNode(
                    name=parent_name,
                    rule=parent,
                    children=[child_name],
                    input_values=[],
                    output_value="",
                )
            )

        last_node_name = rule_names[-1] + str(seen_names[rule_names[-1]])
        last_node = UnvalidatedNode(
            name=last_node_name,
            rule=rule_names[-1],
            output_value="",
        )
        return cls(nodes=nodes + [last_node])

    @classmethod
    def from_node_list(
        cls,
        dag_op_list: OperationList,
        argument_mappings_list: List[ArgumentMappingMetadata],
    ) -> Union["UnvalidatedDAG", InvalidGraph]:
        argument_mappings = {
            mapping.node_name: {
                "inputs": mapping.inputs,
                "output": mapping.node_name,
            }
            for mapping in argument_mappings_list
        }
        nodes: list[UnvalidatedNode] = []
        seen_names: set[str] = set()
        parents_of_nodes: dict[str, list[str]] = defaultdict(list)
        for op in dag_op_list.items:
            mapping_available = op.name in argument_mappings.keys()
            node_mappings: dict = {}
            if mapping_available:
                # Non-root case, assumed to have >1 inputs.
                node_mappings = argument_mappings[op.name]
            else:
                if parents_of_nodes == {}:
                    # This must be the root.
                    node_mappings = {
                        "inputs": [],
                        "output": op.name,
                    }
                else:
                    # Non-root case with no mapping - assumed to be unambiguous,
                    # meaning exactly one input.
                    if op.name not in parents_of_nodes.keys():
                        return InvalidGraph(
                            message=(
                                f"Input has >1 root node: {op.name} has no "
                                f"parents in {dag_op_list}"
                            )
                        )
                    node_input = parents_of_nodes[op.name][0]
                    node_mappings = {
                        "inputs": [node_input],
                        "output": op.name,
                    }
            node = UnvalidatedNode(
                name=op.name,
                rule=op.rule,
                children=op.children,
                input_values=node_mappings["inputs"],
                output_value=node_mappings["output"],
            )
            seen_names.add(node.name)
            for child in node.children:
                parents_of_nodes[child].append(node.name)
            # Check if any children reference a previously seen name,
            # since we iterate from first to last, this indicates a cycle.
            # By extension this also implies that the input is not topologically
            # sorted.
            if any(child in seen_names for child in node.children):
                return InvalidGraph(
                    message=f"Input is not topologically sorted: {node} references {seen_names}"
                )
            # Check that mappings align with the relationships.
            parents = [n for n in nodes if n.name in node_mappings["inputs"]]
            correct_relationships = all(
                node.name in parent.children for parent in parents
            )
            correct_inputs = set(parents_of_nodes[node.name]) == set(
                node_mappings["inputs"]
            )
            if not correct_relationships or not correct_inputs:
                return InvalidGraph(
                    message=(
                        f"Input has invalid mappings: {node} has {parents=} "
                        f"but has these mappings: {node_mappings}"
                    )
                )
            nodes.append(node)
        # Ensure there is one tail.
        tails = list(filter(lambda n: n.children == [], nodes))
        if len(tails) != 1:
            return InvalidGraph(message=f"Input has {len(tails)} tails: {tails}")
        return cls(nodes=nodes)
