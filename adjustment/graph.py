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


class EmptyDAG(BaseModel):
    message: str


class InvalidGraph(BaseModel):
    message: str


class PrevalidatedNode(BaseModel):
    # The name of this node. It must be unique.
    name: str
    # The rule of this node.
    rule: str
    # The names of the nodes that depend on this node.
    children: List[str] = []
    # The names of the nodes that this node depends on. These must be unique.
    input_nodes: List[str] = []

    @model_validator(mode="after")
    def name_and_rule_not_empty(self):
        if self.name == "":
            raise ValueError("PrevalidatedNode must have a name")
        if self.rule == "":
            raise ValueError("PrevalidatedNode must have a rule")
        return self

    @model_validator(mode="after")
    def unique_names(self):
        if len(self.children) != len(set(self.children)):
            raise ValueError("PrevalidatedNode must have unique children")
        if len(self.input_nodes) != len(set(self.input_nodes)):
            raise ValueError("PrevalidatedNode must have unique input nodes")
        return self


class PrevalidatedDAG(BaseModel):
    """
    This represents an pre-validated DAG. It guarantees the following on
    construction:

    * The graph cannot be empty.
    * The graph has exactly one head, and exactly one tail.
    * The graph is topologically sorted.
    * Each node has a unique name.
    * There is at most one connection between any pair of nodes.
    """

    nodes: List[PrevalidatedNode]

    @model_validator(mode="after")
    def dag_not_empty(self):
        if len(self.nodes) == 0:
            raise ValueError("PrevalidatedDAG must contain at least one node")
        return self

    @classmethod
    def from_string(cls, dag_string: str) -> Union["PrevalidatedDAG", EmptyDAG]:
        dag_string = dag_string.strip()
        if dag_string == "":
            return EmptyDAG(message="DAG string is empty and therefore invalid")

        rule_names = list(map(str.strip, dag_string.split(">>")))
        nodes = []
        seen_names = {rule: 0 for rule in rule_names}
        node_parents: dict[str, str] = {}

        for i, rule_name in enumerate(rule_names[:-1]):
            # This indexing ensures each node has a unique name.
            current, child = rule_name, rule_names[i + 1]
            current_name = current + str(seen_names[current])
            seen_names[current] += 1
            child_name = child + str(seen_names[child])
            parent_name = node_parents.get(current_name, None)

            nodes.append(
                PrevalidatedNode(
                    name=current_name,
                    rule=current,
                    children=[child_name],
                    input_nodes=[parent_name] if parent_name else [],
                )
            )
            node_parents[child_name] = current_name

        last_node_name = rule_names[-1] + str(seen_names[rule_names[-1]])
        parent_name = node_parents.get(last_node_name, None)

        last_node = PrevalidatedNode(
            name=last_node_name,
            rule=rule_names[-1],
            input_nodes=[parent_name] if parent_name else [],
        )
        return cls(nodes=nodes + [last_node])

    @classmethod
    def from_node_list(
        cls,
        dag_op_list: OperationList,
        argument_mappings_list: List[ArgumentMappingMetadata],
    ) -> Union["PrevalidatedDAG", InvalidGraph]:
        argument_mappings = {
            mapping.node_name: {"inputs": mapping.inputs}
            for mapping in argument_mappings_list
        }
        nodes: list[PrevalidatedNode] = []
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
                    node_mappings = {"inputs": []}
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
                    node_mappings = {"inputs": [node_input]}
            node = PrevalidatedNode(
                name=op.name,
                rule=op.rule,
                children=op.children,
                input_nodes=node_mappings["inputs"],
            )
            seen_names.add(node.name)
            for child in node.children:
                parents_of_nodes[child].append(node.name)
            # Check if any children reference a previously seen name,
            # since we iterate from first to last, this indicates a cycle.
            # By extension this also implies that the input is not topologically
            # sorted. As a bonus it ensures unique names!
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
