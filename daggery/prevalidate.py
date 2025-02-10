from collections import defaultdict
from typing import Tuple, Union

from pydantic import BaseModel, model_validator

from .description import DAGDescription
from .utils.logging import logger_factory

logger = logger_factory(__name__)


class EmptyDAG(BaseModel):
    message: str


class InvalidDAG(BaseModel):
    message: str


class PrevalidatedNode(BaseModel):
    # A descriptive name for this specific node. It must be unique.
    name: str
    # The classname of the underlying node to evaluate.
    node_class: str
    # The names of dependent nodes. These must be unique.
    children: Tuple[str, ...] = ()
    # The names of nodes this node depends on. These must be unique.
    input_nodes: Tuple[str, ...] = ()

    @model_validator(mode="after")
    def name_and_node_class_not_empty(self):
        if self.name == "":
            raise ValueError("PrevalidatedNode must have a name")
        if self.node_class == "":
            raise ValueError("PrevalidatedNode must have a node classname")
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

    nodes: Tuple[PrevalidatedNode, ...]

    @model_validator(mode="after")
    def dag_not_empty(self):
        if len(self.nodes) == 0:
            raise ValueError("PrevalidatedDAG must contain at least one node")
        return self

    @classmethod
    def from_string(cls, dag_description: str) -> Union["PrevalidatedDAG", EmptyDAG]:
        dag_description = dag_description.strip()
        if dag_description == "":
            return EmptyDAG(message="DAG string is empty and therefore invalid")

        node_class_names = list(map(str.strip, dag_description.split(">>")))
        nodes = []
        seen_names = {classname: 0 for classname in node_class_names}
        node_parents: dict[str, str] = {}

        for i, node_class_name in enumerate(node_class_names[:-1]):
            # This indexing ensures each node has a unique name.
            current, child = node_class_name, node_class_names[i + 1]
            current_name = current + str(seen_names[current])
            seen_names[current] += 1
            child_name = child + str(seen_names[child])
            parent_name = node_parents.get(current_name, None)

            nodes.append(
                PrevalidatedNode(
                    name=current_name,
                    node_class=current,
                    children=(child_name,),
                    input_nodes=(parent_name,) if parent_name else (),
                )
            )
            node_parents[child_name] = current_name

        last_node_name = node_class_names[-1] + str(seen_names[node_class_names[-1]])
        parent_name = node_parents.get(last_node_name, None)

        last_node = PrevalidatedNode(
            name=last_node_name,
            node_class=node_class_names[-1],
            input_nodes=(parent_name,) if parent_name else (),
        )
        return cls(nodes=(*nodes, last_node))

    @classmethod
    def from_dag_description(
        cls, dag_description: DAGDescription
    ) -> Union["PrevalidatedDAG", InvalidDAG]:
        argument_mappings = {
            mapping.op_name: {"inputs": mapping.inputs}
            for mapping in dag_description.argument_mappings
        }
        nodes: list[PrevalidatedNode] = []
        seen_names: set[str] = set()
        parents_of_nodes: dict[str, list[str]] = defaultdict(list)
        for op in dag_description.operations.ops:
            mapping_available = op.name in argument_mappings.keys()
            op_node_mappings: dict = {}
            if mapping_available:
                # Non-root case, assumed to have >1 inputs.
                op_node_mappings = argument_mappings[op.name]
            else:
                if parents_of_nodes == {}:
                    # This must be the root.
                    op_node_mappings = {"inputs": ()}
                else:
                    # Non-root case with no mapping - assumed to be unambiguous,
                    # meaning exactly one input.
                    if op.name not in parents_of_nodes.keys():
                        return InvalidDAG(
                            message=(
                                f"Input has >1 root node: {op.name} has no "
                                f"parents in {dag_description}"
                            )
                        )
                    node_input = parents_of_nodes[op.name][0]
                    op_node_mappings = {"inputs": (node_input,)}
            node = PrevalidatedNode(
                name=op.name,
                node_class=op.op_name,
                children=op.children,
                input_nodes=op_node_mappings["inputs"],
            )
            seen_names.add(node.name)
            for child in node.children:
                parents_of_nodes[child].append(node.name)
            # Check if any children reference a previously seen name,
            # since we iterate from first to last, this indicates a cycle.
            # By extension this also implies that the input is not topologically
            # sorted. As a bonus it ensures unique names!
            if any(child in seen_names for child in node.children):
                return InvalidDAG(
                    message=f"Input is not topologically sorted: {node} references {seen_names}"
                )
            # Check that mappings align with the relationships.
            parents = [n for n in nodes if n.name in op_node_mappings["inputs"]]
            correct_relationships = all(
                node.name in parent.children for parent in parents
            )
            correct_inputs = set(parents_of_nodes[node.name]) == set(
                op_node_mappings["inputs"]
            )
            if not correct_relationships or not correct_inputs:
                return InvalidDAG(
                    message=(
                        f"Input has invalid mappings: {node} has {parents=} "
                        f"but has these mappings: {op_node_mappings}"
                    )
                )
            nodes.append(node)
        # Ensure there is one tail.
        tails = list(filter(lambda n: n.children == (), nodes))
        if len(tails) != 1:
            return InvalidDAG(message=f"Input has {len(tails)} tails: {tails}")
        return cls(nodes=tuple(nodes))
