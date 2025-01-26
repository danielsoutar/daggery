from typing import Dict, List, Union

from pydantic import BaseModel, model_validator

from .node import Bar, Baz, Foo, Node, Quux, Qux
from .request import ArgumentMappingMetadata
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


class InvalidSequence(BaseModel):
    message: str


class UnvalidatedNode(BaseModel):
    name: str
    rule: str
    children: List[str] = []


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
        current_names = {rule: 0 for rule in rule_names}

        for i, rule_name in enumerate(rule_names[:-1]):
            # This indexing ensures each node has a unique name.
            parent, child = rule_name, rule_names[i + 1]
            parent_name = parent + str(current_names[parent])
            current_names[parent] += 1
            child_name = child + str(current_names[child])

            nodes.append(
                UnvalidatedNode(
                    name=parent_name,
                    rule=parent,
                    children=[child_name],
                )
            )

        last_node_name = rule_names[-1] + str(current_names[rule_names[-1]])
        last_node = UnvalidatedNode(name=last_node_name, rule=rule_names[-1])
        return cls(nodes=nodes + [last_node])

    @classmethod
    def from_node_list(
        cls,
        dag_list: List[UnvalidatedNode],
        argument_mappings: List[ArgumentMappingMetadata],
    ) -> Union["UnvalidatedDAG", InvalidGraph]:
        # is_list = isinstance(dag_list, list)
        # if not (
        #     is_list and all(isinstance(node, UnvalidatedNode) for node in dag_list)
        # ):
        #     return InvalidGraph(
        #         message=f"Input must be a list of dictionaries: {dag_list}"
        #     )

        # nodes, seen_names = [], set()
        # for node in dag_list:
        #     node = UnvalidatedNode(**node)
        #     seen_names.add(node.name)
        #     # Check if any children reference a previously seen name
        #     if any(child in seen_names for child in node.children):
        #         return InvalidGraph(
        #             message=f"Input is not topologically sorted: {node} references {seen_names}"
        #         )
        #     nodes.append(node)
        return UnvalidatedDAG(
            nodes=[UnvalidatedNode(name="dummy", rule="foo", children=[])]
        )
