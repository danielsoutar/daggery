from typing import List

from pydantic import BaseModel, model_validator


class EmptyDAG(BaseModel):
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


def parse_linear_list_string(dag_string: str) -> UnvalidatedDAG | EmptyDAG:
    if dag_string == "":
        return EmptyDAG(message="DAG string is empty and therefore invalid")

    rule_names = list(map(str.strip, dag_string.split(">>")))
    nodes = []
    current_names = {rule: 0 for rule in rule_names}

    for i, rule_name in enumerate(rule_names[:-1]):
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
    return UnvalidatedDAG(nodes=nodes + [last_node])
