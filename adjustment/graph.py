from typing import Dict

from pydantic import BaseModel

from .models import Bar, Baz, Foo, Node, Quux, Qux


class DAG(BaseModel):
    head: Node

    def transform(self, value: int) -> int:
        return self.head.transform(value)


node_map: Dict[str, Node] = {
    "foo": Foo(),
    "bar": Bar(),
    "baz": Baz(),
    "qux": Qux(),
    "quux": Quux(),
}


# Factory function to create a map from names to node instances
def from_string(dag_string: str) -> DAG:
    node_names = list(map(str.strip, dag_string.split(">>")))
    # Validate node names
    if any(node_name not in node_map.keys() for node_name in node_names):
        raise ValueError(f"Invalid node name encountered in: {dag_string}")
    # Create a DAG object
    head_node = node_map[node_names[0]]
    current = head_node
    # Because Nodes cannot point to Nodes with their own children set, we must
    # set nodes from head to tail.
    for node_name in node_names[1:]:
        node = node_map[node_name]
        current.child = node
        current = node
    return DAG(head=head_node)
