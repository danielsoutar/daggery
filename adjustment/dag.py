from collections import deque
from typing import Dict, Union

from pydantic import BaseModel

from .node import Bar, Baz, Foo, Node, Quux, Qux
from .parse import EmptyDAG, UnvalidatedDAG, UnvalidatedNode, parse_linear_list_string
from .utils import logger_factory

logger = logger_factory(__name__)


node_map: Dict[str, type[Node]] = {
    "foo": Foo,
    "bar": Bar,
    "baz": Baz,
    "qux": Qux,
    "quux": Quux,
}


class InvalidGraph(BaseModel):
    message: str


class DAG(BaseModel):
    head: Node

    def __init__(self, head: Node):
        # Set the head of the DAG
        super().__init__(head=head)

    @classmethod
    def create(cls, unvalidated_dag: UnvalidatedDAG) -> Union["DAG", InvalidGraph]:
        node_names = [node.name for node in unvalidated_dag.nodes]
        if len(node_names) != len(set(node_names)):
            return InvalidGraph(message="DAG must contain unique node names")

        node_rules = [node.rule for node in unvalidated_dag.nodes]
        for rule in node_rules:
            if rule not in node_map.keys():
                return InvalidGraph(
                    message=f"Invalid rule found in unvalidated DAG: {rule}"
                )

        graph_nodes: dict[str, Node] = {}
        child_counts: dict[str, int] = {name: 0 for name in node_names}
        parent_counts: dict[str, int] = {name: 0 for name in node_names}
        head: Node = Foo(name="dummy", children=[])

        # Creating immutable nodes back-to-front guarantees an immutable DAG.
        for unvalidated_node in reversed(unvalidated_dag.nodes):
            name = unvalidated_node.name
            child_nodes: list[Node] = []

            for child_name in unvalidated_node.children:
                child_nodes.append(graph_nodes[child_name])
                parent_counts[child_name] += 1

            child_counts[name] = len(unvalidated_node.children)

            node_class = node_map[unvalidated_node.rule]
            node = node_class(name=name, children=child_nodes)

            graph_nodes[name] = node
            head = node

        # Confirm there is exactly one head node. The head has no parent.
        heads = list(filter(lambda count: count == 0, parent_counts.values()))
        if len(heads) != 1:
            return InvalidGraph(message=f"DAG must have exactly one head node: {heads}")

        # Confirm there is exactly one tail node. The tail has no children.
        tails = list(filter(lambda count: count == 0, child_counts.values()))
        if len(tails) != 1:
            return InvalidGraph(message=f"DAG must have exactly one tail node: {tails}")

        return cls(head=head)

    @classmethod
    def from_input(cls, input_data: str | list[dict]) -> Union["DAG", InvalidGraph]:
        if isinstance(input_data, str):
            # Input is a linear string representation
            linear_dag = parse_linear_list_string(input_data)
            if isinstance(linear_dag, EmptyDAG):
                return InvalidGraph(message=linear_dag.message)
            return cls.create(unvalidated_dag=linear_dag)

        is_list = isinstance(input_data, list)
        if not (is_list and all(isinstance(node, dict) for node in input_data)):
            return InvalidGraph(
                message=f"Input must be a list of dictionaries: {input_data}"
            )

        nodes, seen_names = [], set()
        for node_dict in input_data:
            node = UnvalidatedNode(**node_dict)
            seen_names.add(node.name)
            # Check if any children reference a previously seen name
            if any(child in seen_names for child in node.children):
                return InvalidGraph(
                    message=f"Input is not topologically sorted: {node} references {seen_names}"
                )
            nodes.append(node)
        unvalidated_dag = UnvalidatedDAG(nodes=nodes)

        return cls.create(unvalidated_dag)

    def transform(self, value: int) -> int:
        node_queue = deque([self.head])
        val_queue = deque([value])

        while node_queue:
            current_node = node_queue.popleft()
            node_input_value = val_queue.popleft()

            # Log the node name and intermediate result.
            logger.info(
                f"Node: {current_node.__class__.__name__}, "
                f"Intermediate Result (input to this Node): {node_input_value}"
            )

            node_output_value = current_node.transform(node_input_value)
            node_queue.extend(current_node.children)
            val_queue.extend([node_output_value] * len(current_node.children))

        # The last node_output_value is the final transformed result
        return node_output_value
