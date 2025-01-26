from collections import deque
from typing import Union

from pydantic import BaseModel

from .graph import InvalidGraph, UnvalidatedDAG, UnvalidatedNode, node_map
from .node import Foo, Node
from .request import Operation
from .utils import logger_factory

logger = logger_factory(__name__)


class FunctionDAG(BaseModel):
    head: Node

    def __init__(self, head: Node):
        # Set the head of the DAG
        super().__init__(head=head)

    # We separate the creation of the DAG from the init method since this allows
    # returning instances of InvalidGraph, making this code exception-free.
    @classmethod
    def from_unvalidated_dag(
        cls, unvalidated_dag: UnvalidatedDAG
    ) -> Union["FunctionDAG", InvalidGraph]:
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
        # Set a dummy node to ensure head is never null.
        head: Node = Foo(name="dummy", children=())

        # Creating immutable nodes back-to-front guarantees an immutable DAG.
        for unvalidated_node in reversed(unvalidated_dag.nodes):
            name = unvalidated_node.name
            child_nodes: list[Node] = []

            for child_name in unvalidated_node.children:
                child_nodes.append(graph_nodes[child_name])
                parent_counts[child_name] += 1

            child_counts[name] = len(unvalidated_node.children)

            node_class = node_map[unvalidated_node.rule]
            node = node_class(name=name, children=tuple(child_nodes))

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
    def from_node_list(
        cls, dag_op_list: list[Operation]
    ) -> Union["FunctionDAG", InvalidGraph]:
        is_list = isinstance(dag_op_list, list)
        if not (is_list and all(isinstance(op, Operation) for op in dag_op_list)):
            return InvalidGraph(
                message=f"Input must be a list of Operation: {dag_op_list}"
            )

        nodes, seen_names = [], set()
        for op in dag_op_list:
            node = UnvalidatedNode.model_validate_json(op.model_dump_json())
            seen_names.add(node.name)
            # Check if any children reference a previously seen name
            if any(child in seen_names for child in node.children):
                return InvalidGraph(
                    message=f"Input is not topologically sorted: {node} references {seen_names}"
                )
            nodes.append(node)
        unvalidated_dag = UnvalidatedDAG(nodes=nodes)

        return cls.from_unvalidated_dag(unvalidated_dag)

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
