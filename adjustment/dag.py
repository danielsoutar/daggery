from collections import deque
from typing import Any, List, Union

from .graph import (
    AbstractFunctionGraph,
    EmptyDAG,
    InvalidGraph,
    UnvalidatedDAG,
    node_map,
)
from .node import Foo, Node
from .request import ArgumentMappingMetadata, OperationList
from .utils import logger_factory

logger = logger_factory(__name__)


class FunctionDAG(AbstractFunctionGraph):
    head: Node

    def __init__(self, head: Node):
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
        cls,
        dag_op_list: OperationList,
        argument_mappings: List[ArgumentMappingMetadata],
    ) -> Union["FunctionDAG", InvalidGraph]:
        unvalidated_dag = UnvalidatedDAG.from_node_list(
            dag_op_list,
            argument_mappings,
        )
        if isinstance(unvalidated_dag, InvalidGraph):
            return unvalidated_dag
        return cls.from_unvalidated_dag(unvalidated_dag)

    def transform(self, value: Any) -> Any:
        # The context could contain values and intermediate values.
        # Alternatively, or maybe additionally, it can also contain
        # node mappings.
        # If (intermediate) values are contained but not mappings,
        # this would imply nodes have to contain mappings instead.
        # Concretely, it might look something like:
        #
        # inputs_available = lambda node: all(
        #   value in context for value in node.input_values
        # )
        # evaluable_nodes = list(map(inputs_available, node_queue))
        # node_queue = list(filter(lambda n: n not in evaluable_nodes, node_queue))
        #
        # for node in evaluable_nodes:
        #   input_values = [context[value] for value in node.input_values]
        #   logger.info(
        #       f"Node: {node.__class__.__name__}, "
        #       f"Intermediate Result(s): {input_values}
        #   )
        #   node_output_value = node.transform(*input_values)
        #   context[node.output_value] = node_output_value
        #   node_queue.extend(node.children)
        # # Last node evaluated will be the tail, and therefore this reference
        # # will return the final result.
        # return context[node.output_value]
        # -------------------------
        # In other words, nodes would store the names of their input values
        # and output values. They would need to be mangled in a way that ensures
        # uniqueness. It would make sense to do this during construction.
        # TODO: Figure out whether names should be allowed and unique.
        # Nodes could be stored in a list in topologically-sorted
        # order, and thus also a valid execution/evaluation order.
        # Concretely, this would change the above to something like this:
        #
        # for node in node_queue:
        #   input_values = [context[value] for value in node.input_values]
        #   logger.info(
        #       f"Node: {node.__class__.__name__}, "
        #       f"Intermediate Result(s): {input_values}
        #   )
        #   node_output_value = node.transform(*input_values)
        #   context[node.output_value] = node_output_value
        # # Last node evaluated will be the tail, and therefore this reference
        # # will return the final result.
        # return context[node.output_value]
        # -------------------------
        # You could additionally optimise this to run nodes concurrently:
        # offsets = ...
        # offset_idx, pos = 0, 0
        # while pos != len(node_queue):
        #   current_offset = offsets[offset_idx]
        #   tasks = []
        #   output_names = []
        #   for node in node_queue[pos:pos+current_offset]:
        #       input_values = [context[value] for value in node.input_values]
        #       logger.info(
        #           f"Node: {node.__class__.__name__}, "
        #           f"Intermediate Result(s): {input_values}
        #       )
        #       tasks.append(node.transform(*input_values))
        #       output_names.append(node.output_value)
        #   output_values = await asyncio.gather(*tasks)
        #   for output_name, output_value in zip(output_names, output_values):
        #       context[output_name] = output_value
        #   pos += current_offset
        #   offset_idx += 1
        # return context[node.output_value]
        #
        pass

    # TODO: Fill this in.
    def serialise(self) -> str:
        return str(self.head)
