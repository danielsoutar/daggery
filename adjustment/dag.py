from typing import Any, List, Tuple, Union

from pydantic import BaseModel

from .graph import (
    InvalidGraph,
    PrevalidatedDAG,
    node_map,
)
from .node import Node
from .request import ArgumentMappingMetadata, OperationList
from .utils import logger_factory

logger = logger_factory(__name__)


class AnnotatedNode(BaseModel):
    naked_node: Node
    input_values: Tuple[str, ...]
    output_value: str

    def transform(self, value: Any) -> Any:
        return self.naked_node.transform(value)


class FunctionDAG(BaseModel):
    nodes: Tuple[AnnotatedNode, ...]

    @property
    def head(self) -> AnnotatedNode:
        return self.nodes[0]

    # We separate the creation of the DAG from the init method since this allows
    # returning instances of InvalidGraph, making this code exception-free.
    @classmethod
    def from_prevalidated_dag(
        cls, prevalidated_dag: PrevalidatedDAG
    ) -> Union["FunctionDAG", InvalidGraph]:
        node_rules = [node.rule for node in prevalidated_dag.nodes]
        for rule in node_rules:
            if rule not in node_map.keys():
                return InvalidGraph(
                    message=f"Invalid rule found in unvalidated DAG: {rule}"
                )

        graph_nodes: dict[str, Node] = {}
        ordered_nodes: list[AnnotatedNode] = []

        # Creating immutable nodes back-to-front guarantees an immutable DAG.
        for prevalidated_node in reversed(prevalidated_dag.nodes):
            name = prevalidated_node.name
            child_nodes = [graph_nodes[child] for child in prevalidated_node.children]

            node_class = node_map[prevalidated_node.rule]
            node = node_class(name=name, children=tuple(child_nodes))

            graph_nodes[name] = node
            ordered_nodes.append(
                AnnotatedNode(
                    naked_node=node,
                    input_values=tuple(prevalidated_node.input_values),
                    output_value=prevalidated_node.output_value,
                )
            )

        return cls(nodes=tuple(reversed(ordered_nodes)))

    @classmethod
    def from_node_list(
        cls,
        dag_op_list: OperationList,
        argument_mappings: List[ArgumentMappingMetadata],
    ) -> Union["FunctionDAG", InvalidGraph]:
        prevalidated_dag = PrevalidatedDAG.from_node_list(
            dag_op_list,
            argument_mappings,
        )
        if isinstance(prevalidated_dag, InvalidGraph):
            return prevalidated_dag
        return cls.from_prevalidated_dag(prevalidated_dag)

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

        context: dict[str, Any] = {}
        for node in self.nodes:
            input_values = [context[value] for value in node.input_values]
            logger.info(
                (
                    f"Node: {node.__class__.__name__}, "
                    f"Intermediate Result(s): {input_values}"
                )
            )
            node_output_value = node.transform(*input_values)
            context[node.output_value] = node_output_value
        return node_output_value

    # TODO: Fill this in.
    def serialise(self) -> str:
        return str(self.head)
