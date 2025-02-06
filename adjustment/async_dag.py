import asyncio
from typing import Any, List, Tuple, Union

from pydantic import BaseModel

from .async_node import AsyncNode
from .graph import EmptyDAG, InvalidGraph, PrevalidatedDAG
from .request import ArgumentMappingMetadata, OperationList
from .utils import _logger_factory

logger = _logger_factory(__name__)


class AsyncAnnotatedNode(BaseModel):
    naked_node: AsyncNode
    input_nodes: Tuple[str, ...]

    async def transform(self, *args) -> Any:
        return await self.naked_node.transform(*args)


class AsyncFunctionDAG(BaseModel):
    nodes: Tuple[Tuple[AsyncAnnotatedNode, ...], ...]

    @property
    def is_sequence(self) -> bool:
        return all(
            len(node.input_nodes) <= 1
            for node_batch in self.nodes
            for node in node_batch
        )

    # We separate the creation of the DAG from the init method since this allows
    # returning instances of InvalidGraph, making this code exception-free.
    @classmethod
    def from_prevalidated_dag(
        cls,
        prevalidated_dag: PrevalidatedDAG,
        custom_node_map: dict[str, type[AsyncNode]],
    ) -> Union["AsyncFunctionDAG", InvalidGraph]:
        node_rules = [node.rule for node in prevalidated_dag.nodes]
        for rule in node_rules:
            if rule not in custom_node_map.keys():
                return InvalidGraph(
                    message=f"Invalid rule found in unvalidated DAG: {rule}"
                )

        graph_nodes: dict[str, AsyncNode] = {}
        current_batch: list[AsyncAnnotatedNode] = []
        ordered_nodes: list[tuple[AsyncAnnotatedNode, ...]] = []

        # Creating immutable nodes back-to-front guarantees an immutable DAG.
        # When building the graph, we keep track of all nodes in the same
        # logical 'batch'. A batch is just a set of nodes where none of them
        # have any parent/child relationships between them, direct or otherwise.
        # This implies they are independent of each other. Starting from the
        # tail (which is the last batch and has size 1), we build up a set of
        # nodes and ensure none of them are each other's parent/child. If and
        # when this eventually happens, we know we have crossed into another
        # batch. Consequently, this set of nodes is stored as a batch, and we
        # create the next set with the current node in the next batch.
        for prevalidated_node in reversed(prevalidated_dag.nodes):
            name = prevalidated_node.name
            child_nodes = [graph_nodes[child] for child in prevalidated_node.children]

            node_class = custom_node_map[prevalidated_node.rule]
            node = node_class(name=name, children=tuple(child_nodes))

            graph_nodes[name] = node
            # We have a special case for the root node, enabling a standard
            # fetching of inputs in the transform.
            input_nodes = tuple(prevalidated_node.input_nodes) or ("__INPUT__",)
            annotated_node = AsyncAnnotatedNode(
                naked_node=node,
                input_nodes=input_nodes,
            )
            # Given the order of traversal, check if any nodes in the current batch
            # are children of this node. Given the sortedness we know they can't be
            # its parents.
            child_names = [child.name for child in child_nodes]
            found_new_batch = any(
                sibling.naked_node.name in child_names for sibling in current_batch
            )
            if found_new_batch:
                ordered_nodes.append(tuple(reversed(current_batch)))
                current_batch = [annotated_node]
            else:
                current_batch.append(annotated_node)

        # Ensure the last batch is added.
        ordered_nodes.append(tuple(reversed(current_batch)))
        return cls(nodes=tuple(reversed(ordered_nodes)))

    @classmethod
    def from_node_list(
        cls,
        graph_description: OperationList,
        argument_mappings: List[ArgumentMappingMetadata],
        custom_node_map: dict[str, type[AsyncNode]],
    ) -> Union["AsyncFunctionDAG", InvalidGraph]:
        prevalidated_dag = PrevalidatedDAG.from_node_list(
            graph_description,
            argument_mappings,
        )
        if isinstance(prevalidated_dag, InvalidGraph):
            return prevalidated_dag
        return cls.from_prevalidated_dag(prevalidated_dag, custom_node_map)

    @classmethod
    def from_string(
        cls,
        graph_description: str,
        custom_node_map: dict[str, type[AsyncNode]],
    ) -> Union["AsyncFunctionDAG", InvalidGraph]:
        prevalidated_dag = PrevalidatedDAG.from_string(graph_description)
        if isinstance(prevalidated_dag, EmptyDAG):
            return InvalidGraph(message=prevalidated_dag.message)
        return cls.from_prevalidated_dag(prevalidated_dag, custom_node_map)

    async def transform(self, value: Any) -> Any:
        context = {"__INPUT__": value}
        # The nodes are topologically sorted. As it turns out, this is also
        # a valid order of evaluation - by the time a node is reached, all
        # of its parents will already have been evaluated.
        # When nodes are independent of each other - which we group in
        # 'batches', we can evaluate them concurrently.
        for batch in self.nodes:
            nodes_with_args = [
                (n, tuple(context[v] for v in n.input_nodes)) for n in batch
            ]
            tasks = [n.transform(*vs) for (n, vs) in nodes_with_args]
            output_values = await asyncio.gather(*tasks)
            zipped_nodes = zip(nodes_with_args, output_values)
            for (node, input_vs), output_v in zipped_nodes:
                self._pretty_log_node(node, input_vs, output_v)
                context[node.naked_node.name] = output_v
        return context[node.naked_node.name]

    def _pretty_log_node(
        self,
        node: AsyncAnnotatedNode,
        input_values: tuple[Any, ...],
        output_value: Any,
    ) -> None:
        input_node_names = node.input_nodes
        zipped_inputs = zip(input_values, input_node_names)
        tied_inputs = tuple(
            f"{inp_val}@{inp_name}" for inp_val, inp_name in zipped_inputs
        )
        formatted_inputs = tied_inputs[0] if len(tied_inputs) == 1 else tied_inputs
        logger.info(f"Node: {node.naked_node.name}:")
        logger.info(f"  Input(s): {formatted_inputs}")
        logger.info(f"  Output(s): {output_value}")
