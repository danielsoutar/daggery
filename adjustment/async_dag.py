import asyncio
from typing import Any, List, Tuple, Union

from pydantic import BaseModel

from .async_node import AsyncNode
from .graph import EmptyDAG, InvalidGraph, PrevalidatedDAG, async_node_map
from .request import ArgumentMappingMetadata, OperationList
from .utils import logger_factory

logger = logger_factory(__name__)


class AsyncAnnotatedNode(BaseModel):
    naked_node: AsyncNode
    input_nodes: Tuple[str, ...]

    async def transform(self, *args) -> Any:
        return await self.naked_node.transform(*args)


class AsyncFunctionDAG(BaseModel):
    nodes: Tuple[Tuple[AsyncAnnotatedNode, ...], ...]

    @property
    def head(self) -> AsyncAnnotatedNode:
        return self.nodes[0][0]

    @property
    def is_sequence(self) -> bool:
        return all(
            len(node.input_nodes) <= 1
            for node_level in self.nodes
            for node in node_level
        )

    # We separate the creation of the DAG from the init method since this allows
    # returning instances of InvalidGraph, making this code exception-free.
    @classmethod
    def from_prevalidated_dag(
        cls,
        prevalidated_dag: PrevalidatedDAG,
        node_map: dict[str, type[AsyncNode]] = async_node_map,
    ) -> Union["AsyncFunctionDAG", InvalidGraph]:
        node_rules = [node.rule for node in prevalidated_dag.nodes]
        for rule in node_rules:
            if rule not in node_map.keys():
                return InvalidGraph(
                    message=f"Invalid rule found in unvalidated DAG: {rule}"
                )

        graph_nodes: dict[str, AsyncNode] = {}
        current_level: list[AsyncAnnotatedNode] = []
        ordered_nodes: list[tuple[AsyncAnnotatedNode, ...]] = []

        # Creating immutable nodes back-to-front guarantees an immutable DAG.
        # When building the graph, we keep track of all nodes at the same
        # logical 'level'. A logical level is simply a set of nodes where
        # none of them have any parent/child relationships between them,
        # direct or otherwise. This implies they are independent of each other.
        # Starting from the tail (which is the last level and has size 1),
        # we build up a set of nodes where we check none of them are each
        # other's parent/child. If and when this eventually happens, we know
        # we have crossed into another level. Consequently, this set of nodes
        # is stored as a level, and we create the next set with the current
        # node in the next level.
        for prevalidated_node in reversed(prevalidated_dag.nodes):
            name = prevalidated_node.name
            child_nodes = [graph_nodes[child] for child in prevalidated_node.children]

            node_class = node_map[prevalidated_node.rule]
            node = node_class(name=name, children=tuple(child_nodes))

            graph_nodes[name] = node
            # We have a special case for the root node, enabling a standard
            # fetching of inputs in the transform.
            input_nodes = tuple(prevalidated_node.input_nodes) or ("__INPUT__",)
            annotated_node = AsyncAnnotatedNode(
                naked_node=node,
                input_nodes=input_nodes,
            )
            # Given the order of traversal, check if any nodes in the current level
            # are children of this node. Given the sortedness we know they can't be
            # its parents.
            child_names = [child.name for child in child_nodes]
            found_new_level = any(
                sibling.naked_node.name in child_names for sibling in current_level
            )
            if found_new_level:
                ordered_nodes.append(tuple(reversed(current_level)))
                current_level = [annotated_node]
            else:
                current_level.append(annotated_node)

        # Ensure the last level as added.
        ordered_nodes.append(tuple(reversed(current_level)))
        return cls(nodes=tuple(reversed(ordered_nodes)))

    @classmethod
    def from_node_list(
        cls,
        dag_op_list: OperationList,
        argument_mappings: List[ArgumentMappingMetadata],
    ) -> Union["AsyncFunctionDAG", InvalidGraph]:
        prevalidated_dag = PrevalidatedDAG.from_node_list(
            dag_op_list,
            argument_mappings,
        )
        if isinstance(prevalidated_dag, InvalidGraph):
            return prevalidated_dag
        return cls.from_prevalidated_dag(prevalidated_dag)

    @classmethod
    def from_string(cls, dag_string: str) -> Union["AsyncFunctionDAG", InvalidGraph]:
        prevalidated_dag = PrevalidatedDAG.from_string(dag_string)
        if isinstance(prevalidated_dag, EmptyDAG):
            return InvalidGraph(message=prevalidated_dag.message)
        return cls.from_prevalidated_dag(prevalidated_dag)

    async def transform(self, value: Any) -> Any:
        context = {"__INPUT__": value}
        # The nodes are topologically sorted. As it turns out, this is also
        # a valid order of evaluation - by the time a node is reached, all
        # of its parents will already have been evaluated.
        # When nodes are independent of each other - which we group in
        # 'levels', we can evaluate them concurrently.
        for level in self.nodes:
            nodes_with_args = [
                (n, tuple(context[v] for v in n.input_nodes)) for n in level
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

    # TODO: Fill this in.
    def serialise(self) -> str:
        return str(self.head)
