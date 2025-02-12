import asyncio
from typing import Any, Optional, Tuple, Union

from pydantic import BaseModel

from .async_node import AsyncNode
from .description import DAGDescription
from .prevalidate import EmptyDAG, InvalidDAG, PrevalidatedDAG
from .utils.logging import logger_factory

logger = logger_factory(__name__)


class AsyncDAGNode(BaseModel, frozen=True):
    naked_node: AsyncNode
    input_nodes: Tuple[str, ...]

    async def transform(self, *args) -> Any:
        return await self.naked_node.transform(*args)


class AsyncFunctionDAG(BaseModel, frozen=True):
    nodes: Tuple[Tuple[AsyncDAGNode, ...], ...]

    # We separate the creation of the DAG from the init method since this allows
    # returning instances of InvalidDAG, making this code exception-free.
    @classmethod
    def from_prevalidated_dag(
        cls,
        prevalidated_dag: PrevalidatedDAG,
        custom_op_node_map: dict[str, type[AsyncNode]],
    ) -> Union["AsyncFunctionDAG", InvalidDAG]:
        node_classes = [node.node_class for node in prevalidated_dag.nodes]
        for node_class in node_classes:
            if node_class not in custom_op_node_map.keys():
                return InvalidDAG(
                    message=f"Invalid internal node class found in prevalidated DAG: {node_class}"
                )

        current_batch: list[AsyncDAGNode] = []
        ordered_nodes: list[tuple[AsyncDAGNode, ...]] = []

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
            child_nodes = prevalidated_node.children

            node_class_constructor = custom_op_node_map[prevalidated_node.node_class]
            node = node_class_constructor(name=name, children=child_nodes)
            if not node.model_config.get("frozen", False):
                return InvalidDAG(
                    message=f"Mutable node found in DAG ({node}). This is not supported."
                )

            # We have a special case for the root node, enabling a standard
            # fetching of inputs in the transform.
            input_nodes = tuple(prevalidated_node.input_nodes) or ("__INPUT__",)
            annotated_node = AsyncDAGNode(
                naked_node=node,
                input_nodes=input_nodes,
            )
            # Given the order of traversal, check if any nodes in the current batch
            # are children of this node. Given the sortedness we know they can't be
            # its parents.
            found_new_batch = any(
                sibling.naked_node.name in child_nodes for sibling in current_batch
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
    def from_dag_description(
        cls,
        dag_description: DAGDescription,
        custom_op_node_map: dict[str, type[AsyncNode]],
    ) -> Union["AsyncFunctionDAG", InvalidDAG]:
        prevalidated_dag = PrevalidatedDAG.from_dag_description(dag_description)
        if isinstance(prevalidated_dag, InvalidDAG):
            return prevalidated_dag
        return cls.from_prevalidated_dag(prevalidated_dag, custom_op_node_map)

    @classmethod
    def nullable_from_dag_description(
        cls,
        dag_description: DAGDescription,
        custom_op_node_map: dict[str, type[AsyncNode]],
    ) -> Optional["AsyncFunctionDAG"]:
        dag = cls.from_dag_description(dag_description, custom_op_node_map)
        if isinstance(dag, InvalidDAG):
            return None
        return dag

    @classmethod
    def throwable_from_dag_description(
        cls,
        dag_description: DAGDescription,
        custom_op_node_map: dict[str, type[AsyncNode]],
    ) -> "AsyncFunctionDAG":
        dag = cls.from_dag_description(dag_description, custom_op_node_map)
        if isinstance(dag, InvalidDAG):
            raise Exception(dag.message)
        return dag

    @classmethod
    def from_string(
        cls,
        dag_description: str,
        custom_op_node_map: dict[str, type[AsyncNode]],
    ) -> Union["AsyncFunctionDAG", InvalidDAG]:
        prevalidated_dag = PrevalidatedDAG.from_string(dag_description)
        if isinstance(prevalidated_dag, EmptyDAG):
            return InvalidDAG(message=prevalidated_dag.message)
        return cls.from_prevalidated_dag(prevalidated_dag, custom_op_node_map)

    @classmethod
    def nullable_from_string(
        cls,
        dag_description: str,
        custom_op_node_map: dict[str, type[AsyncNode]],
    ) -> Optional["AsyncFunctionDAG"]:
        dag = cls.from_string(dag_description, custom_op_node_map)
        if isinstance(dag, InvalidDAG):
            return None
        return dag

    @classmethod
    def throwable_from_string(
        cls,
        dag_description: str,
        custom_op_node_map: dict[str, type[AsyncNode]],
    ) -> "AsyncFunctionDAG":
        dag = cls.from_string(dag_description, custom_op_node_map)
        if isinstance(dag, InvalidDAG):
            raise Exception(dag.message)
        return dag

    # TODO: Consider implementing threadpool policy along with tests/verification.
    # The current policy of batching nodes into a single task is not
    # optimal, but is provably correct and serves as a baseline.
    # This would likely include changing `from_prevalidated_dag` as well.
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

    # TODO: Consider adding a `reorder` method returning a new DAG with
    # optimal batching.

    def _pretty_log_node(
        self,
        node: AsyncDAGNode,
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
