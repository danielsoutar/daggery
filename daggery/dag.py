from typing import Any, Optional, Tuple, Union

from pydantic import BaseModel

from .description import DAGDescription
from .graph import EmptyDAG, InvalidGraph, PrevalidatedDAG
from .node import Node
from .utils.logging import logger_factory

logger = logger_factory(__name__)


class DAGNode(BaseModel, frozen=True):
    naked_node: Node
    input_nodes: Tuple[str, ...]

    def transform(self, *args) -> Any:
        return self.naked_node.transform(*args)


class FunctionDAG(BaseModel, frozen=True):
    nodes: Tuple[DAGNode, ...]

    @property
    def is_sequence(self) -> bool:
        return all(len(node.input_nodes) <= 1 for node in self.nodes)

    # We separate the creation of the DAG from the init method since this allows
    # returning instances of InvalidGraph, making this code exception-free.
    @classmethod
    def from_prevalidated_dag(
        cls,
        prevalidated_dag: PrevalidatedDAG,
        custom_op_node_map: dict[str, type[Node]],
    ) -> Union["FunctionDAG", InvalidGraph]:
        node_names = [node.node_name for node in prevalidated_dag.nodes]
        for name in node_names:
            if name not in custom_op_node_map.keys():
                return InvalidGraph(
                    message=f"Invalid internal node_name found in prevalidated DAG: {name}"
                )

        graph_nodes: dict[str, Node] = {}
        ordered_nodes: list[DAGNode] = []

        # Creating immutable nodes back-to-front guarantees an immutable DAG.
        for prevalidated_node in reversed(prevalidated_dag.nodes):
            name = prevalidated_node.name
            child_nodes = [graph_nodes[child] for child in prevalidated_node.children]

            node_class = custom_op_node_map[prevalidated_node.node_name]
            node = node_class(name=name, children=tuple(child_nodes))
            if not node.model_config.get("frozen", False):
                return InvalidGraph(
                    message=f"Mutable node found in DAG ({node}). This is not supported."
                )

            graph_nodes[name] = node
            # We have a special case for the root node, enabling a standard
            # fetching of inputs in the transform.
            input_nodes = tuple(prevalidated_node.input_nodes) or ("__INPUT__",)
            ordered_nodes.append(
                DAGNode(
                    naked_node=node,
                    input_nodes=input_nodes,
                )
            )

        return cls(nodes=tuple(reversed(ordered_nodes)))

    @classmethod
    def from_dag_description(
        cls,
        dag_description: DAGDescription,
        custom_op_node_map: dict[str, type[Node]],
    ) -> Union["FunctionDAG", InvalidGraph]:
        prevalidated_dag = PrevalidatedDAG.from_dag_description(dag_description)
        if isinstance(prevalidated_dag, InvalidGraph):
            return prevalidated_dag
        return cls.from_prevalidated_dag(
            prevalidated_dag,
            custom_op_node_map,
        )

    @classmethod
    def nullable_from_dag_description(
        cls,
        dag_description: DAGDescription,
        custom_op_node_map: dict[str, type[Node]],
    ) -> Optional["FunctionDAG"]:
        dag = cls.from_dag_description(dag_description, custom_op_node_map)
        if isinstance(dag, InvalidGraph):
            return None
        return dag

    @classmethod
    def throwable_from_dag_description(
        cls,
        dag_description: DAGDescription,
        custom_op_node_map: dict[str, type[Node]],
    ) -> "FunctionDAG":
        dag = cls.from_dag_description(dag_description, custom_op_node_map)
        if isinstance(dag, InvalidGraph):
            raise ValueError(dag.message)
        return dag

    @classmethod
    def from_string(
        cls, dag_description: str, custom_op_node_map: dict[str, type[Node]]
    ) -> Union["FunctionDAG", InvalidGraph]:
        prevalidated_dag = PrevalidatedDAG.from_string(dag_description)
        if isinstance(prevalidated_dag, EmptyDAG):
            return InvalidGraph(message=prevalidated_dag.message)
        return cls.from_prevalidated_dag(prevalidated_dag, custom_op_node_map)

    @classmethod
    def nullable_from_string(
        cls, dag_description: str, custom_op_node_map: dict[str, type[Node]]
    ) -> Optional["FunctionDAG"]:
        dag = cls.from_string(dag_description, custom_op_node_map)
        if isinstance(dag, InvalidGraph):
            return None
        return dag

    @classmethod
    def throwable_from_string(
        cls, dag_description: str, custom_op_node_map: dict[str, type[Node]]
    ) -> "FunctionDAG":
        dag = cls.from_string(dag_description, custom_op_node_map)
        if isinstance(dag, InvalidGraph):
            raise ValueError(dag.message)
        return dag

    def transform(self, value: Any) -> Any:
        context = {"__INPUT__": value}
        # The nodes are topologically sorted. As it turns out, this is also
        # a valid order of evaluation - by the time a node is reached, all
        # of its parents will already have been evaluated.
        for node in self.nodes:
            input_values = tuple(context[node_name] for node_name in node.input_nodes)
            node_output_value = node.transform(*input_values)
            self._pretty_log_node(node, input_values, node_output_value)
            context[node.naked_node.name] = node_output_value
        return node_output_value

    def _pretty_log_node(
        self,
        node: DAGNode,
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
