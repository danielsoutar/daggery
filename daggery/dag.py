from typing import Any, List, Optional, Tuple, Union

from pydantic import BaseModel, ConfigDict

from .graph import EmptyDAG, InvalidGraph, PrevalidatedDAG
from .node import Node
from .request import ArgumentMappingMetadata, OperationList
from .utils.logging import logger_factory

logger = logger_factory(__name__)


class DAGNode(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    naked_node: Node
    input_nodes: Tuple[str, ...]

    def transform(self, *args) -> Any:
        return self.naked_node.transform(*args)


class FunctionDAG(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    nodes: Tuple[DAGNode, ...]

    @property
    def is_sequence(self) -> bool:
        return all(len(node.input_nodes) <= 1 for node in self.nodes)

    # We separate the creation of the DAG from the init method since this allows
    # returning instances of InvalidGraph, making this code exception-free.
    @classmethod
    def from_prevalidated_dag(
        cls, prevalidated_dag: PrevalidatedDAG, custom_node_map: dict[str, type[Node]]
    ) -> Union["FunctionDAG", InvalidGraph]:
        node_rules = [node.rule for node in prevalidated_dag.nodes]
        for rule in node_rules:
            if rule not in custom_node_map.keys():
                return InvalidGraph(
                    message=f"Invalid rule found in unvalidated DAG: {rule}"
                )

        graph_nodes: dict[str, Node] = {}
        ordered_nodes: list[DAGNode] = []

        # Creating immutable nodes back-to-front guarantees an immutable DAG.
        for prevalidated_node in reversed(prevalidated_dag.nodes):
            name = prevalidated_node.name
            child_nodes = [graph_nodes[child] for child in prevalidated_node.children]

            node_class = custom_node_map[prevalidated_node.rule]
            node = node_class(name=name, children=tuple(child_nodes))

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
    def from_node_list(
        cls,
        graph_description: OperationList,
        argument_mappings: List[ArgumentMappingMetadata],
        custom_node_map: dict[str, type[Node]],
    ) -> Union["FunctionDAG", InvalidGraph]:
        prevalidated_dag = PrevalidatedDAG.from_node_list(
            graph_description,
            argument_mappings,
        )
        if isinstance(prevalidated_dag, InvalidGraph):
            return prevalidated_dag
        return cls.from_prevalidated_dag(
            prevalidated_dag,
            custom_node_map,
        )

    @classmethod
    def nullable_from_node_list(
        cls,
        graph_description: OperationList,
        argument_mappings: List[ArgumentMappingMetadata],
        custom_node_map: dict[str, type[Node]],
    ) -> Optional["FunctionDAG"]:
        dag = cls.from_node_list(
            graph_description,
            argument_mappings,
            custom_node_map,
        )
        if isinstance(dag, InvalidGraph):
            return None
        return dag

    @classmethod
    def throwable_from_node_list(
        cls,
        graph_description: OperationList,
        argument_mappings: List[ArgumentMappingMetadata],
        custom_node_map: dict[str, type[Node]],
    ) -> "FunctionDAG":
        dag = cls.from_node_list(
            graph_description,
            argument_mappings,
            custom_node_map,
        )
        if isinstance(dag, InvalidGraph):
            raise ValueError(dag.message)
        return dag

    @classmethod
    def from_string(
        cls, graph_description: str, custom_node_map: dict[str, type[Node]]
    ) -> Union["FunctionDAG", InvalidGraph]:
        prevalidated_dag = PrevalidatedDAG.from_string(graph_description)
        if isinstance(prevalidated_dag, EmptyDAG):
            return InvalidGraph(message=prevalidated_dag.message)
        return cls.from_prevalidated_dag(prevalidated_dag, custom_node_map)

    @classmethod
    def nullable_from_string(
        cls, graph_description: str, custom_node_map: dict[str, type[Node]]
    ) -> Optional["FunctionDAG"]:
        dag = cls.from_string(graph_description, custom_node_map)
        if isinstance(dag, InvalidGraph):
            return None
        return dag

    @classmethod
    def throwable_from_string(
        cls, graph_description: str, custom_node_map: dict[str, type[Node]]
    ) -> "FunctionDAG":
        dag = cls.from_string(graph_description, custom_node_map)
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
