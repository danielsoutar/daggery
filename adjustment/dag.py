import logging
from typing import Dict

import colorlog
from pydantic import BaseModel

from .node import Bar, Baz, Foo, Node, Quux, Qux


def logger_factory() -> logging.Logger:
    # Create a logger instance
    logger = logging.getLogger(__name__)

    logger.setLevel(logging.INFO)

    # Create a console handler and set the level to INFO
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    # Create a color formatter and set it for the handler
    formatter = colorlog.ColoredFormatter(
        fmt="%(log_color)s%(levelname)s%(reset)s: %(asctime)s [%(name)s]  %(message)s",
        datefmt=None,
        reset=True,
        log_colors={
            "DEBUG": "cyan",
            "INFO": "green",
            "WARNING": "yellow",
            "ERROR": "red",
            "CRITICAL": "red,bg_white",
        },
    )
    console_handler.setFormatter(formatter)

    # Add the handler to the logger
    logger.addHandler(console_handler)

    return logger


logger = logger_factory()


class DAG(BaseModel):
    head: Node

    def transform(self, value: int) -> int:
        current_node: Node | None = self.head
        while current_node is not None:
            # Log the node name and intermediate result.
            logger.info(
                f"Node: {current_node.__class__.__name__}, "
                f"Intermediate Result: {value}"
            )
            value = current_node.transform(value)
            current_node = current_node.child
        return value


node_map: Dict[str, type[Node]] = {
    "foo": Foo,
    "bar": Bar,
    "baz": Baz,
    "qux": Qux,
    "quux": Quux,
}


def from_string(dag_string: str) -> DAG:
    if dag_string == "":
        raise ValueError("DAG string cannot be empty")
    node_names = list(map(str.strip, dag_string.split(">>")))
    # Validate node names
    if any(node_name not in node_map.keys() for node_name in node_names):
        raise ValueError(f"Invalid node name encountered in: {dag_string}")
    # Assert there is at least one node.
    if len(node_names) == 0:
        raise ValueError("DAG string must name at least one node")

    # Create a DAG object using back-to-front construction
    current = node_map[node_names[-1]](child=None)
    for node_name in reversed(node_names[:-1]):
        node = node_map[node_name](child=current)
        current = node

    return DAG(head=current)
