from abc import ABC, abstractmethod
from typing import Tuple

from pydantic import BaseModel


class Node(BaseModel, ABC, frozen=True):
    name: str
    children: Tuple[str, ...] = ()

    @abstractmethod
    def transform(self, *args):
        """Abstract method that should never be called."""


# The below example illustrates an important point:
# Nodes are *immutable*, and this is checked!

# class ExampleNode(Node, frozen=True):
#     def transform(self, value: Any) -> Any:
#         return value
