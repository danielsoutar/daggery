from abc import ABC, abstractmethod
from typing import Tuple

from pydantic import BaseModel


class Node(BaseModel, ABC):
    name: str
    children: Tuple["Node", ...] = ()

    @abstractmethod
    def transform(self, *args):
        pass  # Abstract method


# The below example illustrates an important point:
# Nodes are *immutable*, and this is checked!

# class ExampleNode(Node):
#     model_config = ConfigDict(extra="forbid", frozen=True)

#     def transform(self, value: Any) -> Any:
#         return value
