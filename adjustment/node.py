from abc import ABC, abstractmethod
from typing import Any, Tuple

from pydantic import BaseModel, ConfigDict


class Node(BaseModel, ABC):
    name: str
    children: Tuple["Node", ...] = ()

    @abstractmethod
    def transform(self, *args):
        pass  # Abstract method


class ExampleNode(Node):
    model_config = ConfigDict(extra="forbid", frozen=True)

    def transform(self, value: Any) -> Any:
        return value
