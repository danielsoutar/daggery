from abc import ABC, abstractmethod
from typing import Optional

from pydantic import BaseModel, ConfigDict


class Node(BaseModel, ABC):
    child: Optional["Node"] = None

    @abstractmethod
    def transform(self, value):
        pass  # Abstract method


class Foo(Node):
    model_config = ConfigDict(extra="forbid", frozen=True)

    def transform(self, value: int) -> int:
        return value * value


class Bar(Node):
    model_config = ConfigDict(extra="forbid", frozen=True)

    def transform(self, value: int) -> int:
        return value + 10


class Baz(Node):
    model_config = ConfigDict(extra="forbid", frozen=True)

    def transform(self, value: int) -> int:
        return value - 5


class Qux(Node):
    model_config = ConfigDict(extra="forbid", frozen=True)

    def transform(self, value: int) -> int:
        return value * 2


class Quux(Node):
    model_config = ConfigDict(extra="forbid", frozen=True)

    def transform(self, value: int) -> int:
        return value // 2
