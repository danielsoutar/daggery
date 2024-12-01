from typing import Callable, Optional

from pydantic import BaseModel, field_validator


class Node(BaseModel):
    child: Optional["Node"] = None

    def transform(self, value: int) -> int:
        return value  # Default pass-through transformation

    # This validator ensures that a Node cannot accidentally produce a cycle
    # in its containing graph.
    @field_validator("child")
    def validate_child(cls, v):
        if v is not None and v.child is not None:
            raise ValueError("Child node cannot have its own child set.")
        return v


class Foo(Node):
    def transform(self, value: int) -> int:
        return value * value


class Bar(Node):
    def transform(self, value: int) -> int:
        return value + 10


class Baz(Node):
    def transform(self, value: int) -> int:
        return value - 5


class Qux(Node):
    def transform(self, value: int) -> int:
        return value * 2


class Quux(Node):
    def transform(self, value: int) -> int:
        return value // 2
