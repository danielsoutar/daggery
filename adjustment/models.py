from typing import Callable, Optional

from pydantic import BaseModel, field_validator


class Node(BaseModel):
    child: Optional["Node"] = None

    @field_validator("child")
    def validate_child(cls, v):
        if v is not None and v.child is not None:
            raise ValueError("Child node cannot have its own child set.")
        return v


class Foo(Node):
    transform: Callable[[int], int] = lambda x: x * x


class Bar(Node):
    transform: Callable[[int], int] = lambda x: x + 10


class Baz(Node):
    transform: Callable[[int], int] = lambda x: x - 5


class Qux(Node):
    transform: Callable[[int], int] = lambda x: x * 2


class Quux(Node):
    transform: Callable[[int], int] = lambda x: x // 2
