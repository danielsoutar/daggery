import asyncio
from abc import ABC, abstractmethod
from typing import Any, Tuple

from pydantic import BaseModel, ConfigDict


class AsyncNode(BaseModel, ABC):
    name: str
    children: Tuple["AsyncNode", ...] = ()

    @abstractmethod
    async def transform(self, *args):
        pass  # Abstract method


class AsyncExampleNode(AsyncNode):
    model_config = ConfigDict(extra="forbid", frozen=True)

    async def transform(self, value: Any) -> Any:
        await asyncio.sleep(1)
        return value
