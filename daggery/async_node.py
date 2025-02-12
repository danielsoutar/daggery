from abc import ABC, abstractmethod
from typing import Tuple

from pydantic import BaseModel


class AsyncNode(BaseModel, ABC, frozen=True):
    name: str
    children: Tuple[str, ...] = ()

    @abstractmethod
    async def transform(self, *args):
        """Abstract method that should never be called."""


# The below example illustrates an important point:
# Nodes are *immutable*, and this is checked!

# class AsyncExampleNode(AsyncNode, frozen=True):
#     async def transform(self, value: Any) -> Any:
#         await asyncio.sleep(1)
#         return value
