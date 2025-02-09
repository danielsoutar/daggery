from abc import ABC, abstractmethod
from typing import Tuple

from pydantic import BaseModel


class AsyncNode(BaseModel, ABC, frozen=True):
    name: str
    children: Tuple["AsyncNode", ...] = ()

    @abstractmethod
    async def transform(self, *args):
        pass  # Abstract method


# The below example illustrates an important point:
# Nodes are *immutable*, and this is checked!

# class AsyncExampleNode(AsyncNode, frozen=True):
#     async def transform(self, value: Any) -> Any:
#         await asyncio.sleep(1)
#         return value
