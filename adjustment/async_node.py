import asyncio
from abc import ABC, abstractmethod
from typing import Tuple

from pydantic import BaseModel, ConfigDict


class AsyncNode(BaseModel, ABC):
    name: str
    children: Tuple["AsyncNode", ...] = ()

    @abstractmethod
    async def transform(self, *args):
        pass  # Abstract method


class AsyncFoo(AsyncNode):
    model_config = ConfigDict(extra="forbid", frozen=True)

    async def transform(self, value: int) -> int:
        await asyncio.sleep(1)
        return value * value


class AsyncPing(AsyncNode):
    model_config = ConfigDict(extra="forbid", frozen=True)

    async def transform(self, count: int) -> int:
        proc = await asyncio.create_subprocess_exec(
            "ping",
            "-c",
            str(count),
            "8.8.8.8",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        return await proc.wait()
