import asyncio
import os
from typing import AsyncIterable

import httpx
import pytest

from spectacles.client import LookerClient


@pytest.fixture
async def looker_client(
    event_loop: asyncio.AbstractEventLoop,
) -> AsyncIterable[LookerClient]:
    async with httpx.AsyncClient(trust_env=False) as async_client:
        client = LookerClient(
            async_client=async_client,
            base_url="https://spectacles.looker.com",
            client_id=os.environ.get("LOOKER_CLIENT_ID", ""),
            client_secret=os.environ.get("LOOKER_CLIENT_SECRET", ""),
        )
        await client.update_workspace("production")
        yield client
