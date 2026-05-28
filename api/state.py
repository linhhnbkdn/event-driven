from __future__ import annotations
import asyncio

response_queues: dict[str, asyncio.Queue] = {}
