from __future__ import annotations
import asyncio

from consumer.persistence_runner import run

if __name__ == "__main__":
    asyncio.run(run())
