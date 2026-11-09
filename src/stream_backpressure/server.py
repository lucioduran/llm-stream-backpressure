from __future__ import annotations

import asyncio
import json
import socket
import time
from collections.abc import Callable
from typing import Any

from .core import Provider, Session, Settings, Trace


class Server:
    """Minimal loopback HTTP/1.0 SSE experiment server, not a general HTTP server."""

    def __init__(
        self,
        factory: Callable[[], Provider],
        settings: Settings = Settings(),
        on_session: Callable[[Session], None] | None = None,
        trace: Trace | None = None,
    ) -> None:
        self.factory = factory
        self.settings = settings
        self.on_session = on_session
        self.trace = trace
        self.tasks: set[asyncio.Task[None]] = set()
        self.listener: asyncio.Server | None = None
        self.port = 0

    async def __aenter__(self) -> Server:
        self.listener = await asyncio.start_server(self.accept, "127.0.0.1", 0, limit=8192)
        self.port = self.listener.sockets[0].getsockname()[1]
        return self

    async def __aexit__(self, *args: object) -> None:
        assert self.listener is not None
        self.listener.close()
        await self.listener.wait_closed()
        if self.tasks:
            await asyncio.gather(*self.tasks)

    async def accept(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        task = asyncio.current_task()
        assert task is not None
        self.tasks.add(task)
        try:
            header = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), 2)
            if header.split(b"\r\n", 1)[0] != b"GET /events HTTP/1.0":
                writer.write(b"HTTP/1.0 400 Bad Request\r\n\r\n")
                await writer.drain()
                return
            writer.get_extra_info("socket").setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 8192)
            writer.write(b"HTTP/1.0 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\n\r\n")
            session = Session(self.factory(), self.settings, self.trace)
            if self.on_session:
                self.on_session(session)
            await session.run(reader, writer)
        except (ConnectionError, TimeoutError, asyncio.IncompleteReadError, asyncio.LimitOverrunError):
            pass
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except (ConnectionError, OSError):
                pass
            self.tasks.discard(task)


async def consume(
    port: int,
    delay: float = 0,
    stop_after: int | None = None,
    trace: Trace | None = None,
    abort: bool = False,
) -> list[dict[str, Any]]:
    """Collect a finite fixture stream. Production clients should process incrementally."""
    reader, writer = await asyncio.open_connection("127.0.0.1", port, limit=131072)
    events: list[dict[str, Any]] = []
    try:
        writer.write(b"GET /events HTTP/1.0\r\nHost: localhost\r\n\r\n")
        await writer.drain()
        header = await reader.readuntil(b"\r\n\r\n")
        if not header.startswith(b"HTTP/1.0 200"):
            raise ValueError("Server rejected request")
        if stop_after == 0:
            return events
        while True:
            try:
                frame = await reader.readuntil(b"\n\n")
            except asyncio.IncompleteReadError as exc:
                raise ConnectionError("Stream ended without done event") from exc
            kind, data, _ = frame.decode().split("\n", 2)
            event = {"kind": kind.removeprefix("event: "), **json.loads(data.removeprefix("data: "))}
            events.append(event)
            if trace:
                trace({"action": "received", "time_ns": time.monotonic_ns(), "sequence": event["sequence"], "kind": event["kind"]})
            if event["kind"] == "done" or (stop_after is not None and len(events) >= stop_after):
                return events
            await asyncio.sleep(delay)
    finally:
        if trace:
            trace({"action": "client_close", "time_ns": time.monotonic_ns()})
        if abort:
            writer.transport.abort()
        else:
            writer.close()
        try:
            await writer.wait_closed()
        except (ConnectionError, OSError):
            pass
