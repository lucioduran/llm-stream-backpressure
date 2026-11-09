from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True)
class Event:
    kind: str
    sequence: int
    text: str

    def encode(self) -> bytes:
        if self.kind not in {"token", "tool", "error", "done"}:
            raise ValueError("Unsupported event kind")
        data = json.dumps({"sequence": self.sequence, "text": self.text})
        return f"event: {self.kind}\ndata: {data}\n\n".encode()


class Provider(Protocol):
    """One provider instance per request; next() must be sequential.

    Cancellable adapters must tolerate task cancellation during next(), and
    cancel() must finish upstream cleanup. Noncancellable adapters must have a
    finite completion budget and remain pullable until exhaustion.
    """

    supports_cancel: bool

    async def next(self) -> Event | None: ...

    async def cancel(self) -> None: ...


class SyntheticProvider:
    """Deterministic local work, not model inference or a provider integration."""

    def __init__(
        self,
        count: int = 100,
        delay: float = 0.001,
        payload: int = 32,
        supports_cancel: bool = True,
        fail_at: int | None = None,
    ) -> None:
        if count < 0 or delay < 0 or payload < 0:
            raise ValueError("Work parameters must be nonnegative")
        self.count = count
        self.delay = delay
        self.payload = payload
        self.supports_cancel = supports_cancel
        self.fail_at = fail_at
        self.produced = 0
        self.cancelled = False

    async def next(self) -> Event | None:
        if self.cancelled or self.produced == self.count:
            return None
        await asyncio.sleep(self.delay)
        if self.produced == self.fail_at:
            raise RuntimeError("Synthetic upstream failure")
        index = self.produced
        self.produced += 1
        return Event("tool" if index % 7 == 3 else "token", index, "x" * self.payload)

    async def cancel(self) -> None:
        if not self.supports_cancel:
            raise RuntimeError("Cancellation is unsupported")
        self.cancelled = True


@dataclass(frozen=True)
class Settings:
    capacity: int = 4
    max_event_bytes: int = 65536
    write_high: int = 32768
    write_timeout: float = 5.0

    def __post_init__(self) -> None:
        if min(self.capacity, self.max_event_bytes, self.write_high) <= 0:
            raise ValueError("Buffer bounds must be positive")
        if self.max_event_bytes < 128:
            raise ValueError("Event bound must accommodate terminal frames")
        if self.write_timeout <= 0:
            raise ValueError("Write timeout must be positive")


Trace = Callable[[dict[str, object]], None]


class Session:
    def __init__(
        self, provider: Provider, settings: Settings, trace: Trace | None = None
    ) -> None:
        self.provider = provider
        self.settings = settings
        self.trace = trace
        self.queue: asyncio.Queue[bytes] = asyncio.Queue(settings.capacity)
        self.produced = 0
        self.sent = 0
        self.high_water = 0
        self.write_buffer_peak = 0
        self.max_drain_seconds = 0.0
        self.disconnect_produced: int | None = None
        self.finished = asyncio.Event()
        self.failure: str | None = None

    def record(self, action: str, **fields: object) -> None:
        if self.trace:
            self.trace({"action": action, "time_ns": time.monotonic_ns(), **fields})

    async def enqueue(self, event: Event) -> None:
        frame = event.encode()
        if len(frame) > self.settings.max_event_bytes:
            raise ValueError("Encoded event exceeds max_event_bytes")
        start = time.monotonic_ns()
        await self.queue.put(frame)
        self.high_water = max(self.high_water, self.queue.qsize())
        self.record(
            "enqueued", queue=self.queue.qsize(), wait_ns=time.monotonic_ns() - start
        )

    async def pump(self) -> None:
        try:
            while (event := await self.provider.next()) is not None:
                self.produced += 1
                self.record("produced", sequence=event.sequence, kind=event.kind)
                await self.enqueue(event)
        except Exception as exc:
            self.failure = type(exc).__name__
            self.record("upstream_error", error=self.failure)
            # Do not expose provider exception text over the transport.
            await self.enqueue(Event("error", self.produced, "Upstream failed"))
        await self.enqueue(Event("done", self.produced, ""))

    async def send(self, writer: asyncio.StreamWriter) -> None:
        while True:
            frame = await self.queue.get()
            writer.write(frame)
            self.write_buffer_peak = max(
                self.write_buffer_peak, writer.transport.get_write_buffer_size()
            )
            start = time.monotonic()
            await asyncio.wait_for(writer.drain(), self.settings.write_timeout)
            elapsed = time.monotonic() - start
            self.max_drain_seconds = max(self.max_drain_seconds, elapsed)
            self.sent += 1
            self.record("sent", bytes=len(frame), drain_seconds=elapsed)
            if frame.startswith(b"event: done\n"):
                return

    async def discard(self) -> None:
        while True:
            frame = await self.queue.get()
            self.record("discarded", bytes=len(frame))
            if frame.startswith(b"event: done\n"):
                return

    async def run(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        writer.transport.set_write_buffer_limits(high=self.settings.write_high)
        pump = asyncio.create_task(self.pump())
        send = asyncio.create_task(self.send(writer))
        gone = asyncio.create_task(reader.read(1))
        tasks = [pump, send, gone]
        try:
            completed, _ = await asyncio.wait(
                [send, gone], return_when=asyncio.FIRST_COMPLETED
            )
            disconnected = gone in completed or (
                send.done() and send.exception() is not None
            )
            if disconnected:
                self.disconnect_produced = self.produced
                self.record("disconnect", produced=self.produced)
                writer.transport.abort()
                send.cancel()
                if self.provider.supports_cancel:
                    pump.cancel()
                    await asyncio.gather(send, pump, return_exceptions=True)
                    await self.provider.cancel()
                    self.record("cancelled", produced=self.produced)
                else:
                    await asyncio.gather(send, return_exceptions=True)
                    discard = asyncio.create_task(self.discard())
                    tasks.append(discard)
                    await asyncio.gather(pump, discard)
            else:
                await send
                await pump
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            writer.close()
            try:
                await writer.wait_closed()
            except (ConnectionError, OSError):
                pass
            self.record("finished", produced=self.produced, sent=self.sent)
            self.finished.set()
