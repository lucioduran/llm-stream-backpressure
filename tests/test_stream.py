import asyncio
import unittest

from stream_backpressure import Event, Server, Settings, SyntheticProvider, consume


class ValidationTests(unittest.TestCase):
    def test_invalid_settings(self):
        for kwargs in ({"capacity": 0}, {"max_event_bytes": 1}, {"write_high": 0}, {"write_timeout": 0}):
            with self.assertRaises(ValueError):
                Settings(**kwargs)

    def test_invalid_provider(self):
        for kwargs in ({"count": -1}, {"delay": -1}, {"payload": -1}):
            with self.assertRaises(ValueError):
                SyntheticProvider(**kwargs)

    def test_event_encoding(self):
        encoded = Event("token", 0, "line\nline").encode()
        self.assertEqual(encoded.count(b"\n"), 3)
        with self.assertRaises(ValueError):
            Event("injected\nevent", 0, "").encode()


class TransportTests(unittest.IsolatedAsyncioTestCase):
    async def scenario(self, provider, **client):
        sessions = []
        async with Server(lambda: provider, on_session=sessions.append) as server:
            events = await asyncio.wait_for(consume(server.port, **client), 4)
        self.assertTrue(sessions[0].finished.is_set())
        self.assertFalse(server.tasks)
        return events, sessions[0]

    async def test_order_and_interleaved_tools(self):
        events, session = await self.scenario(SyntheticProvider(count=20), delay=0.001)
        self.assertEqual([e["sequence"] for e in events[:-1]], list(range(20)))
        self.assertEqual(events[3]["kind"], "tool")
        self.assertEqual(events[-1]["kind"], "done")
        self.assertLessEqual(session.high_water, 4)

    async def test_cancellation(self):
        provider = SyntheticProvider(count=60, delay=0.002)
        _, session = await self.scenario(provider, stop_after=3)
        self.assertTrue(provider.cancelled)
        self.assertLess(provider.produced, 60)
        self.assertEqual(session.produced, session.disconnect_produced)

    async def test_unsupported_cancellation(self):
        provider = SyntheticProvider(count=20, delay=0.001, supports_cancel=False)
        _, session = await self.scenario(provider, stop_after=2)
        self.assertFalse(provider.cancelled)
        self.assertEqual(provider.produced, 20)
        self.assertGreater(session.produced, session.disconnect_produced)
        with self.assertRaises(RuntimeError):
            await provider.cancel()

    async def test_failure_is_terminal_and_sanitized(self):
        events, session = await self.scenario(SyntheticProvider(fail_at=3))
        self.assertEqual([e["kind"] for e in events[-2:]], ["error", "done"])
        self.assertEqual(session.failure, "RuntimeError")
        self.assertNotIn("Synthetic", str(events))

    async def test_oversize_event(self):
        sessions = []
        async with Server(lambda: SyntheticProvider(payload=400), Settings(max_event_bytes=128), sessions.append) as server:
            events = await consume(server.port)
        self.assertEqual(events[0]["kind"], "error")
        self.assertEqual(sessions[0].failure, "ValueError")

    async def test_empty(self):
        events, _ = await self.scenario(SyntheticProvider(count=0))
        self.assertEqual([e["kind"] for e in events], ["done"])

    async def test_disconnect_races(self):
        for stop in (0, 1, 4):
            for abort in (False, True):
                await self.scenario(SyntheticProvider(count=4), stop_after=stop, abort=abort)

    async def test_concurrent_isolation(self):
        sessions = []
        async with Server(lambda: SyntheticProvider(count=12), on_session=sessions.append) as server:
            outputs = await asyncio.gather(consume(server.port), consume(server.port, stop_after=2), consume(server.port))
        self.assertEqual([len(x) for x in outputs], [13, 2, 13])
        self.assertEqual(len(sessions), 3)
        self.assertTrue(all(s.finished.is_set() for s in sessions))

    async def test_stalled_socket_bounds_and_cancels(self):
        sessions = []
        async with Server(lambda: SyntheticProvider(count=200, delay=0, payload=60000), Settings(capacity=2), sessions.append) as server:
            reader, writer = await asyncio.open_connection("127.0.0.1", server.port, limit=1024)
            writer.write(b"GET /events HTTP/1.0\r\n\r\n")
            await writer.drain()
            await reader.readuntil(b"\r\n\r\n")
            await asyncio.sleep(0.05)
            session = sessions[0]
            self.assertEqual(session.high_water, 2)
            self.assertLess(session.produced, 200)
            self.assertLessEqual(session.write_buffer_peak, 32768 + 65536)
            writer.transport.abort()
            await asyncio.wait_for(session.finished.wait(), 2)
            self.assertEqual(session.produced, session.disconnect_produced)

    async def test_write_timeout(self):
        sessions = []
        async with Server(lambda: SyntheticProvider(count=200, delay=0, payload=60000), Settings(write_timeout=0.02), sessions.append) as server:
            reader, writer = await asyncio.open_connection("127.0.0.1", server.port, limit=1024)
            writer.write(b"GET /events HTTP/1.0\r\n\r\n")
            await writer.drain()
            await reader.readuntil(b"\r\n\r\n")
            # Closing a stalled socket can await the peer's receive window.
            await asyncio.sleep(0.1)
            writer.transport.abort()
            await asyncio.wait_for(sessions[0].finished.wait(), 2)
            self.assertIsNotNone(sessions[0].disconnect_produced)

    async def test_invalid_http(self):
        async with Server(SyntheticProvider) as server:
            reader, writer = await asyncio.open_connection("127.0.0.1", server.port)
            writer.write(b"POST / HTTP/1.0\r\n\r\n")
            await writer.drain()
            self.assertIn(b"400", await reader.read())
            writer.close()
            await writer.wait_closed()

    async def test_truncated_response(self):
        async def broken(reader, writer):
            await reader.readuntil(b"\r\n\r\n")
            writer.write(b"HTTP/1.0 200 OK\r\n\r\n")
            writer.close()
            await writer.wait_closed()
        server = await asyncio.start_server(broken, "127.0.0.1", 0)
        async with server:
            with self.assertRaises(ConnectionError):
                await consume(server.sockets[0].getsockname()[1])
