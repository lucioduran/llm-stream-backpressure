# Transport and adapter contract

Each request owns an adapter, a producer task, a bounded FIFO queue, a writer task,
and a disconnect reader. HTTP/1.0 closes the connection after SSE completion.
The implementation binds only to loopback and accepts exactly GET /events HTTP/1.0.
It intentionally omits HTTP keepalive, TLS, chunking, authentication, and proxy deployment.

`Provider.next()` is sequential and returns an Event or None. It must yield to the
scheduler. When `supports_cancel` is true, cancellation of next() must interrupt
pending work, and `cancel()` must acknowledge cleanup of the upstream operation.
When false, the server continues pulling and discarding after disconnect until the
finite adapter budget is exhausted. Such adapters need explicit total-work limits.
This experiment does not imply that closing a socket stops a real provider's compute.

A disconnect cancels the sender first. A cancellable producer is stopped and its
adapter cancellation hook is awaited. A noncancellable producer is drained into a
bounded discard consumer. Normal completion and errors use terminal SSE events.
Provider exception details are not sent to clients. Error events are terminal for
this adapter; there is no retry or automatic stream resumption.

The queue holds at most capacity encoded frames, each at most max_event_bytes.
There are additional frames held by producer and sender, serialization temporaries,
asyncio write buffers, OS socket buffers, client buffers and caller instrumentation.
The write buffer can overshoot its high watermark by one bounded frame. This is not
a whole-process memory guarantee. The reference fixture client retains its finite
received events; production consumers should process events incrementally.
An adapter must itself bound pre-encoding event size. The encoder cannot prevent a
malicious adapter from allocating a huge input object before validation.

`drain()` applies transport backpressure only after socket buffers fill. A consumer
sleeping between reads does not immediately stop upstream work. Experiments use
large frames to expose this boundary. EOF or write failure detects disconnection;
a timeout also ends writes to a stalled peer. Server context exit waits for active
requests, including finite residual work, and is not an immediate shutdown API.

References: Python's official [stream documentation](https://docs.python.org/3/library/asyncio-stream.html)
and [queue documentation](https://docs.python.org/3/library/asyncio-queue.html).
