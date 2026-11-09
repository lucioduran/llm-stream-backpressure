# llm-stream-backpressure

## Problem

Slow stream consumers can hide unbounded queues and abandoned upstream work.

## Scope

Local HTTP/SSE experiments with deterministic synthetic producers. No model inference or external provider is claimed.

## Quick start

Requires Python 3.12 or later. Installation and full instructions follow with the evidence release.

## Usage

The package exports `Server`, `Session`, `Settings`, `SyntheticProvider` and `consume`.

## Architecture

A bounded application queue connects an upstream adapter to a drain-aware socket writer.

## Reproduction

The frozen experiment matrix and replay verifier accompany the measured results.

## Results

Measurements pending.

## Testing

Run `python -m unittest discover -s tests -v`.

## Limitations

Loopback transport and synthetic work do not predict inference performance.

## Contributing

Keep tests deterministic and report observed failures.

## License

MIT. All synthetic fixtures and evidence created for this repository are also MIT licensed.
