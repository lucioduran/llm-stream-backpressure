# Contributing

Use Python 3.12.11 and the pinned development dependencies. Run the README checks
before submitting changes. Keep commits focused, documentation in English, and
provider capabilities explicit. Do not include credentials or private prompt data.

New measurements should use a new output directory. Freeze scenarios and thresholds
before measurement, serialize heavy workloads, record hardware and source versions,
and retain failures. Never present deterministic producer timing as model inference.
Keep code comments sparse and place design explanations in docs/design.md.
