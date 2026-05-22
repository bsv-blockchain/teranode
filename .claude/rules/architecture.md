---
description: High-level architecture context for Teranode
globs:
---

## Architecture Reference

For detailed architecture, see these docs (read them when you need context, don't memorize):

- [`docs/topics/teranodeIntro.md`](docs/topics/teranodeIntro.md) — why Teranode exists
- [`docs/topics/architecture/teranode-microservices-overview.md`](docs/topics/architecture/teranode-microservices-overview.md) — full microservices breakdown
- [`docs/topics/architecture/teranode-overall-system-design.md`](docs/topics/architecture/teranode-overall-system-design.md) — system design

## Quick Orientation

- **Core services**: Propagation → Validator → Block Assembly → Block Validation → Block Persister → Blockchain (state/FSM)
- **Communication**: gRPC (sync), Kafka (async), HTTP/WebSocket (external), UDP multicast (high-perf tx propagation)
- **Key patterns**: Horizontal scaling, event-driven, UTXO model, Merkle trees, two-phase commit
- **Port config**: `settings.conf` lines 88-140
