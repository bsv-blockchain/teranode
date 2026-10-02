# Changelog

Notable changes to Teranode, with an emphasis on anything an operator must act on.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

This file records **breaking changes**: anything that alters the behaviour of an
existing deployment without the operator changing their configuration, or that
requires a configuration or deployment change before upgrading. Changes that only
add capability, fix a bug without altering a documented contract, or remove
configuration that was already inert are not listed as breaking; a few are noted
under "Not breaking, but worth knowing" where they look alarming but are not.

## [Unreleased]

### Breaking — action required before upgrading

- **`grpc_admin_api_key` is now mandatory for the Blockchain service.** The service
  validates it at startup and refuses to start when it is empty, carries leading or
  trailing whitespace, is a well-known placeholder (`testkey`, `changeme`, …), or is
  shorter than the minimum length. Every Blockchain RPC except `HealthGRPC` requires
  the key, and the HTTP `invalidate`/`revalidate` routes require it too.

  Every service that talks to Blockchain must be given the same value. Upgrade
  credential-capable clients first and Blockchain last, or perform a coordinated
  restart — a Blockchain pod that starts before its clients have the key will reject
  them. The key travels in plaintext at `security_level_grpc=0`; use verified TLS
  across untrusted networks. (#1755)

- **Kubernetes: Kafka now sits behind a NetworkPolicy.** `deploy/kubernetes/kafka/kafka-shared-networkpolicy.yaml`
  restricts which pods may reach the shared Kafka broker. Pods that are not matched
  by the policy's selectors lose Kafka connectivity on upgrade. Check your pod labels
  against the policy before rolling it out. (#1617)

### Breaking — behaviour changes on upgrade

- **P2P HTTP endpoints are now rate limited.** `/health` and the websocket upgrade
  path are limited per source address, defaulting to 100 requests per second
  (`p2p_httpRateLimit`); requests over the limit receive HTTP 429. These endpoints
  were previously unlimited. Health checkers or probes polling faster than this will
  start being rejected. Set `p2p_httpRateLimit = 0` to disable. (#1566)

- **Settings that previously did nothing now take effect.** Each of the following
  carried a `key:` struct tag and appeared in the reference documentation, but
  `NewSettings()` never populated it, so the field stayed at its Go zero value no
  matter what was in `settings.conf`. They are now wired. **If you already have any
  of these set, its value takes effect on upgrade — review them before rolling out.**

  | Setting | Previously | Now |
  |---|---|---|
  | `p2p_peer_registry_max_size` | `0` — LRU bound disabled | your configured bound is enforced |
  | `p2p_peer_registry_ttl` | `0` | your configured TTL applies |
  | `p2p_peer_registry_cleanup_interval` | `0` — the cleanup loop never ran | cleanup runs; peers are evicted |
  | `pruner_skipDuringCatchup` | `false` — flag unreachable | configurable |
  | `pruner_skipProcessExpiredPreservations` | `false` — flag unreachable | configurable |
  | `aerospike_enable_preserve_filter_expressions` | permanently `false` | configurable |
  | `txMetaCacheTrimRatio` | read directly from gocore, untyped | read from typed settings |

  The peer-registry entries are the ones most likely to be noticed: with the cleanup
  interval at zero the eviction loop never started, so registries grew without bound.
  After upgrading, peers are evicted on the configured TTL and LRU bound.
  (#1645, #1641, #1644, #1746)

- **`aerospike_enable_preserve_filter_expressions` is mutually exclusive with
  `aerospike_use_native_teranode_ops` for preserve operations.** `PreserveTransactions`
  is native-op-covered (`subOpPreserveUntil`). With native ops active and this flag
  false, the preserve write and its prune-eligibility filter go through the native
  operate path. Setting this flag true short-circuits that dispatch and uses a
  client-side BatchWrite instead, so preserve silently stops using the native path
  while spend and setMined continue to use it. Do not enable both unless that trade
  is intended. (#1644)

### Breaking — pending, not yet merged

- **Unauthenticated Blockchain read RPCs will be bounded.** Count and range
  parameters on the public read RPCs will be rejected with `InvalidArgument` above
  configurable caps: `blockchain_maxBlockHeadersPerRequest`,
  `blockchain_maxBlocksByHeightRange`, `blockchain_maxInvalidBlocksPerRequest` and
  `blockchain_maxMedianTimePastHeights`. Callers that currently request very large
  counts will start receiving errors.

  **Upgrade-order hazard:** the median-time-past cap rejects the whole-chain request
  that every pre-change client sends on first load, and those clients do not retry
  `InvalidArgument`. During a rolling upgrade, an old-image validator,
  subtreevalidation or blockvalidation pod talking to an upgraded Blockchain pod will
  fail every `EnsureMTPLoaded` and validate nothing until it is reimaged. No chain
  state is corrupted. Upgrade Blockchain last, or raise
  `blockchain_maxMedianTimePastHeights` for the duration of the rollout. (#1776)

### Not breaking, but worth knowing

- **Dead configuration keys were removed.** These had no readers anywhere in the
  codebase, so removing them changes no behaviour. If you have them in a config
  file they were already doing nothing and will continue to be ignored — the
  config loader has no schema and does not reject or report unknown keys, which
  is precisely why these survived as long as they did. Removed: `blockmaxsize`
  (#1640), `securityLevelGRPC` — a casing variant that never matched the real
  `security_level_grpc` (#1674), and `blockchain_subscription_timeout` (#1643).

- **Two Blockchain RPCs now work that previously did not.**
  `GetLatestBlockHeaderFromBlockLocator` and `GetBlockHeadersFromOldest` were
  implemented under names that did not satisfy the generated server interface, so
  both returned `Unimplemented` to every gRPC caller. They now dispatch correctly.
  Callers that treated `Unimplemented` as "unsupported" will start receiving real
  responses. (#1848, pending)

- **The Postgres circuit breaker is now reachable.** `*_postgres_circuitBreakerEnabled`
  and its companions were never populated, so the breaker could not be turned on.
  They are wired now, but the default remains off, so no deployment changes behaviour
  unless you opt in. (#1642)
