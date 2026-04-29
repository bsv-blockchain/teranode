# P2P / libp2p code review

Library version pinned in `go.mod`: `github.com/bsv-blockchain/go-p2p-message-bus v0.1.15`. Vendor source at `$GOMODCACHE/github.com/bsv-blockchain/go-p2p-message-bus@v0.1.15`.

This review covers the six areas in the brief, plus one additional finding (Static peers) that touches several of them. Each section ends with concrete `file:line` references.

## 1. Peer discovery

### DHT
- DHT mode is wired correctly: `services/p2p/Server.go:332-335` passes `BootstrapPeers`, `DHTMode`, `DHTCleanupInterval` and `EnableNAT` straight through to the library config.
- "Silent" listen mode forces `dhtMode = "off"` (`services/p2p/Server.go:322-324`) and disables address advertisement (`Server.go:288-291`). A silent node stops participating in the DHT entirely; it can still receive over already-established subscriptions but cannot be discovered. This matches the documented intent.
- DHT "client" mode is **not** lightweight despite the name. Per the library docs (`go-p2p-message-bus/config.go:94-97`), client mode still crawls the DHT and sustains 100+ peer connections — it just doesn't write provider records. If anyone selects "client" expecting a privacy-friendly low-resource node, they will be surprised.

### Bootstrap vs. static peers
- **Static peers are silently discarded.** `settings.P2P.StaticPeers` is defined in `settings/p2p_settings.go:27`, persisted from `p2p_static_peers` in config, and is consumed only by `cmd/diagnose/config_checks.go:446` and `cmd/monitor/monitor.go:1046` — both for *display*. There is no consumer in `services/p2p/`. The library exposes `Client.Connect(ctx, addr)` (go-p2p-message-bus types) but auto-reconnect is only wired for `BootstrapPeers` (library `client.go:860-894` `maintainBootstrapConnections`). Operators who configure `p2p_static_peers` get no behaviour from it, which is what bit our multinode docker-compose tests for the past several days.
- Bootstrap peers themselves are correctly maintained — the library reconnects every 30s if they drop.

### mDNS
- `EnableMDNS` defaults to false and is correctly passed through (`Server.go:337` → library `client.go:136-145`). The library prints a "production safe default" log line if disabled.
- `settings/p2p_settings.go:62-65` warns about shared-hosting risks (mDNS multicasts on `224.0.0.251/5353` and is visible to every tenant in the same broadcast domain). There is no runtime guard — if `EnableMDNS=true` is set on a Hetzner/AWS deployment, it'll happily broadcast.

### Peer churn
- `services/p2p/peer_registry.go` has no eviction policy. `Put()` (line 67) only removes via explicit `Remove()` (line 111) or via the disconnect handler in `sync_coordinator.go:218`. A peer that reconnects with a fresh peer ID adds an entry; one that goes offline forever leaves its entry behind. There is no TTL, LRU, or background sweep over the registry.
- `BanManager` does have a background cleanup ticker (`BanManager.go:156`, runs every `decayInterval` defaulting to 1 minute) that decays scores by 1/min and removes entries with score 0 and no active ban.
- `peer_registry.go:600` defines `ReconsiderBadPeers()` with exponential cooldown logic — but it's never called from anywhere in the codebase. Peers stuck at low reputation never recover.

## 2. Pub/sub topology and relay

### Implementation
- The library uses **gossipsub** (`pubsub.NewGossipSub(ctx, h, pubsub.WithPeerExchange(true))` at `go-p2p-message-bus/client.go:127`). Only `WithPeerExchange(true)` is set; every other parameter is libp2p's default. Underlying lib is `go-libp2p-pubsub v0.15.0`.

### Mesh parameters (libp2p defaults)
| Parameter | Default | Note |
|---|---|---|
| `D` (target mesh degree) | 6 | direct gossip peers per topic per node |
| `D_lo` / `D_hi` | 5 / 12 | graft / prune thresholds |
| `D_out` | 2 | outbound quota (sybil defense) |
| `D_lazy`, `GossipFactor` | 6, 0.25 | IHAVE/IWANT gossip fanout |
| heartbeat interval | 1 s | mesh maintenance tick |
| seenMessages TTL | 2 min | dedup cache |

These defaults are sensible for networks up to ~10K nodes. Latency is `O(log N)` because each node forwards to D=6 peers. Beyond ~100K nodes the constant-degree mesh is no longer adequate; that's a future-tuning concern, not a current issue.

### Are messages relayed via gossip, or fanned out via direct connections?
**Gossip, confirmed.** Every publish path (`Server.go:820, 1067, 1393, 1428`) calls `s.P2PClient.Publish(ctx, topic, bytes)`, which delegates to `topic.Publish` on the gossipsub topic handle. There is exactly one place in `services/p2p/` that iterates over all known peers (`Server.go:1191`) and that's a *read* — looking up sync peer heights, not a publish loop. The reviewer's specific worry — that we hold a direct connection to every known peer — is not happening today.

### Seen-message dedup
- libp2p's pubsub maintains a `seenMessages` time-cache keyed by message ID (`go-libp2p-pubsub/pubsub.go:536, 1399, 1416`). Duplicate messages by ID are dropped before being forwarded. Default TTL is 2 minutes.
- Teranode adds an application-layer 10MB size guard (`Server.go:66, 913-914`) before JSON unmarshal.
- A peer publishing many *different* malformed messages would still propagate them, since the dedup is by message ID, not by content. See "Validators" below.

### Validators / rate limiting
- **No `RegisterTopicValidator` calls anywhere.** No `pubsub.WithValidator`. No per-topic schema check before forwarding. A malformed-but-parseable JSON payload will be relayed by the whole mesh before any node catches the schema violation.
- No per-peer rate limiting on inbound topic messages. A peer publishing 100 fake `node_status` messages per second produces 100 distinct message IDs and gossipsub will fan all of them out.

### NodeStatus heartbeat scaling
`NodeStatusMessage` ≈ 846 bytes JSON-serialised (per `Server.go:1219-1345`). Published every 10 s on `p2p_node_status_topic` (`Server.go:1081`). Every node receives every other node's heartbeat once via gossip. So per-node ingress from the node-status topic alone is roughly:

| N | heartbeats/s received | bandwidth |
|---|---|---|
| 100 | 10 | ~8 KB/s |
| 1,000 | 100 | ~85 KB/s |
| 10,000 | 1,000 | ~850 KB/s |

(Connection count stays constant at D=6 — that's not the issue. The issue is that each node still sees every heartbeat at least once.) At 10K nodes the heartbeat alone is ~7 Mbps, before any block or subtree traffic, before any retransmits. That's not a hard wall but it's a real cost at scale that nobody seems to have modelled. Easy mitigations exist (back off the interval as cluster size grows; piggyback heartbeat on existing block/subtree messages; use a slower secondary topic for heartbeats).

### Topic granularity
Four topics — block, subtree, node_status, rejected_tx (`Server.go:608-611`). Boundaries look reasonable. Block and subtree carry payloads, node_status is steady-state, rejected_tx is event-driven and sparse. No obvious wins from merging or splitting today.

## 3. Message handling

Already covered above:

- **No validators** — schema and rate are not enforced at the gossipsub layer. **Application-layer parsing happens after the message has been forwarded to the mesh**, so garbage propagates one hop further than it needs to.
- The 10 MB ceiling is generous for a metadata message. A 9.5 MB JSON blob still fits and still costs CPU to parse. Tighter per-topic limits (e.g. 1 MB for `node_status`, 256 KB for `rejected_tx`) would catch obvious abuse.
- No message signing / origin authentication. Today the network trusts `peer.FromID`. Acceptable while bootstrap peers are curated; risky once the network is open.

## 4. Reputation, ban management, peer-map

### Reputation (`peer_registry.go`)
- Hybrid model: event-driven recompute (`calculateAndUpdateReputation` at `peer_registry.go:381`) on every interaction, no time-based decay loop. Score range 0–100, starting at 50. Inputs: success rate (60% weight), recency penalties / bonuses, latency factor (0.6×–1.2×).
- Consumed by `GetPeersByReputation()` (line 538), `GetPeersForCatchup()` (line 712), `PeerSelector.isEligible()` (peer_selector.go:202), and `isViableSyncCandidate()` (sync_coordinator.go:104) — all gate on reputation ≥ 20.
- Score is **not persisted**. Restart resets every peer to neutral (50). Hard-won deprioritisation of misbehaving peers is lost across restarts.

### BanManager (`BanManager.go`)
- Triggers add weighted scores (invalid-block/subtree +10, protocol-violation +20, spam +50). Threshold default 100 → 24-hour ban.
- Decay –1/minute via background ticker (`BanManager.go:156`). Bans expire lazily on `IsBanned()` check (line 279) — fine.
- **Ban state is not persisted either.** A restart wipes the ban list. Coordinated peers can time their reconnection around restarts. Bans are 24h by default so persistence is meaningful.
- Reputation and ban score are decoupled. A peer can have reputation 5 and ban score 0, and still be selectable until reputation drops it below the eligibility threshold. Tighter coupling (e.g. very-low rep auto-applies a soft ban) would be a defensible product choice but that's tuning, not correctness.

### PeerMap settings
- `PeerMapMaxSize` (100 000), `PeerMapTTL` (30 m), `PeerMapCleanupInterval` (5 m) all do what their names imply. `startPeerMapCleanup` (`server_helpers.go:949`) starts a goroutine that runs `cleanupPeerMaps` (line 733) on a ticker: TTL pass first, then LRU eviction if still over the limit.
- 5-minute cleanup tick on a 30-min TTL is generous, but in a high-throughput period the maps can spike past `MaxSize` between ticks before LRU catches up. Spike memory pressure, not unbounded growth.

### Stale-entry growth
- `PeerRegistry` itself is the unbounded one. Peers are added on first contact and never aged out. Given the registry is used in O(n) iteration paths (`GetAll`, `GetPeersForCatchup`), this becomes both a memory and a latency problem under sustained churn. This is the most operationally serious of all the findings.
- `LastSyncAttempt` is cleared only by `ClearAllSyncAttempts()` on backoff recovery. Functionally harmless but adds to the registry-bloat picture.

## 5. Sync peer selection

`services/p2p/sync_coordinator.go` and `peer_selector.go`.

- **Single peer at a time** — deliberate, locked behind `mu sync.RWMutex` (line 32). `TriggerSync` clears, selects, sends via Kafka. No concurrent peer changes.
- Selection key (peer_selector.go:42): full-storage nodes first, sorted reputation desc → response time asc → ban score asc → height desc. Falls back to youngest pruned node only if `AllowPrunedNodeFallback`.
- Stall handling (`evaluateSyncPeer` line 566) runs every `SyncCoordinatorPeriodicEvaluationInterval` (default 30s) and switches peers when:
  - reputation drops below 20,
  - peer disappears from registry,
  - syncing > 5 min with no inbound message for 1 min (records `RecordCatchupFailure`).
- Repeated failures degrade reputation; they don't auto-ban. There's commented-out malicious-detection logic at `sync_coordinator.go:390-405` that would close the loop, currently disabled.
- **No parallel fan-out.** This is a deliberate "cooperative" choice — a single-peer model is simpler to reason about and avoids redundant block downloads — but it also means a slow but-not-failing peer can throttle the whole node's catch-up. Worth reconsidering once we're past the initial network growth phase.

## 6. Idiomatic libp2p

- Teranode does not call `host.Connect`, `Peerstore.Add`, or `Routing` queries directly anywhere in `services/`. Everything goes through go-p2p-message-bus. That's a clean abstraction line.
- The library does not currently expose: peerstore queries, custom multistream handlers, hole-punching control, or static-peer maintenance with auto-reconnect. The main one we *would* want is the static-peer primitive (see #1) — the library's `Connect` is a one-shot and we'd have to build the supervisor ourselves on top.
- Connection manager defaults to min=25 / max=35 / grace=20s (`go-p2p-message-bus/client.go:276-286`). Teranode does not override. For a private network of say 10 nodes that's fine. For a node sitting in a 1K-peer mesh, hitting max=35 means the connection manager is constantly pruning, which is precisely the regime where peer churn matters and where lost connections cost the most. Worth exposing as a setting and tuning.

---

## Concerns ranked

### Blocker

1. **`StaticPeers` is dead code in `services/p2p/`.** Defined, parsed, displayed in diagnostics, never used. Operators who configure it expect persistent peer connections; they get nothing. This caused us to chase phantom flake in the multinode test harness for a week. Either make `services/p2p/Server.go` consume it (extending go-p2p-message-bus to handle auto-reconnect for static peers, or supervising it in teranode), or drop the setting entirely so it can't mislead.
2. **`PeerRegistry` has no eviction policy.** In a network with regular peer churn the map grows without bound, exhausting memory and slowing down every O(n) lookup. Need TTL or LRU.
3. **No gossipsub topic validators.** Schema and rate aren't enforced before relay. Garbage propagates through the entire mesh before any node rejects it on JSON unmarshal. Highest-leverage hardening change against an open network.

### Should-fix

4. **Reputation scores and ban state are not persisted.** Restart wipes the lot. A coordinated misbehaving peer can survive any 24h ban by waiting for a restart. Persist to local storage (sqlite or a simple file) and rehydrate on boot.
5. **`ReconsiderBadPeers()` is dead code** (`peer_registry.go:600`). Wire it into a periodic tick — or delete it. Currently a peer at rep 15 stays at 15 forever even if everything else has changed.
6. **`NodeStatus` heartbeat bandwidth is `O(N)` per node and unaccounted for.** At 10K nodes it's ~7 Mbps just for heartbeats. Either back off the interval as `N` grows, or piggyback heartbeats on existing block/subtree traffic.
7. **Connection-manager limits are not exposed as settings.** Library default of 35 max connections is too low for the steady-state of even a moderate mesh, given that gossipsub wants D=6 connections *per topic*. At 4 topics that's 24 right there before bootstrap and DHT.
8. **DHT "client" mode is misnamed.** It's not a lightweight mode. Settings docs at `settings/p2p_settings.go:50` should call this out, or we should ask the library to rename it.
9. **mDNS has no runtime guard.** Docs warn but the code happily multicasts on shared infrastructure. Either gate on a private-network probe at startup or make it a build-tag.

### Nice-to-have

10. Tighter per-topic size limits (`node_status` does not need 10MB).
11. Per-message origin signature so we can stop trusting `peer.FromID` once the network is open.
12. Re-enable the disabled malicious-peer detection in `sync_coordinator.go:390-405`.
13. Optional parallel sync from multiple peers, with the existing single-peer mode kept as the default.
14. A `connection-churn` rate metric would make peer-stability problems easier to spot.

## Suggested follow-up tickets

1. **Wire StaticPeers through to libp2p connect-and-supervise.** Either extend `go-p2p-message-bus` to maintain static peers automatically, or call `Client.Connect` from `services/p2p/Server.go` on startup and supervise reconnects in teranode.
2. **PeerRegistry eviction.** Add LRU + TTL using the existing `PeerMapMaxSize` / `PeerMapTTL` style settings.
3. **Persist reputation and bans.** Spec the storage format, decide on a file vs. SQLite.
4. **Register gossipsub topic validators** for all four topics. Schema + per-peer rate limit.
5. **Heartbeat bandwidth at scale.** Either rate-adapt with cluster size or merge into block topic.
6. **Expose connection-manager settings.** Min/Max/Grace as `settings.P2P.MaxConnections` etc.
7. **Documentation pass on DHT "client" mode and mDNS production safety.**
8. **Activate the commented-out catchup-malice detection** in `sync_coordinator.go`.
