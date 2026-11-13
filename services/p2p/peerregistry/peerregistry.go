// Package peerregistry carries the timing facts that both halves of the peer
// registry lifecycle must agree on: the P2P service refreshes a peer's
// registry liveness on one cadence, the Blockchain service stores and expires
// the records on another, and the expiry floor is arithmetic over the P2P
// cadence. Before this package that arithmetic lived as prose comments in
// three files across two services.
//
// Keep this package a dependency-free leaf: services/p2p imports
// services/blockchain, so the shared numbers must sit where both can reach
// them without a cycle.
package peerregistry

import "time"

// NodeStatusPublishInterval is how often the P2P service recomputes and
// republishes node status, which refreshes LastSeen for connected peers in
// the registry (registered via the Blockchain service's Register).
const NodeStatusPublishInterval = 10 * time.Second

// RegistryReassertTTL is how long a gossiped RegisterPeer / connection-state
// assertion counts as fresh before the gossip batcher re-asserts it. It is
// the batcher's coalescing window: a peer keeps gossiping inside it, but the
// registry sees no refresh for that long.
const RegistryReassertTTL = time.Minute

// MinTTLMultiplier is how many reassert windows the registry TTL floor spans.
// Five gives room for a missed batcher beat or two (GC pause, Kafka stall)
// between a live peer's own registry refreshes.
const MinTTLMultiplier = 5

// MinTTL is the floor below which a configured p2p_peer_registry_ttl is
// raised at start-up: the Blockchain cleanup loop must not be able to expire
// an actively-connected peer between its own batched refreshes. An evicted
// peer then fails the catch-up health check and is silently refused as a
// source of blocks - so the floor is not a performance knob, and it is
// computed here rather than restated as a literal.
const MinTTL = time.Duration(MinTTLMultiplier) * RegistryReassertTTL
