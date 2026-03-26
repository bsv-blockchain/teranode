# p2p Service

This service uses libp2p (via `go-p2p-message-bus`) for peer communication.

* Peer discovery via DHT and optional bootstrap/static peers
* Custom gossip-based message propagation for blocks and subtrees
* Persists Ed25519 private key for peer identity and encryption

Connects to other peers and listens to the blockchain service via Kafka and gRPC.