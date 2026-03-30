# P2P Silent Mode

A third listen mode — `silent` — which goes further than `listen_only`:

| Behaviour | `full` | `listen_only` | `silent` (new) |
|---|---|---|---|
| Receives blocks/subtrees from gossip | ✅ | ✅ | ✅ |
| Publishes block/subtree/rejected-tx announcements | ✅ | ❌ | ❌ |
| Publishes `node_status` to P2P network | ✅ | ✅ | ❌ |
| Advertises its addresses to peers | ✅ | ✅ | ❌ |
| Participates in DHT (peer discovery) | ✅ | ✅ | ❌ (forced `"off"`) |
| Suppresses DataHub/PropagationURL | ❌ | ✅ | ✅ |
| Forwards status to local WebSocket | ✅ | ✅ | ✅ |

The key distinction: `listen_only` still publishes its node status and participates in DHT/address advertisement, so other peers can discover it. `silent` suppresses all of that — it connects outbound to configured peers but never announces its own existence to the network.
