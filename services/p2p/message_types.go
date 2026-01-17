package p2p

// NodeStatusMessage represents a node status update message
type NodeStatusMessage struct {
	PeerID              string   `json:"peer_id"`
	ClientName          string   `json:"client_name"` // Name of this node client
	Type                string   `json:"type"`
	BaseURL             string   `json:"base_url"`
	PropagationURL      string   `json:"propagation_url,omitempty"` // Optional URL for peers to use for propagating txs (defaults to BaseURL if empty)
	Version             string   `json:"version"`
	CommitHash          string   `json:"commit_hash"`
	BestBlockHash       string   `json:"best_block_hash"`
	BestHeight          uint32   `json:"best_height"`
	TxCount             uint64   `json:"tx_count,omitempty"`      // Number of transactions in block assembly
	SubtreeCount        uint32   `json:"subtree_count,omitempty"` // Number of subtrees in block assembly
	FSMState            string   `json:"fsm_state"`
	StartTime           int64    `json:"start_time"`
	Uptime              float64  `json:"uptime"`
	MinerName           string   `json:"miner_name"` // Name of the miner that mined the best block
	ListenMode          string   `json:"listen_mode"`
	ChainWork           string   `json:"chain_work"`                      // Chain work as hex string
	SyncPeerID          string   `json:"sync_peer_id,omitempty"`          // ID of the peer we're syncing from
	SyncPeerHeight      uint32   `json:"sync_peer_height,omitempty"`      // Height of the sync peer
	SyncPeerBlockHash   string   `json:"sync_peer_block_hash,omitempty"`  // Best block hash of the sync peer
	SyncConnectedAt     int64    `json:"sync_connected_at,omitempty"`     // Unix timestamp when we first connected to this sync peer
	MinMiningTxFee      *float64 `json:"min_mining_tx_fee,omitempty"`     // Minimum mining transaction fee configured for this node (nil = unknown, 0 = no fee)
	ConnectedPeersCount int      `json:"connected_peers_count,omitempty"` // Number of connected peers
	Storage             string   `json:"storage,omitempty"`               // Storage mode: "full" (block persister running and caught up), "pruned" (no persister or lagging), or empty (old version)
}

// BlockMessage announces the availability of a new block to the P2P network.
// This message is used for block propagation, allowing nodes to efficiently
// distribute new blocks across the network. It contains essential block metadata
// and a reference to where the full block data can be retrieved.
//
// The message enables efficient block distribution by providing metadata first,
// allowing peers to decide whether they need the full block data before downloading it.
type BlockMessage struct {
	PeerID     string // Identifier of the peer announcing the block
	ClientName string // Name of the client software announcing the block
	DataHubURL string // URL where the complete block data can be retrieved
	Hash       string // Unique hash identifier of the block
	Height     uint32 // Position of the block in the blockchain
	Header     string // Hexadecimal representation of the block header
	Coinbase   string // Hexadecimal representation of the coinbase transaction
}

// SubtreeMessage announces the availability of a subtree (transaction batch) to the network.
// In Teranode's architecture, subtrees represent collections of transactions that are
// processed and validated together. This message enables efficient distribution of
// transaction data across the P2P network.
//
// The message allows nodes to:
//   - Discover new transaction batches
//   - Coordinate transaction processing
//   - Maintain network-wide transaction visibility
//   - Optimize bandwidth usage through selective downloading
type SubtreeMessage struct {
	PeerID     string // Identifier of the peer announcing the subtree
	ClientName string // Name of the client software announcing the subtree
	DataHubURL string // URL where the subtree data can be retrieved
	Hash       string // Unique hash identifier of the subtree
}

// RejectedTxMessage notifies peers about a rejected transaction.
// This message is used to inform the network about transactions that have been
// rejected due to validation errors or other reasons, helping to prevent
// unnecessary retransmissions and optimize network traffic.
//
// The message contains the transaction ID and the reason for rejection,
// enabling peers to update their transaction sets and avoid retransmitting
// the rejected transaction.
type RejectedTxMessage struct {
	PeerID     string // Identifier of the peer reporting the rejection
	ClientName string // Name of the client software reporting the rejection
	TxID       string // Identifier of the rejected transaction
	Reason     string // Reason for the transaction rejection
}
