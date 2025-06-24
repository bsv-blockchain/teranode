package p2p

// BestBlockRequestMessage represents a request for the best block information from a peer.
// This message is used in the P2P handshake process to query peers about their current
// best block, enabling blockchain synchronization and peer height comparison.
//
// The message contains the requesting peer's identifier to allow the recipient
// to respond appropriately and track the source of the request.
type BestBlockRequestMessage struct {
	PeerID string // Identifier of the peer making the request
}

// MiningOnMessage announces that a peer has started mining on a specific block.
// This message is broadcast to inform the network about mining activity and helps
// coordinate mining efforts across the network. It contains comprehensive information
// about the block being mined and the mining peer's capabilities.
//
// The message enables other nodes to:
//   - Track mining activity across the network
//   - Coordinate their own mining efforts
//   - Monitor network hash rate and mining distribution
//   - Validate mining announcements against known block data
type MiningOnMessage struct {
	Hash         string // Hash of the block being mined
	PreviousHash string // Hash of the previous block in the chain
	DataHubURL   string // URL where block data can be retrieved
	PeerID       string // Identifier of the mining peer
	Height       uint32 // Block height in the blockchain
	Miner        string // Identifier or address of the miner
	SizeInBytes  uint64 // Size of the block in bytes
	TxCount      uint64 // Number of transactions in the block
}

// BlockMessage announces the availability of a new block to the P2P network.
// This message is used for block propagation, allowing nodes to efficiently
// distribute new blocks across the network. It contains essential block metadata
// and a reference to where the full block data can be retrieved.
//
// The message enables efficient block distribution by providing metadata first,
// allowing peers to decide whether they need the full block data before downloading it.
type BlockMessage struct {
	Hash       string // Unique hash identifier of the block
	Height     uint32 // Position of the block in the blockchain
	DataHubURL string // URL where the complete block data can be retrieved
	PeerID     string // Identifier of the peer announcing the block
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
	Hash       string // Unique hash identifier of the subtree
	DataHubURL string // URL where the subtree data can be retrieved
	PeerID     string // Identifier of the peer announcing the subtree
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
	TxID   string // Identifier of the rejected transaction
	Reason string // Reason for the transaction rejection
	PeerID string // Identifier of the peer reporting the rejection
}

// MessageType represents the type of handshake message in the P2P protocol.
// These constants define the standard message types used during the peer handshake
// process to establish communication and exchange version information.
type MessageType string

const (
	Version MessageType = "version" // Initial handshake message containing peer capabilities and blockchain state
	Verack  MessageType = "verack"  // Acknowledgment message confirming receipt of version message
)

// HandshakeMessage carries version/verack information between peers during connection establishment.
// This message is the core of the P2P handshake protocol, allowing peers to exchange their
// current blockchain state, capabilities, and connection information. It supports both
// initial version announcements and acknowledgment responses.
//
// The handshake process works as follows:
//   1. Connecting peer sends a "version" message with their current state
//   2. Receiving peer responds with a "verack" message to acknowledge
//   3. Both peers now have each other's capabilities and can begin normal communication
//
// Fields are JSON-tagged to support serialization for network transmission.
type HandshakeMessage struct {
	Type       MessageType `json:"type"`
	PeerID     string      `json:"peerID"`
	BestHeight uint32      `json:"bestHeight"`
	BestHash   string      `json:"bestHash"`
	DataHubURL string      `json:"dataHubURL"`
	UserAgent  string      `json:"userAgent"`
	Services   uint64      `json:"services"`
}
