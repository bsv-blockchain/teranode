package p2p

type BestBlockMessage struct {
	PeerID string
}

type MiningOnMessage struct {
	Hash         string
	PreviousHash string
	DataHubURL   string
	PeerID       string
	Height       uint32
	Miner        string
	SizeInBytes  uint64
	TxCount      uint64
}

type BlockMessage struct {
	Hash       string
	Height     uint32
	DataHubURL string
	PeerID     string
}
type SubtreeMessage struct {
	Hash       string
	DataHubURL string
	PeerID     string
}

type RejectedTxMessage struct {
	TxID   string
	Reason string
	PeerID string
}
