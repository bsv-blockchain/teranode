package p2p

type BestBlockMessage struct {
	PeerId string
}

type MiningOnMessage struct {
	Hash         string
	PreviousHash string
	DataHubUrl   string
	PeerId       string
	Height       uint32
	Miner        string
	SizeInBytes  uint64
	TxCount      uint64
}

type BlockMessage struct {
	Hash       string
	Height     uint32
	DataHubUrl string
	PeerId     string
}
type SubtreeMessage struct {
	Hash       string
	DataHubUrl string
	PeerId     string
}

type RejectedTxMessage struct {
	TxId   string
	Reason string
	PeerId string
}
