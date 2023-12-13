package model

type BlockHeaderMeta struct {
	ID          uint32 `json:"id"`            // ID of the block in the internal blockchain DB.
	Height      uint32 `json:"height"`        // Height of the block in the blockchain.
	TxCount     uint64 `json:"tx_count"`      // Number of transactions in the block.
	SizeInBytes uint64 `json:"size_in_bytes"` // Size of the block in bytes.
	Miner       string `json:"miner"`         // Miner
}
