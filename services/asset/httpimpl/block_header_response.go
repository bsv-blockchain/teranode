// Package httpimpl provides HTTP response handling for the blockchain service,
// implementing custom JSON marshaling for block header data.
package httpimpl

import (
	"encoding/json"
	"time"

	"github.com/bsv-blockchain/teranode/model"
)

// blockHeaderResponse represents a formatted block header response that includes
// additional metadata beyond the basic block header information. It embeds the
// BlockHeader model and adds fields for presentation purposes.
type blockHeaderResponse struct {
	*model.BlockHeader            // Embedded block header model
	Hash               string     `json:"hash"`          // Block hash in hexadecimal format
	Height             uint32     `json:"height"`        // Block height in the blockchain
	TxCount            uint64     `json:"tx_count"`      // Number of transactions in the block
	SizeInBytes        uint64     `json:"size_in_bytes"` // Total size of the block in bytes
	Miner              string     `json:"miner"`         // Miner information if available
	Invalid            bool       `json:"invalid"`       // Whether the block is marked as invalid
	ProcessedAt        *time.Time `json:"processed_at"`  // Timestamp when the block was processed (nullable)
}

// MarshalJSON implements the json.Marshaler interface for blockHeaderResponse.
// It provides a custom JSON serialization format for block header data, ensuring
// proper formatting and escaping of fields.
//
// The resulting JSON structure includes:
//   - hash: Block hash in hexadecimal
//   - version: Block version number
//   - previousblockhash: Hash of the previous block
//   - merkleroot: Merkle root hash of transactions
//   - time: Block timestamp
//   - bits: Difficulty target in compact format
//   - nonce: Proof-of-work nonce value
//   - height: Block height
//   - txCount: Number of transactions
//   - sizeInBytes: Block size in bytes
//   - miner: Escaped miner string
//
// Returns:
//   - []byte: JSON-encoded block header data
//   - error: Any error encountered during marshaling
//
// Example JSON output:
//
//	{
//	    "hash": "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f",
//	    "version": 1,
//	    "previousblockhash": "0000000000000000000000000000000000000000000000000000000000000000",
//	    "merkleroot": "4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b",
//	    "time": 1231006505,
//	    "bits": "1d00ffff",
//	    "nonce": 2083236893,
//	    "height": 0,
//	    "txCount": 1,
//	    "sizeInBytes": 285,
//	    "miner": "Satoshi"
//	}
func (r *blockHeaderResponse) MarshalJSON() ([]byte, error) {
	type aliasResponse struct {
		Hash              string     `json:"hash"`
		Version           uint32     `json:"version"`
		PreviousBlockHash string     `json:"previousblockhash"`
		MerkleRoot        string     `json:"merkleroot"`
		Time              uint32     `json:"time"`
		Bits              string     `json:"bits"`
		Nonce             uint32     `json:"nonce"`
		Height            uint32     `json:"height"`
		TxCount           uint64     `json:"txCount"`
		SizeInBytes       uint64     `json:"sizeInBytes"`
		Miner             string     `json:"miner"`
		Invalid           bool       `json:"invalid"`
		ProcessedAt       *time.Time `json:"processed_at"`
	}

	// Validate ProcessedAt timestamp before marshaling
	var processedAt *time.Time
	if r.ProcessedAt != nil {
		year := r.ProcessedAt.Year()
		// Check if the year is within the valid JSON marshaling range [0, 9999]
		if year >= 0 && year <= 9999 {
			processedAt = r.ProcessedAt
		}
		// If invalid, processedAt remains nil and will be omitted from JSON
	}

	a := aliasResponse{
		Hash:              r.Hash,
		Version:           r.Version,
		PreviousBlockHash: r.HashPrevBlock.String(),
		MerkleRoot:        r.HashMerkleRoot.String(),
		Time:              r.Timestamp,
		Bits:              r.Bits.String(),
		Nonce:             r.Nonce,
		Height:            r.Height,
		TxCount:           r.TxCount,
		SizeInBytes:       r.SizeInBytes,
		Miner:             r.Miner,
		Invalid:           r.Invalid,
		ProcessedAt:       processedAt,
	}
	return json.Marshal(a)
}

// UnmarshalJSON implements the json.Unmarshaler interface for blockHeaderResponse.
// Currently a placeholder implementation that could be expanded for future use.
//
// Parameters:
//   - []byte: JSON data to unmarshal
//
// Returns:
//   - error: Any error encountered during unmarshaling
func (r *blockHeaderResponse) UnmarshalJSON([]byte) error {
	return nil
}
