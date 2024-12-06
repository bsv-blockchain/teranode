// Package httpimpl provides HTTP response handling for the blockchain service,
// implementing custom JSON marshaling for block header data.
package httpimpl

import (
	"encoding/json"
	"fmt"

	"github.com/bitcoin-sv/ubsv/model"
)

// blockHeaderResponse represents a formatted block header response that includes
// additional metadata beyond the basic block header information. It embeds the
// BlockHeader model and adds fields for presentation purposes.
type blockHeaderResponse struct {
	*model.BlockHeader        // Embedded block header model
	Hash               string `json:"hash"`          // Block hash in hexadecimal format
	Height             uint32 `json:"height"`        // Block height in the blockchain
	TxCount            uint64 `json:"tx_count"`      // Number of transactions in the block
	SizeInBytes        uint64 `json:"size_in_bytes"` // Total size of the block in bytes
	Miner              string `json:"miner"`         // Miner information if available
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
	miner, _ := escapeJSON(r.Miner)

	return []byte(fmt.Sprintf(`{"hash":"%s","version":%d,"previousblockhash":"%s","merkleroot":"%s","time":%d,"bits":"%s","nonce":%d,"height":%d,"txCount":%d,"sizeInBytes":%d,"miner":"%s"}`,
		r.Hash,
		r.Version,
		r.HashPrevBlock.String(),
		r.HashMerkleRoot.String(),
		r.Timestamp,
		r.Bits.String(),
		r.Nonce,
		r.Height,
		r.TxCount,
		r.SizeInBytes,
		miner,
	)), nil
}

// escapeJSON properly escapes a string for use in JSON output.
// It handles special characters and ensures the resulting string
// is valid for JSON encoding.
//
// Parameters:
//   - input: Raw string to be escaped
//
// Returns:
//   - string: Properly escaped string for JSON inclusion
//   - error: Any error encountered during escaping process
//
// Example:
//
//	escaped, err := escapeJSON("Miner's Note: \"Hello\"")
//	// Returns: "Miner\'s Note: \"Hello\""
func escapeJSON(input string) (string, error) {
	// Use json.Marshal to escape the input string.
	escapedJSON, err := json.Marshal(input)
	if err != nil {
		return "", err
	}

	// Convert the JSON bytes to a string, removing the surrounding double quotes.
	escapedString := string(escapedJSON[1 : len(escapedJSON)-1])

	// Return the escaped string.
	return escapedString, nil
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
