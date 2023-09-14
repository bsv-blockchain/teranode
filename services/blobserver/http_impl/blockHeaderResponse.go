package http_impl

import (
	"encoding/json"
	"fmt"

	"github.com/bitcoin-sv/ubsv/model"
)

type blockHeaderResponse struct {
	*model.BlockHeader
	Hash        string `json:"hash"`
	Height      uint32 `json:"height"`
	TxCount     uint64 `json:"tx_count"`
	SizeInBytes uint64 `json:"size_in_bytes"`
	Miner       string `json:"miner"`
}

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

func (r *blockHeaderResponse) UnmarshalJSON([]byte) error {
	return nil
}
