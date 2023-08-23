package http_impl

import (
	"fmt"

	"github.com/bitcoin-sv/ubsv/model"
)

type blockHeaderResponse struct {
	*model.BlockHeader
	Hash   string `json:"hash"`
	Height uint32 `json:"height"`
}

func (r *blockHeaderResponse) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`{"hash":"%s","version":%d,"previousblockhash":"%s","merkleroot":"%s","time":%d,"bits":"%s","nonce":%d,"height":%d}`,
		r.Hash,
		r.Version,
		r.HashPrevBlock.String(),
		r.HashMerkleRoot.String(),
		r.Timestamp,
		r.Bits.String(),
		r.Nonce,
		r.Height,
	)), nil
}

func (r *blockHeaderResponse) UnmarshalJSON([]byte) error {
	return nil
}
