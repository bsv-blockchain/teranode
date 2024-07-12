package legacy

import (
	"bytes"
	"fmt"
	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/legacy/wire"
	"github.com/libsv/go-bt/v2/chainhash"
)

// Function to verify the headers chain
func verifyHeadersChain(headers []*wire.BlockHeader, lastHash *chainhash.Hash) error {
	prevHash := lastHash

	for _, header := range headers {
		// Check if the header's previous block hash matches the previous header's hash
		if !header.PrevBlock.IsEqual(prevHash) {
			return fmt.Errorf("header's PrevBlock doesn't match previous header's hash")
		}

		// Serialize the header and double hash it to get the proof-of-work hash
		var buf bytes.Buffer
		err := header.Serialize(&buf)
		if err != nil {
			return fmt.Errorf("failed to serialize header: %v", err)
		}

		// Check if the proof-of-work hash meets the target difficulty
		if !checkProofOfWork(header) {
			return fmt.Errorf("header does not meet proof-of-work requirements")
		}

		// Move to the next header
		h := header.BlockHash()
		prevHash = &h
	}

	return nil
}

func checkProofOfWork(header *wire.BlockHeader) bool {
	// Serialize the header
	var buf bytes.Buffer
	err := header.Serialize(&buf)
	if err != nil {
		return false
	}

	bh, err := model.NewBlockHeaderFromBytes(buf.Bytes())
	if err != nil {
		return false
	}

	ok, _, err := bh.HasMetTargetDifficulty()
	if err != nil {
		return false
	}

	return ok
}
