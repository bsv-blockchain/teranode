// Package legacy implements a Bitcoin SV legacy protocol server that handles peer-to-peer communication
// and blockchain synchronization using the traditional Bitcoin network protocol.
package legacy

import (
	"bytes"
	"fmt"

	"github.com/bitcoin-sv/teranode/model"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
)

// verifyHeadersChain validates a sequence of block headers to ensure they form a valid chain.
//
// This function performs critical validation checks on a sequence of block headers:
// - Ensures each header's previous block hash matches the hash of the preceding header
// - Verifies each header meets proof-of-work requirements through difficulty validation
// - Validates block header serialization integrity
//
// The validation starts from a trusted header (lastHash) and works forward through the sequence,
// ensuring each subsequent header properly links to form a valid blockchain.
//
// Parameters:
//   - headers: Slice of block headers to verify, in sequential order from oldest to newest
//   - lastHash: Hash of the known-valid block that precedes the first header in the sequence
//
// Returns an error if any validation check fails, detailing the specific failure reason:
//   - Broken chain linkage (header's PrevBlock doesn't match previous hash)
//   - Serialization failures
//   - Insufficient proof-of-work (difficulty target not met)
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

// checkProofOfWork verifies that a block header meets the required proof-of-work difficulty target.
//
// This function implements the core consensus rule of Bitcoin that ensures blocks have
// demonstrated sufficient computational work. It does this by:
// 1. Serializing the block header to bytes
// 2. Converting to the model.BlockHeader type for validation
// 3. Checking if the resulting hash meets the difficulty target embedded in the header
//
// The function handles all serialization and validation errors internally, returning
// a simple boolean result indicating whether the proof-of-work requirement was met.
//
// Parameters:
//   - header: The block header to check for proof-of-work validity
//
// Returns:
//   - true if the header meets the proof-of-work requirement
//   - false if the header fails validation or any error occurs during processing
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
