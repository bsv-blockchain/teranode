// Package mining provides functionality for Bitcoin mining operations in Teranode.
//
// The mining package implements critical components for the Bitcoin mining process:
//   - Block header construction and validation
//   - Mining candidate preparation
//   - Proof-of-work calculation and verification
//   - Mining solution processing
//
// This package serves as the interface between the block assembly service and
// the mining process, translating assembled transactions into mineable block
// templates and verifying mining solutions against the consensus rules.

package mining

import (
	"encoding/binary"

	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/libsv/go-bt/v2"
)

// BuildBlockHeader constructs a block header byte array from the mining candidate and solution.
// It assembles the various components of a block header including version, previous block hash,
// merkle root, timestamp, difficulty bits, and nonce.
//
// Parameters:
//   - candidate: The mining candidate containing block template information
//   - solution: The mining solution containing the successful nonce and other parameters
//
// Returns:
//   - []byte: The constructed block header as a byte array
//   - error: Any error encountered during header construction
func BuildBlockHeader(candidate *model.MiningCandidate, solution *model.MiningSolution) ([]byte, error) {
	version := candidate.Version
	if solution.Version != nil {
		version = *solution.Version
	}

	v := make([]byte, 4)
	binary.LittleEndian.PutUint32(v, version)

	time := candidate.Time
	if solution.Time != nil {
		time = *solution.Time
	}

	t := make([]byte, 4)
	binary.LittleEndian.PutUint32(t, time)

	n := make([]byte, 4)
	binary.LittleEndian.PutUint32(n, solution.Nonce)

	coinbaseTx, err := bt.NewTxFromBytes(solution.Coinbase)
	if err != nil {
		return nil, err
	}

	merkleRoot := util.BuildMerkleRootFromCoinbase(coinbaseTx.TxIDChainHash().CloneBytes(), candidate.MerkleProof)

	a := []byte{}
	a = append(a, v...)
	a = append(a, candidate.PreviousHash...)
	a = append(a, merkleRoot...)
	a = append(a, t...)
	a = append(a, candidate.NBits...)
	a = append(a, n...)

	return a, nil
}
