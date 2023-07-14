package cpuminer

import (
	"encoding/binary"

	"github.com/TAAL-GmbH/ubsv/model"
	"github.com/TAAL-GmbH/ubsv/util"
	"github.com/libsv/go-bt/v2"
)

// BuildBlockHeader builds the block header byte array from the specific fields in the header.
func BuildBlockHeader(candidate *model.MiningCandidate, solution *model.MiningSolution) ([]byte, error) {
	v := make([]byte, 4)
	binary.LittleEndian.PutUint32(v, solution.Version)
	t := make([]byte, 4)
	binary.LittleEndian.PutUint32(t, solution.Time)
	n := make([]byte, 4)
	binary.LittleEndian.PutUint32(n, solution.Nonce)

	coinbaseTx, err := bt.NewTxFromBytes(solution.Coinbase)
	if err != nil {
		return nil, err
	}
	merkleRoot := util.BuildMerkleRootFromCoinbase(bt.ReverseBytes(coinbaseTx.TxIDBytes()), candidate.MerkleProof)

	a := []byte{}
	a = append(a, v...)
	a = append(a, candidate.PreviousHash...)
	a = append(a, merkleRoot...)
	a = append(a, t...)
	a = append(a, candidate.NBits...)
	a = append(a, n...)
	return a, nil
}
