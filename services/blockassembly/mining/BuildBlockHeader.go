package mining

import (
	"encoding/binary"

	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/libsv/go-bt/v2"
)

// BuildBlockHeader builds the block header byte array from the specific fields in the header.
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
