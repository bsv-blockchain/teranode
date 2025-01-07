package mining

import (
	"context"
	"math"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
)

func Mine(ctx context.Context, candidate *model.MiningCandidate, address *string) (*model.MiningSolution, error) {
	var coinbaseTx *bt.Tx

	var err error

	if address != nil {
		coinbaseTx, err = candidate.CreateCoinbaseTxCandidateForAddress(address)
		if err != nil {
			return nil, err
		}
	} else {
		coinbaseTx, err = candidate.CreateCoinbaseTxCandidate()
		if err != nil {
			return nil, err
		}
	}

	merkleRoot := util.BuildMerkleRootFromCoinbase(coinbaseTx.TxIDChainHash().CloneBytes(), candidate.MerkleProof)

	previousHash, _ := chainhash.NewHash(candidate.PreviousHash)
	merkleRootHash, _ := chainhash.NewHash(merkleRoot)

	nBits, _ := model.NewNBitFromSlice(candidate.NBits)

	nonce := uint32(0)

	for {
		select {
		case <-ctx.Done():
			break
		default:
			blockHeader := model.BlockHeader{
				Version:        candidate.Version,
				HashPrevBlock:  previousHash,
				HashMerkleRoot: merkleRootHash,
				Timestamp:      candidate.Time,
				Bits:           *nBits,
				Nonce:          nonce,
			}

			headerValid, blockHash, _ := blockHeader.HasMetTargetDifficulty()
			if headerValid {
				return &model.MiningSolution{
					Id:        candidate.Id,
					Nonce:     nonce,
					Time:      &candidate.Time,
					Coinbase:  coinbaseTx.Bytes(),
					Version:   &candidate.Version,
					BlockHash: blockHash.CloneBytes(),
				}, nil
			}
		}

		if nonce == math.MaxUint32 {
			return nil, errors.NewProcessingError("nonce overflow")
		}

		nonce++
	}
}
