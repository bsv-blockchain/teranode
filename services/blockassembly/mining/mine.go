// Package mining provides functionality for Bitcoin mining operations in Teranode.
package mining

import (
	"context"
	"math"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
)

// Mine attempts to mine a block using the provided mining candidate and optional address.
// It performs the mining operation by trying different nonce values until finding one that
// produces a valid block hash meeting the target difficulty.
//
// Parameters:
//   - ctx: Context for cancellation
//   - tSettings: The Teranode settings
//   - candidate: The mining candidate containing block template information
//   - address: Optional address to receive mining rewards
//
// Returns:
//   - *model.MiningSolution: Contains the successful mining solution if found
//   - error: Any error encountered during mining
func Mine(ctx context.Context, tSettings *settings.Settings, candidate *model.MiningCandidate, address *string) (*model.MiningSolution, error) {
	var coinbaseTx *bt.Tx

	var err error

	if address != nil {
		coinbaseTx, err = candidate.CreateCoinbaseTxCandidateForAddress(tSettings, address)
		if err != nil {
			return nil, err
		}
	} else {
		coinbaseTx, err = candidate.CreateCoinbaseTxCandidate(tSettings)
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
