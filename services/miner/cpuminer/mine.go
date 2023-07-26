package cpuminer

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/TAAL-GmbH/ubsv/model"
	"github.com/TAAL-GmbH/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
	"github.com/ordishs/gocore"
)

func Mine(ctx context.Context, candidate *model.MiningCandidate) (*model.MiningSolution, error) {
	// Create a new coinbase transaction

	arbitraryText, _ := gocore.Config().Get("coinbase_arbitrary_text", "/TERANODE/")
	walletAddress, found := gocore.Config().Get("coinbase_wallet_address")
	if !found {
		log.Fatal(errors.New("coinbase_wallet_address not found in config"))
	}

	a, b, err := GetCoinbaseParts(candidate.Height, candidate.CoinbaseValue, arbitraryText, walletAddress)
	if err != nil {
		return nil, fmt.Errorf("error creating coinbase transaction: %v", err)
	}

	// The extranonce length is 12 bytes.  We need to add 12 bytes to the coinbase a part
	extranonce := make([]byte, 12)
	a = append(a, extranonce...)
	a = append(a, b...)

	coinbaseTx, err := bt.NewTxFromBytes(a)
	if err != nil {
		return nil, fmt.Errorf("error decoding coinbase transaction: %v", err)
	}

	merkleRoot := util.BuildMerkleRootFromCoinbase(bt.ReverseBytes(coinbaseTx.TxIDBytes()), candidate.MerkleProof)

	previousHash, _ := chainhash.NewHash(candidate.PreviousHash)
	merkleRootHash, _ := chainhash.NewHash(merkleRoot)

	var nonce uint32

miningLoop:
	for {
		select {
		case <-ctx.Done():
			return nil, nil
		default:
			blockHeader := model.BlockHeader{
				Version:        candidate.Version,
				HashPrevBlock:  previousHash,
				HashMerkleRoot: merkleRootHash,
				Timestamp:      candidate.Time,
				Bits:           model.NewNBitFromSlice(candidate.NBits),
				Nonce:          nonce,
			}

			headerValid, _ := blockHeader.HasMetTargetDifficulty()
			if headerValid { // header is valid if the hash is less than the target
				break miningLoop
			}

			// TODO: remove this when Siggi gets a laptop without a fan...
			// 😂
			time.Sleep(10 * time.Millisecond)

			nonce++
		}
	}
	return &model.MiningSolution{
		Id:       candidate.Id,
		Nonce:    nonce,
		Time:     candidate.Time,
		Coinbase: coinbaseTx.Bytes(),
		Version:  candidate.Version,
	}, nil
	// m.logger.Infof("submitting mining solution: %s", utils.ReverseAndHexEncodeSlice(candidate.Id))
	// err = m.blockAssemblyClient.SubmitMiningSolution(context.Background(), candidate.Id, coinbaseTx.Bytes(), candidate.Time, nonce, 1)
	// if err != nil {
	// 	m.logger.Errorf("Error submitting mining solution: %v", err)
	// }
}
