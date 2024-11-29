//go:build manual_tests

package miner

import (
	"context"
	"testing"
	"time"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

func Test_mine(t *testing.T) {
	t.Run("mine", func(t *testing.T) {
		ctx, ctxCancel := context.WithCancel(context.Background())

		// cancel context after 10 seconds
		go func() {
			time.Sleep(10 * time.Second)
			ctxCancel()
		}()

		hash := chainhash.Hash{}

		nbits, err := model.NewNBitFromString("1d00ffff")
		require.NoError(t, err)

		miner := Miner{
			coinbaseSig:  "test",
			coinbaseAddr: "mgqipciCS56nCYSjB1vTcDGskN82yxfo1G",
		}

		// mine
		_, rounds, err := miner.tryMining(ctx, &model.MiningCandidate{
			Id:            []byte("test"),
			PreviousHash:  hash.CloneBytes(),
			CoinbaseValue: 123,
			Version:       2,
			NBits:         nbits.CloneBytes(),
			//nolint:gosec
			Time:                uint32(time.Now().Unix()),
			Height:              123,
			MerkleProof:         [][]byte{hash.CloneBytes()},
			SizeWithoutCoinbase: 123,
		}, 12)
		t.Logf("Rounds: %d", rounds)
		require.NoError(t, err)
	})
}
