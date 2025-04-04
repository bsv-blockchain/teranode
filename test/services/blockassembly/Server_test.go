//go:build test_all || test_services || test_blockassembly || test_longlong || debug

package blockassembly

import (
	"context"
	"encoding/binary"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/chaincfg"
	"github.com/bitcoin-sv/teranode/services/blockassembly"
	"github.com/bitcoin-sv/teranode/services/blockassembly/blockassembly_api"
	"github.com/bitcoin-sv/teranode/services/blockchain"
	"github.com/bitcoin-sv/teranode/stores/blob/memory"
	blockchainstore "github.com/bitcoin-sv/teranode/stores/blockchain"
	utxostore "github.com/bitcoin-sv/teranode/stores/utxo/memory"
	"github.com/bitcoin-sv/teranode/tracing"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// go test -v -tags test_blockassembly ./test/...

func TestServer_Performance(t *testing.T) {
	// Create a single blockassembly grpc server for multiple test case here
	// because there are only 1 possible port for ba server
	// We accumulated measurement for each test case and run them sequentially

	ba, err := initMockedServer(t)
	require.NoError(t, err)

	t.Run("GetMiningCandidate", func(t *testing.T) {
		ctx := context.Background()
		miningCandidate, err := ba.GetMiningCandidate(ctx, &blockassembly_api.GetMiningCandidateRequest{})
		require.NoError(t, err)
		require.NotNil(t, miningCandidate)

		assert.NotEmpty(t, miningCandidate.Id)
		assert.NotEmpty(t, miningCandidate.PreviousHash)
		assert.NotEmpty(t, miningCandidate.NBits)
		assert.NotEmpty(t, miningCandidate.Time)
		assert.NotEmpty(t, miningCandidate.Version)
	})

	prevTxCount := uint64(0)
	prevSubtreeCount := 0

	t.Run("1_million_txs_-_1_by_1", func(t *testing.T) {

		startingTxCount := ba.TxCount()
		assert.Equal(t, uint64(1), startingTxCount)

		var wg sync.WaitGroup
		for n := uint64(0); n < 1_024; n++ {
			bytesN := make([]byte, 8)
			binary.LittleEndian.PutUint64(bytesN, n)

			wg.Add(1)

			go func(bytesN []byte) {
				defer wg.Done()

				txid := make([]byte, 32)
				bytesI := make([]byte, 8)

				for i := uint64(0); i < 1_024; i++ {
					// set the n and i as bytes to the txid
					binary.LittleEndian.PutUint64(bytesI, i)

					copy(txid[0:8], bytesN)
					copy(txid[8:16], bytesI)
					_, _ = ba.AddTx(context.Background(), &blockassembly_api.AddTxRequest{
						Txid:     txid,
						Fee:      i,
						Size:     i,
						Locktime: 0,
						Utxos:    nil,
					})
				}
			}(bytesN)
		}

		wg.Wait()

		for {
			if ba.TxCount() >= 1_048_576+startingTxCount {
				break
			}

			time.Sleep(100 * time.Millisecond)
		}

		prevTxCount = ba.TxCount()
		prevSubtreeCount = ba.SubtreeCount()

		assert.Equal(t, uint64(1_048_576+startingTxCount), prevTxCount)
		assert.Equal(t, 33, prevSubtreeCount)
		prevSubtreeCount -= 1 // Decrement 1 so the next test accumulate with it has already decrease 1
	})

	t.Run("1_million_txs_-_in_batches", func(t *testing.T) {
		var wg sync.WaitGroup
		startingTxCount := ba.TxCount()
		

		for n := uint64(0); n < 1_024; n++ {
			bytesN := make([]byte, 8)
			binary.LittleEndian.PutUint64(bytesN, n)

			wg.Add(1)

			go func(bytesN []byte) {
				defer wg.Done()

				txid := make([]byte, 32)
				bytesI := make([]byte, 8)

				requests := make([]*blockassembly_api.AddTxRequest, 0, 1_024)

				for i := uint64(0); i < 1_024; i++ {
					// set the n and i as bytes to the txid
					binary.LittleEndian.PutUint64(bytesI, i)

					copy(txid[0:8], bytesN)
					copy(txid[8:16], bytesI)
					requests = append(requests, &blockassembly_api.AddTxRequest{
						Txid:     txid,
						Fee:      i,
						Size:     i,
						Locktime: 0,
						Utxos:    nil,
					})
				}

				_, err := ba.AddTxBatch(context.Background(), &blockassembly_api.AddTxBatchRequest{
					TxRequests: requests,
				})
				require.NoError(t, err)
			}(bytesN)
		}
		wg.Wait()

		for {
			if ba.TxCount() >= startingTxCount+1_048_576 {
				break
			}
		}

		assert.Equal(t, prevSubtreeCount+33, ba.SubtreeCount())      // SubtreeCount now is accumulated with the previous test case
	})
}

func initMockedServer(t *testing.T) (*blockassembly.BlockAssembly, error) {
	memStore := memory.New()
	utxoStore := utxostore.New(ulogger.TestLogger{})

	tracing.SetGlobalMockTracer()

	tSettings := test.CreateBaseTestSettings()
	tSettings.Policy.BlockMaxSize = 1000000
	tSettings.ChainCfgParams = &chaincfg.MainNetParams

	blockchainStoreURL, _ := url.Parse("sqlitememory://")
	blockchainStore, err := blockchainstore.NewStore(ulogger.TestLogger{}, blockchainStoreURL, tSettings)
	if err != nil {
		return nil, err
	}

	blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, blockchainStore, nil, nil)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	ba := blockassembly.New(ulogger.TestLogger{}, tSettings, memStore, utxoStore, memStore, blockchainClient)

	err = ba.Init(ctx)
	require.NoError(t, err)

	readyCh := make(chan struct{}, 1)

	go func() {
		err = ba.Start(ctx, readyCh)
		if err != nil {
			panic(err)
		}
	}()

	<-readyCh

	return ba, nil
}