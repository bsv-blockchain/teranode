//go:build fullTest

package blockassembly

import (
	"context"
	"encoding/binary"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/bitcoin-sv/ubsv/chaincfg"
	"github.com/bitcoin-sv/ubsv/services/blockassembly/blockassembly_api"
	"github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/settings"
	"github.com/bitcoin-sv/ubsv/stores/blob/memory"
	blockchainstore "github.com/bitcoin-sv/ubsv/stores/blockchain"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo/memory"
	"github.com/bitcoin-sv/ubsv/tracing"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestServer_Performance(t *testing.T) {
	t.Run("1 million txs - 1 by 1", func(t *testing.T) {
		util.SkipVeryLongTests(t)

		gocore.Config().Set("initial_merkle_items_per_subtree", "32768")

		// this test does not work online, it needs to be run locally
		ba, err := initMockedServer(t)
		require.NoError(t, err)

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
			if ba.blockAssembler.TxCount() >= 1_048_576 {
				break
			}

			time.Sleep(100 * time.Millisecond)
		}

		assert.Equal(t, uint64(1_048_576), ba.blockAssembler.TxCount())
		assert.Equal(t, 33, ba.blockAssembler.subtreeProcessor.SubtreeCount())
	})

	t.Run("1 million txs - in batches", func(t *testing.T) {
		util.SkipVeryLongTests(t)

		gocore.Config().Set("initial_merkle_items_per_subtree", "32768")

		// this test does not work online, it needs to be run locally
		ba, err := initMockedServer(t)
		require.NoError(t, err)

		var wg sync.WaitGroup

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

				_, err = ba.AddTxBatch(context.Background(), &blockassembly_api.AddTxBatchRequest{
					TxRequests: requests,
				})
				require.NoError(t, err)
			}(bytesN)
		}
		wg.Wait()

		for {
			if ba.blockAssembler.TxCount() >= 1_048_576 {
				break
			}

			time.Sleep(100 * time.Millisecond)
		}

		assert.Equal(t, uint64(1_048_576), ba.blockAssembler.TxCount())
		assert.Equal(t, 33, ba.blockAssembler.subtreeProcessor.SubtreeCount())
	})
}

func TestServer_GetMiningCandidate(t *testing.T) {
	ba, err := initMockedServer(t)
	require.NoError(t, err)

	require.NoError(t, err)

	ctx := context.Background()
	miningCandidate, err := ba.GetMiningCandidate(ctx, &blockassembly_api.EmptyMessage{})
	require.NoError(t, err)
	require.NotNil(t, miningCandidate)

	assert.NotEmpty(t, miningCandidate.Id)
	assert.NotEmpty(t, miningCandidate.PreviousHash)
	assert.NotEmpty(t, miningCandidate.NBits)
	assert.NotEmpty(t, miningCandidate.Time)
	assert.NotEmpty(t, miningCandidate.Version)
}

func initMockedServer(t *testing.T) (*BlockAssembly, error) {
	memStore := memory.New()
	utxoStore := utxostore.New(ulogger.TestLogger{})

	tracing.SetGlobalMockTracer()

	blockchainStoreURL, _ := url.Parse("sqlitememory://")
	blockchainStore, err := blockchainstore.NewStore(ulogger.TestLogger{}, blockchainStoreURL)
	if err != nil {
		return nil, err
	}

	blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, blockchainStore, nil, nil)
	if err != nil {
		return nil, err
	}

	gocore.Config().Set("tx_chan_buffer_size", "1000000")

	tSettings := &settings.Settings{
		ChainCfgParams: &chaincfg.MainNetParams,
		Policy: &settings.PolicySettings{
			BlockMaxSize: 1000000,
		},
	}

	ctx := context.Background()
	ba := New(ulogger.TestLogger{}, tSettings, memStore, utxoStore, memStore, blockchainClient)

	err = ba.Init(ctx)
	require.NoError(t, err)

	go func() {
		err = ba.Start(ctx)
		if err != nil {
			panic(err)
		}
	}()

	return ba, nil
}
