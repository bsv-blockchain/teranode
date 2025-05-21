package blockassembly

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/services/blockassembly/blockassembly_api"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// go test -v -tags test_blockassembly ./test/...

func TestServer_Performance(t *testing.T) {
	t.Run("GetMiningCandidate", func(t *testing.T) {
		ba, cancelCtx, err := initMockedServer(t)
		require.NoError(t, err)

		defer cancelCtx()

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

	t.Run("1_million_txs_-_1_by_1", func(t *testing.T) {
		ba, cancelCtx, err := initMockedServer(t)
		require.NoError(t, err)

		defer cancelCtx()

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

		assert.Equal(t, 1_048_576+startingTxCount, ba.TxCount())
		assert.Equal(t, 1025, ba.SubtreeCount())
	})

	var txRequestPool = sync.Pool{
		New: func() any {
			return &blockassembly_api.AddTxRequest{}
		},
	}

	t.Run("1_million_txs_-_1_by_1 with sync pool", func(t *testing.T) {
		ba, cancelCtx, err := initMockedServer(t)
		require.NoError(t, err)

		defer cancelCtx()

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

					requestObject := txRequestPool.Get().(*blockassembly_api.AddTxRequest)
					requestObject.Txid = txid[:]
					requestObject.Fee = i
					requestObject.Size = i
					requestObject.Locktime = 0
					requestObject.Utxos = nil

					_, _ = ba.AddTx(context.Background(), requestObject)

					requestObject.Txid = nil
					requestObject.Utxos = nil
					txRequestPool.Put(requestObject)
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

		assert.Equal(t, 1_048_576+startingTxCount, ba.TxCount())
		assert.Equal(t, 1025, ba.SubtreeCount())
	})

	t.Run("1_million_txs_-_in_batches", func(t *testing.T) {
		ba, cancelCtx, err := initMockedServer(t)
		require.NoError(t, err)

		defer cancelCtx()

		var wg sync.WaitGroup
		startingTxCount := ba.TxCount()

		for n := uint64(0); n < 1_024; n++ {
			wg.Add(1)

			go func() {
				defer wg.Done()

				requests := make([]*blockassembly_api.AddTxRequest, 0, 1_024)

				for i := uint64(0); i < 1_024; i++ {
					txid := chainhash.HashH([]byte(fmt.Sprintf("%d-%d", n, i)))
					requests = append(requests, &blockassembly_api.AddTxRequest{
						Txid:     txid[:],
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
			}()
		}
		wg.Wait()

		for {
			if ba.TxCount() >= startingTxCount+1_048_576 {
				break
			}

			time.Sleep(10 * time.Millisecond)
		}

		assert.Equal(t, 1_048_576+startingTxCount, ba.TxCount())
		assert.Equal(t, 1025, ba.SubtreeCount()) // SubtreeCount now is accumulated with the previous test case
	})

	t.Run("1_million_txs_-_in_batches with sync pool", func(t *testing.T) {
		ba, cancelCtx, err := initMockedServer(t)
		require.NoError(t, err)

		defer cancelCtx()

		var wg sync.WaitGroup
		startingTxCount := ba.TxCount()

		for n := uint64(0); n < 1_024; n++ {
			wg.Add(1)

			go func() {
				defer wg.Done()

				requests := make([]*blockassembly_api.AddTxRequest, 0, 1_024)

				for i := uint64(0); i < 1_024; i++ {
					txid := chainhash.HashH([]byte(fmt.Sprintf("%d-%d", n, i)))

					requestObject := txRequestPool.Get().(*blockassembly_api.AddTxRequest)
					requestObject.Txid = txid[:]
					requestObject.Fee = i
					requestObject.Size = i
					requestObject.Locktime = 0
					requestObject.Utxos = nil

					requests = append(requests, requestObject)
				}

				_, err := ba.AddTxBatch(context.Background(), &blockassembly_api.AddTxBatchRequest{
					TxRequests: requests,
				})
				require.NoError(t, err)

				for _, request := range requests {
					request.Txid = nil
					request.Utxos = nil
					txRequestPool.Put(request)
				}
			}()
		}
		wg.Wait()

		for {
			if ba.TxCount() >= startingTxCount+1_048_576 {
				break
			}

			time.Sleep(10 * time.Millisecond)
		}

		assert.Equal(t, 1_048_576+startingTxCount, ba.TxCount())
		assert.Equal(t, 1025, ba.SubtreeCount()) // SubtreeCount now is accumulated with the previous test case
	})
}
