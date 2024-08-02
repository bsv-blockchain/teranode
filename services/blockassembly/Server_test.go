//go:build fullTest

package blockassembly

import (
	"context"
	"encoding/binary"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/bitcoin-sv/ubsv/services/blockassembly/blockassembly_api"
	"github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/stores/blob/memory"
	blockchainstore "github.com/bitcoin-sv/ubsv/stores/blockchain"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo/memory"
	"github.com/bitcoin-sv/ubsv/tracing"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestServer_Performance(t *testing.T) {
	t.Run("1 million txs", func(t *testing.T) {
		// this test does not work online, it needs to be run locally
		ba, err := initMockedServer(t)
		require.NoError(t, err)

		var wg sync.WaitGroup
		for n := 0; n < 1000; n++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				txid := make([]byte, 32)
				for i := uint64(0); i < 1_000; i++ {
					binary.LittleEndian.PutUint64(txid, i)

					_, _ = ba.AddTx(context.Background(), &blockassembly_api.AddTxRequest{
						Txid:     txid,
						Fee:      i,
						Size:     i,
						Locktime: 0,
						Utxos:    nil,
					})
				}
			}()
		}
		wg.Wait()

		for {
			if ba.blockAssembler.TxCount() == 1_000_000 {
				break
			}
			time.Sleep(1 * time.Millisecond)
		}

		assert.Equal(t, uint64(1_000_000), ba.blockAssembler.TxCount())
		assert.Equal(t, 977, ba.blockAssembler.subtreeProcessor.SubtreeCount())
	})
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

	ctx := context.Background()
	ba := New(ulogger.TestLogger{}, memStore, utxoStore, memStore, blockchainClient)
	err = ba.Init(ctx)
	if err != nil {
		return nil, err
	}

	go func() {
		err = ba.Start(ctx)
		if err != nil {
			panic(err)
		}
	}()

	return ba, nil
}
