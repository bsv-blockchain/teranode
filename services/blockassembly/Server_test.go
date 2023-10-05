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
	txmetastore "github.com/bitcoin-sv/ubsv/stores/txmeta/memory"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo/memory"
	"github.com/libsv/go-p2p"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/mocktracer"
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

		assert.GreaterOrEqual(t, uint64(1_000_000), ba.blockAssembler.TxCount())
	})
}

func initMockedServer(t *testing.T) (*BlockAssembly, error) {
	memStore := memory.New()
	utxoStore := utxostore.New(true)
	txMetaStore := txmetastore.New()

	opentracing.SetGlobalTracer(mocktracer.New())

	blockchainStoreURL, _ := url.Parse("sqlitememory://")
	blockchainStore, err := blockchainstore.NewStore(p2p.TestLogger{}, blockchainStoreURL)
	if err != nil {
		return nil, err
	}

	blockchainClient, err := blockchain.NewLocalClient(p2p.TestLogger{}, blockchainStore)
	if err != nil {
		return nil, err
	}

	gocore.Config().Set("tx_chan_buffer_size", "1000000")

	ctx := context.Background()
	ba := New(p2p.TestLogger{}, memStore, utxoStore, txMetaStore, memStore, blockchainClient)
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
