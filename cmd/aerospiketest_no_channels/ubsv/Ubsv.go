package ubsv

import (
	"context"
	"fmt"
	"net/url"
	"sync"

	// "github.com/aerospike/aerospike-client-go/v6"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/stores/utxo/aerospike"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/ordishs/go-utils"
)

type Ubsv struct {
	logger utils.Logger
	store  utxostore.Interface
}

func New(logger utils.Logger, timeout string, addr string, port int) *Ubsv {
	urlStr := fmt.Sprintf("aerospike://%s:%d/utxostore", addr, port)
	if timeout != "" {
		urlStr += "?timeout=" + timeout
	}
	storeUrl, _ := url.Parse(urlStr)
	store, err := aerospike.New(storeUrl)
	if err != nil {
		panic(err)
	}

	return &Ubsv{
		store:  store,
		logger: logger,
	}
}

func (s *Ubsv) Work(ctx context.Context, id int, txCount int, wg *sync.WaitGroup, counterCh chan int) {
	wg.Add(1)

	go func() {
		defer wg.Done()

		var counter int

		defer func() {
			counterCh <- counter
		}()

		// logger.Infof("Start storer...")
		for i := 0; i < txCount; i++ {
			select {
			case <-ctx.Done():
				return
			default:
				// Generate a random hash
				tx := bt.NewTx()
				_ = tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", uint64(1000+i))
				hash, err := util.UTXOHashFromOutput(tx.TxIDChainHash(), tx.Outputs[0], 0)
				if err != nil {
					s.logger.Warnf("hash failed: %v\n", err)
				}

				err = s.store.Delete(ctx, &utxostore.Spend{
					TxID: hash,
					Vout: 0,
					Hash: hash,
				})
				if err != nil {
					s.logger.Warnf("delete failed: %v\n", err)
				}

				// Store the hash
				err = s.store.Store(ctx, tx)
				if err != nil {
					s.logger.Errorf("stored failed: %v", err)
					return
				}

				counter++
			}
		}
	}()
}
