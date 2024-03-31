package ubsv

import (
	"context"
	"fmt"
	"net/url"
	"sync"

	// "github.com/aerospike/aerospike-client-go/v7"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/stores/utxo/aerospike"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2"
)

type Ubsv struct {
	logger ulogger.Logger
	store  utxostore.Interface
}

func New(logger ulogger.Logger, timeout string, addr string, port int) *Ubsv {
	urlStr := fmt.Sprintf("aerospike://%s:%d/utxostore", addr, port)
	if timeout != "" {
		urlStr += "?timeout=" + timeout
	}
	storeUrl, _ := url.Parse(urlStr)
	store, err := aerospike.New(logger, storeUrl)
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

				if err := s.store.Delete(ctx, tx); err != nil {
					s.logger.Warnf("delete failed: %v\n", err)
				}

				// Store the hash
				if err := s.store.Store(ctx, tx); err != nil {
					s.logger.Errorf("stored failed: %v", err)
					return
				}

				counter++
			}
		}
	}()
}
