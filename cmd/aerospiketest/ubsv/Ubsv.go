package ubsv

import (
	"context"
	"fmt"
	"net/url"
	"sync"

	// "github.com/aerospike/aerospike-client-go/v6"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/stores/utxo/aerospike"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/go-utils"
)

type Ubsv struct {
	logger           utils.Logger
	transactionCount int
	store            utxostore.Interface
}

func New(logger utils.Logger, transactionCount int, timeout string) *Ubsv {
	urlStr := "aerospike://192.168.20.214:3000/utxostore"
	if timeout != "" {
		urlStr += "?timeout=" + timeout
	}
	storeUrl, _ := url.Parse(urlStr)
	store, err := aerospike.New(storeUrl)
	if err != nil {
		panic(err)
	}

	return &Ubsv{
		store:            store,
		transactionCount: transactionCount,
		logger:           logger,
	}
}

func (s *Ubsv) Storer(ctx context.Context, id int, wg *sync.WaitGroup, spenderCh chan *chainhash.Hash, counterCh chan int) {
	wg.Add(1)

	go func() {
		defer wg.Done()

		var counter int

		defer func() {
			counterCh <- counter
		}()

		// logger.Infof("Start storer...")
		for i := 0; i < s.transactionCount; i++ {
			select {
			case <-ctx.Done():
				return
			default:
				// Generate a random hash
				hash := chainhash.HashH([]byte(fmt.Sprintf("%d:%d", id, i)))

				_, err := s.store.Delete(ctx, &hash)
				if err != nil {
					s.logger.Warnf("delete failed: %v\n", err)
				}

				// Store the hash
				resp, err := s.store.Store(ctx, &hash, 0)
				if err != nil {
					s.logger.Errorf("stored failed: %v", err)
					return
				}

				if resp.Status != 0 {
					s.logger.Warnf("store failed: status=%d\n", resp.Status)
					return
				}

				counter++

				spenderCh <- &hash
			}
		}

		// logger.Infof("Storer finished.")
	}()

}

func (s *Ubsv) Spender(ctx context.Context, wg *sync.WaitGroup, spenderCh chan *chainhash.Hash, deleterCh chan *chainhash.Hash, counterCh chan int) {
	wg.Add(1)

	spendingTxHash, err := chainhash.NewHashFromStr("5e3bc5947f48cec766090aa17f309fd16259de029dcef5d306b514848c9687c7")
	if err != nil {
		panic(err)
	}

	go func() {
		defer wg.Done()

		var counter int

		defer func() {
			counterCh <- counter
		}()

		for hash := range spenderCh {
			resp, err := s.store.Spend(ctx, hash, spendingTxHash)
			if err != nil {
				s.logger.Warnf("spend failed: %v\n", err)
			}

			if resp != nil && resp.Status != 0 {
				s.logger.Warnf("spend failed: status=%d\n", resp.Status)
			}

			counter++

			deleterCh <- hash
		}
	}()
}

func (s *Ubsv) Deleter(ctx context.Context, wg *sync.WaitGroup, deleteCh chan *chainhash.Hash, counterCh chan int) {
	wg.Add(1)

	go func() {
		defer wg.Done()

		var counter int

		defer func() {
			counterCh <- counter
		}()

		for hash := range deleteCh {
			resp, err := s.store.Delete(ctx, hash)
			if err != nil {
				s.logger.Warnf("delete failed: %v\n", err)
			}

			if resp != nil && resp.Status != 0 {
				s.logger.Warnf("delete failed: status=%d\n", resp.Status)
			}

			counter++
		}
	}()
}
