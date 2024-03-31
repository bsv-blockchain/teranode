package ubsv_map

import (
	"context"
	"fmt"
	"github.com/bitcoin-sv/ubsv/stores/utxo/aerospikemap"
	"github.com/bitcoin-sv/ubsv/util"
	"net/url"
	"sync"

	// "github.com/aerospike/aerospike-client-go/v7"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
)

type UbsvMap struct {
	logger ulogger.Logger
	store  utxostore.Interface
}

func New(logger ulogger.Logger, timeout string, addr string, port int, namespace string) *UbsvMap {
	urlStr := fmt.Sprintf("aerospikemap://%s:%d/%s", addr, port, namespace)
	if timeout != "" {
		urlStr += "?timeout=" + timeout
	}
	storeUrl, _ := url.Parse(urlStr)

	store, err := aerospikemap.New(logger, storeUrl)
	if err != nil {
		panic(err)
	}

	return &UbsvMap{
		store:  store,
		logger: logger,
	}
}

func (s *UbsvMap) Storer(ctx context.Context, id int, txCount int, wg *sync.WaitGroup, spenderCh chan *bt.Tx, counterCh chan int) {
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
				// Generate a dummy tx
				tx := bt.NewTx()
				hash := chainhash.HashH([]byte(fmt.Sprintf("%d:%d", id, i)))
				_ = tx.AddOpReturnOutput(hash[:])
				tx.Outputs[0].Satoshis = uint64(1000)

				if err := s.store.Delete(ctx, tx); err != nil {
					s.logger.Warnf("delete failed: %v\n", err)
				}

				// Store the hash
				if err := s.store.Store(ctx, tx); err != nil {
					s.logger.Errorf("stored failed: %v", err)
					return
				}

				counter++

				spenderCh <- tx
			}
		}

		// logger.Infof("Storer finished.")
	}()

}

func (s *UbsvMap) Spender(ctx context.Context, wg *sync.WaitGroup, spenderCh chan *bt.Tx, deleterCh chan *bt.Tx, counterCh chan int) {
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

		for tx := range spenderCh {
			spends := make([]*utxostore.Spend, 0, len(tx.Outputs))
			for idx, out := range tx.Outputs {
				utxoHash, err := util.UTXOHashFromOutput(tx.TxIDChainHash(), out, uint32(idx))
				if err != nil {
					s.logger.Warnf("spend failed: %v\n", err)
					continue
				}

				spends = append(spends, &utxostore.Spend{
					TxID:         tx.TxIDChainHash(),
					Hash:         utxoHash,
					Vout:         uint32(idx),
					SpendingTxID: spendingTxHash,
				})
			}
			err = s.store.Spend(ctx, spends)
			if err != nil {
				s.logger.Warnf("spend failed: %v\n", err)
			}

			counter++

			deleterCh <- tx.Clone()
		}
	}()
}

func (s *UbsvMap) Deleter(ctx context.Context, wg *sync.WaitGroup, deleteCh chan *bt.Tx, counterCh chan int) {
	wg.Add(1)

	go func() {
		defer wg.Done()

		var counter int

		defer func() {
			counterCh <- counter
		}()

		for hash := range deleteCh {
			err := s.store.Delete(ctx, hash)
			if err != nil {
				s.logger.Warnf("delete failed: %v\n", err)
			}

			counter++
		}
	}()
}
