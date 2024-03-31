package simple

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/aerospike/aerospike-client-go/v7"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util/uaerospike"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
)

type Simple struct {
	logger    ulogger.Logger
	client    *uaerospike.Client
	namespace string
	timeout   time.Duration
}

func New(logger ulogger.Logger, timeoutStr string, addr string, port int, namespace string) *Simple {
	host := &aerospike.Host{
		Name: addr,
		Port: port,
	}

	var hosts []*aerospike.Host

	hosts = append(hosts, host)

	client, err := uaerospike.NewClientWithPolicyAndHost(nil, hosts...)
	if err != nil {
		panic(err)
	}

	var timeout time.Duration
	if timeoutStr != "" {
		var err error
		if timeout, err = time.ParseDuration(timeoutStr); err != nil {
			logger.Fatalf("invalid timeout: %v", err)
		}
	}

	return &Simple{
		logger:    logger,
		timeout:   timeout,
		client:    client,
		namespace: namespace,
	}
}

func (s *Simple) Storer(ctx context.Context, id int, txCount int, wg *sync.WaitGroup, spenderCh chan *bt.Tx, counterCh chan int) {
	wg.Add(1)

	go func() {
		defer wg.Done()

		var counter int

		defer func() {
			counterCh <- counter
		}()

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

				key, err := aerospike.NewKey(s.namespace, "utxo", tx.TxIDChainHash()[:])
				if err != nil {
					s.logger.Errorf("stored failed to create key: %v", err)
					return
				}

				if _, err := s.client.Delete(nil, key); err != nil {
					s.logger.Errorf("stored failed to delete: %v", err)
				}

				bins := aerospike.BinMap{"txid": hash[:]}

				err = s.client.Put(nil, key, bins)
				if err != nil {
					s.logger.Errorf("stored failed: %v", err)
					return
				}

				counter++

				spenderCh <- tx
			}
		}

		// logger.Infof("Stopped storer %d after %d transactions...", id, i)
	}()
}

func (s *Simple) Spender(_ context.Context, wg *sync.WaitGroup, spenderCh chan *bt.Tx, deleterCh chan *bt.Tx, counterCh chan int) {
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
			// For the simple test, the Spender will GET the record and then PUT it back with a different value
			key, err := aerospike.NewKey(s.namespace, "utxo", tx.TxIDChainHash()[:])
			if err != nil {
				s.logger.Warnf("spend failed to create key: %v\n", err)
			} else {
				value, err := s.client.Get(nil, key, "txid")
				if err != nil {
					s.logger.Warnf("spend - get failed: %v\n", err)
				} else {
					valueBytes, ok := value.Bins["txid"].([]byte)
					if !ok || len(valueBytes) != 32 {
						s.logger.Warnf("spend value failed: bad hash: %v\n", err)
					} else {
						bin := aerospike.NewBin("txid", spendingTxHash.CloneBytes())
						err = s.client.PutBins(nil, key, bin)
						if err != nil {
							s.logger.Warnf("spend failed to put bins: %v\n", err)
						} else {
							counter++
						}
					}
				}
			}

			deleterCh <- tx
		}
	}()
}

func (s *Simple) Deleter(_ context.Context, wg *sync.WaitGroup, deleteCh chan *bt.Tx, counterCh chan int) {
	wg.Add(1)

	go func() {
		defer wg.Done()

		var counter int

		defer func() {
			counterCh <- counter
		}()

		for tx := range deleteCh {

			key, err := aerospike.NewKey(s.namespace, "utxo", tx.TxIDChainHash()[:])
			if err != nil {
				s.logger.Errorf("delete failed to create key: %v", err)
				continue
			}

			if _, err := s.client.Delete(nil, key); err != nil {
				if err != nil {
					s.logger.Warnf("delete failed: %v\n", err)
				}
			}

			counter++
		}
	}()
}
