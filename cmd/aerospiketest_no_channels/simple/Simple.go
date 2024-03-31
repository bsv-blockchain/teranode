package simple

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/aerospike/aerospike-client-go/v7"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util/uaerospike"
	"github.com/libsv/go-bt/v2/chainhash"
)

type Simple struct {
	logger    ulogger.Logger
	client    *uaerospike.Client
	namespace string
	timeout   time.Duration
}

func New(logger ulogger.Logger, timeoutStr string, addr string, port int) *Simple {
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
		namespace: "utxostore",
	}
}

func (s *Simple) Work(ctx context.Context, id int, txCount int, wg *sync.WaitGroup, counterCh chan int) {
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

				// // Generate a random hash
				hash := chainhash.HashH([]byte(fmt.Sprintf("%d:%d", id, i)))

				key, err := aerospike.NewKey(s.namespace, "utxo", hash[:])
				if err != nil {
					s.logger.Errorf("failed to create key: %v", err)
					return
				}

				if _, err := s.client.Delete(nil, key); err != nil {
					s.logger.Errorf("failed to delete: %v", err)
				}

				bins := aerospike.BinMap{"txid": hash[:]}

				err = s.client.Put(nil, key, bins)
				if err != nil {
					s.logger.Errorf("failed: %v", err)
					return
				}

				counter++
			}
		}
	}()
}
