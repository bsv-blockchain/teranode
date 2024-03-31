package direct

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/aerospike/aerospike-client-go/v7"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/bitcoin-sv/ubsv/util/uaerospike"
	"github.com/libsv/go-bt/v2/chainhash"
)

type Direct struct {
	logger    ulogger.Logger
	client    *uaerospike.Client
	namespace string
	timeout   time.Duration
}

func New(logger ulogger.Logger, timeoutStr string, addr string, port int) *Direct {
	policy := aerospike.NewClientPolicy()
	// todo optimize these https://github.com/aerospike/aerospike-client-go/issues/256#issuecomment-479964112
	// todo optimize read policies
	// todo optimize write policies
	policy.LimitConnectionsToQueueSize = true
	policy.ConnectionQueueSize = 10240
	// policy.MaxErrorRate = 0

	host := &aerospike.Host{
		Name: addr,
		Port: port,
	}

	var hosts []*aerospike.Host

	hosts = append(hosts, host)

	client, err := uaerospike.NewClientWithPolicyAndHost(policy, hosts...)
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

	return &Direct{
		logger:    logger,
		timeout:   timeout,
		client:    client,
		namespace: "utxostore",
	}
}

func (s *Direct) Work(ctx context.Context, id int, txCount int, wg *sync.WaitGroup, counterCh chan int) {
	wg.Add(1)

	go func() {
		defer wg.Done()

		var counter int

		defer func() {
			counterCh <- counter
		}()

		// logger.Infof("Start storer %d...", id)
		var options []util.AerospikeWritePolicyOptions

		if s.timeout > 0 {
			options = append(options, util.WithTotalTimeoutWrite(s.timeout))
		}

		policy := util.GetAerospikeWritePolicy(0, math.MaxUint32, options...)
		policy.RecordExistsAction = aerospike.CREATE_ONLY
		policy.CommitLevel = aerospike.COMMIT_ALL // strong consistency
		policy.SendKey = true

		for i := 0; i < txCount; i++ {
			select {
			case <-ctx.Done():
				return
			default:

				// Generate a random hash
				hash := chainhash.HashH([]byte(fmt.Sprintf("%d:%d", id, i)))

				key, err := aerospike.NewKey(s.namespace, "utxo", hash[:])
				if err != nil {
					s.logger.Errorf("stored failed to create key: %v", err)
					return
				}

				if _, err := s.client.Delete(policy, key); err != nil {
					s.logger.Errorf("stored failed to delete: %v", err)
				}

				bins := []*aerospike.Bin{
					aerospike.NewBin("locktime", 0),
				}

				err = s.client.PutBins(policy, key, bins...)
				if err != nil {
					s.logger.Errorf("stored failed: %v", err)
					return
				}

				counter++
			}
		}
	}()
}
