package direct

import (
	"context"
	"fmt"
	"math"
	"sync"

	"github.com/aerospike/aerospike-client-go/v6"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/go-utils"
)

type Direct struct {
	logger           utils.Logger
	client           *aerospike.Client
	namespace        string
	transactionCount int
}

func New(logger utils.Logger, transactionCount int) *Direct {
	policy := aerospike.NewClientPolicy()
	// todo optimize these https://github.com/aerospike/aerospike-client-go/issues/256#issuecomment-479964112
	// todo optimize read policies
	// todo optimize write policies
	policy.LimitConnectionsToQueueSize = false
	policy.ConnectionQueueSize = 1024
	policy.MaxErrorRate = 0

	host := &aerospike.Host{
		Name: "192.168.20.214",
		Port: 3000,
	}

	var hosts []*aerospike.Host

	hosts = append(hosts, host)

	client, err := aerospike.NewClientWithPolicyAndHost(policy, hosts...)
	if err != nil {
		panic(err)
	}

	return &Direct{
		logger:           logger,
		transactionCount: transactionCount,
		client:           client,
		namespace:        "utxostore",
	}
}

func (s *Direct) Storer(ctx context.Context, id int, wg *sync.WaitGroup, spenderCh chan *chainhash.Hash, counterCh chan int) {
	wg.Add(1)

	go func() {
		defer wg.Done()

		var counter int

		defer func() {
			counterCh <- counter
		}()

		// logger.Infof("Start storer %d...", id)

		policy := util.GetAerospikeWritePolicy(0, math.MaxUint32)
		// policy.RecordExistsAction = aerospike.CREATE_ONLY
		policy.CommitLevel = aerospike.COMMIT_ALL // strong consistency
		policy.SendKey = true

		for i := 0; i < s.transactionCount; i++ {
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

				bins := []*aerospike.Bin{
					aerospike.NewBin("locktime", 0),
				}

				err = s.client.PutBins(policy, key, bins...)
				if err != nil {
					s.logger.Errorf("stored failed: %v", err)
					return
				}

				counter++

				spenderCh <- &hash
			}
		}

		// logger.Infof("Stopped storer %d after %d transactions...", id, i)
	}()
}

func (s *Direct) Spender(_ context.Context, wg *sync.WaitGroup, spenderCh chan *chainhash.Hash, deleterCh chan *chainhash.Hash, counterCh chan int) {
	wg.Add(1)

	go func() {
		defer wg.Done()

		var counter int

		defer func() {
			counterCh <- counter
		}()

		for hash := range spenderCh {
			counter++
			// Do nothing but delete the hash
			deleterCh <- hash
		}
	}()
}

func (s *Direct) Deleter(_ context.Context, wg *sync.WaitGroup, deleteCh chan *chainhash.Hash, counterCh chan int) {
	wg.Add(1)

	go func() {
		defer wg.Done()

		var counter int

		defer func() {
			counterCh <- counter
		}()

		policy := util.GetAerospikeWritePolicy(0, 0)
		policy.CommitLevel = aerospike.COMMIT_ALL // strong consistency

		for hash := range deleteCh {

			key, err := aerospike.NewKey(s.namespace, "utxo", hash[:])
			if err != nil {
				s.logger.Errorf("delete failed to create key: %v", err)
				continue
			}

			if _, err := s.client.Delete(policy, key); err != nil {
				if err != nil {
					s.logger.Warnf("delete failed: %v\n", err)
				}
			}

			counter++
		}
	}()
}
