package direct

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/aerospike/aerospike-client-go/v7"
	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/bitcoin-sv/ubsv/util/uaerospike"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
)

type Direct struct {
	logger    ulogger.Logger
	client    *uaerospike.Client
	namespace string
	timeout   time.Duration
}

func New(logger ulogger.Logger, timeoutStr string, addr string, port int, namespace string) *Direct {
	policy := aerospike.NewClientPolicy()
	// todo optimize these https://github.com/aerospike/aerospike-client-go/issues/256#issuecomment-479964112
	// todo optimize read policies
	// todo optimize write policies
	policy.MinConnectionsPerNode = 128
	// policy.LimitConnectionsToQueueSize = true
	policy.ConnectionQueueSize = 128
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

	_, _ = client.WarmUp(0)

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
		namespace: namespace,
	}
}

func (s *Direct) Storer(ctx context.Context, id int, txCount int, wg *sync.WaitGroup, spenderCh chan *bt.Tx, counterCh chan int) {
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

				spenderCh <- tx
			}
		}

		// logger.Infof("Stopped storer %d after %d transactions...", id, i)
	}()
}

func (s *Direct) Spender(_ context.Context, wg *sync.WaitGroup, spenderCh chan *bt.Tx, deleterCh chan *bt.Tx, counterCh chan int) {
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

		options := make([]util.AerospikeWritePolicyOptions, 0)

		if s.timeout > 0 {
			options = append(options, util.WithTotalTimeoutWrite(s.timeout))
		}

		policy := util.GetAerospikeWritePolicy(1, 0, options...)
		policy.RecordExistsAction = aerospike.UPDATE_ONLY
		policy.GenerationPolicy = aerospike.EXPECT_GEN_EQUAL
		policy.CommitLevel = aerospike.COMMIT_ALL // strong consistency

		for tx := range spenderCh {
			key, err := aerospike.NewKey(s.namespace, "utxo", tx.TxIDChainHash()[:])
			if err != nil {
				s.logger.Warnf("spend failed to create key: %v\n", err)
			}

			policy.FilterExpression = aerospike.ExpOr(
				// anything below the block height is spendable, including 0
				aerospike.ExpLessEq(aerospike.ExpIntBin("locktime"), aerospike.ExpIntVal(1)),

				aerospike.ExpAnd(
					aerospike.ExpGreaterEq(aerospike.ExpIntBin("locktime"), aerospike.ExpIntVal(500000000)),
					// TODO Note that since the adoption of BIP 113, the time-based nLockTime is compared to the 11-block median
					// time past (the median timestamp of the 11 blocks preceding the block in which the transaction is mined),
					// and not the block time itself.
					aerospike.ExpLessEq(aerospike.ExpIntBin("locktime"), aerospike.ExpIntVal(time.Now().Unix())),
				),
			)

			bin := aerospike.NewBin("txid", spendingTxHash.CloneBytes())
			err = s.client.PutBins(policy, key, bin)
			if err != nil {
				s.logger.Warnf("spend failed to put bins: %v\n", err)

				if errors.Is(err, aerospike.ErrFilteredOut) {
					s.logger.Warnf("spend failed: locktime: %v\n", err)
				}

				if errors.Is(err, aerospike.ErrKeyNotFound) {
					s.logger.Warnf("spend failed: not found: %v\n", err)
				}

				// check whether we had the same value set as before
				value, err := s.client.Get(nil, key, "txid")
				if err != nil {
					s.logger.Warnf("spend failed: check: %v\n", err)
				} else {

					valueBytes, ok := value.Bins["txid"].([]byte)
					if ok && len(valueBytes) == 32 {
						if [32]byte(valueBytes) == *spendingTxHash {
							counter++
						} else {
							_, err := chainhash.NewHash(valueBytes)
							if err != nil {
								s.logger.Warnf("spend failed: bad hash: %v\n", err)
							}
						}
					}
				}
			} else {
				counter++
			}

			deleterCh <- tx
		}
	}()
}

func (s *Direct) Deleter(_ context.Context, wg *sync.WaitGroup, deleteCh chan *bt.Tx, counterCh chan int) {
	wg.Add(1)

	go func() {
		defer wg.Done()

		var counter int

		defer func() {
			counterCh <- counter
		}()

		var options []util.AerospikeWritePolicyOptions

		if s.timeout > 0 {
			options = append(options, util.WithTotalTimeoutWrite(s.timeout))
		}

		policy := util.GetAerospikeWritePolicy(0, 0, options...)
		policy.TotalTimeout = 30 * time.Second
		policy.CommitLevel = aerospike.COMMIT_ALL // strong consistency

		for tx := range deleteCh {

			key, err := aerospike.NewKey(s.namespace, "utxo", tx.TxIDChainHash()[:])
			if err != nil {
				s.logger.Errorf("delete failed to create key: %v", err)
				continue
			}

			if _, err := s.client.Delete(policy, key); err != nil {
				s.logger.Warnf("delete failed: %v\n", err)
			}

			counter++
		}
	}()
}
