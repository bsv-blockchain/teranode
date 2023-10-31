package distributor

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/bitcoin-sv/ubsv/services/propagation/propagation_api"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
)

type Distributor struct {
	logger             utils.Logger
	propagationServers map[string]propagation_api.PropagationAPIClient
	attempts           int
	backoff            time.Duration
	failureTolerance   int
}

type DistributorOption func(*Distributor)

func WithBackoffDuration(t time.Duration) DistributorOption {
	return func(opts *Distributor) {
		opts.backoff = t
	}
}

func WithRetryAttempts(r int) DistributorOption {
	return func(opts *Distributor) {
		opts.attempts = r
	}
}

func WithFailureTolerance(r int) DistributorOption {
	return func(opts *Distributor) {
		opts.failureTolerance = r
	}
}

func GetPropagationGRPCAddresses() []string {
	addresses, _ := gocore.Config().GetMulti("propagation_grpcAddresses", "|")
	return addresses
}

func NewDistributor(logger utils.Logger, opts ...DistributorOption) (*Distributor, error) {
	addresses := GetPropagationGRPCAddresses()

	if len(addresses) == 0 {
		return nil, errors.New("no propagation server addresses found")
	}

	propagationServers := make(map[string]propagation_api.PropagationAPIClient)

	for _, address := range addresses {
		pConn, err := util.GetGRPCClient(context.Background(), address, &util.ConnectionOptions{
			OpenTracing: gocore.Config().GetBool("use_open_tracing", true),
			Prometheus:  gocore.Config().GetBool("use_prometheus_grpc_metrics", true),
			MaxRetries:  3,
		})
		if err != nil {
			return nil, fmt.Errorf("error connecting to propagation server %s: %w", address, err)
		}

		propagationServers[address] = propagation_api.NewPropagationAPIClient(pConn)
	}

	d := &Distributor{
		logger:             logger,
		propagationServers: propagationServers,
		attempts:           1,
		failureTolerance:   50,
	}

	for _, opt := range opts {
		opt(d)
	}

	return d, nil
}

type errorWrapper struct {
	addr string
	err  error
}

func (d *Distributor) SendTransaction(ctx context.Context, tx *bt.Tx) error {
	var wg sync.WaitGroup

	errorWrapperCh := make(chan errorWrapper, len(d.propagationServers))

	for addr, propagationServer := range d.propagationServers {
		a := addr // Create a local copy
		p := propagationServer

		wg.Add(1)

		go func() {
			defer wg.Done()
			attempts := 0
			backoff := d.backoff

			for {
				if _, err := p.ProcessTransaction(ctx, &propagation_api.ProcessTransactionRequest{
					Tx: tx.ExtendedBytes(),
				}); err == nil {
					break
				} else {
					if attempts < d.attempts {
						attempts++
						time.Sleep(backoff)
						backoff *= 2
					} else {
						errorWrapperCh <- errorWrapper{
							addr: a,
							err:  err,
						}
						break
					}
				}
			}
		}()
	}
	wg.Wait()

	close(errorWrapperCh)

	// Read any errors from the channel
	errors := strings.Builder{}
	errorCount := 0

	for errorWrapper := range errorWrapperCh {
		errors.WriteString(fmt.Sprintf("\t%s: %v\n", errorWrapper.addr, errorWrapper.err))
		errorCount++
	}

	if errorCount > 0 {
		d.logger.Debugf("Error(s) distributing transaction %s:\n%s", tx.TxIDChainHash().String(), errors.String())
	} else {
		d.logger.Debugf("Successfully distributed transaction %s", tx.TxIDChainHash().String())
	}

	failurePercentage := float32(errorCount) / float32(len(d.propagationServers)) * 100
	if failurePercentage > float32(d.failureTolerance) {
		return fmt.Errorf("error sending transaction %s to %.2f%% of the propagation servers", tx.TxIDChainHash().String(), failurePercentage)
	}

	return nil
}
