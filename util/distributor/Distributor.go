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
	"github.com/opentracing/opentracing-go"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
)

type Distributor struct {
	logger             utils.Logger
	propagationServers map[string]propagation_api.PropagationAPIClient
	attempts           int32
	backoff            time.Duration
	failureTolerance   int
}

type Option func(*Distributor)

func WithBackoffDuration(t time.Duration) Option {
	return func(opts *Distributor) {
		opts.backoff = t
	}
}

func WithRetryAttempts(r int32) Option {
	return func(opts *Distributor) {
		opts.attempts = r
	}
}

func WithFailureTolerance(r int) Option {
	return func(opts *Distributor) {
		opts.failureTolerance = r
	}
}

func NewDistributor(logger utils.Logger, opts ...Option) (*Distributor, error) {
	addresses, _ := gocore.Config().GetMulti("propagation_grpcAddresses", "|")

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

type ResponseWrapper struct {
	Addr     string        `json:"addr"`
	Duration time.Duration `json:"duration"`
	Retries  int32         `json:"retries"`
	Error    error         `json:"error,omitempty"`
}

func (d *Distributor) GetPropagationGRPCAddresses() []string {
	addresses := make([]string, 0, len(d.propagationServers))
	for addr := range d.propagationServers {
		addresses = append(addresses, addr)
	}

	return addresses
}

func (d *Distributor) SendTransaction(ctx context.Context, tx *bt.Tx) ([]*ResponseWrapper, error) {
	start, stat, ctx := util.StartStatFromContext(ctx, "Distributor:SendTransaction")
	span, spanCtx := opentracing.StartSpanFromContext(ctx, "Distributor:SendTransaction")
	defer func() {
		stat.AddTime(start)
		span.Finish()
	}()

	var wg sync.WaitGroup

	responseWrapperCh := make(chan ResponseWrapper, len(d.propagationServers))

	for addr, propagationServer := range d.propagationServers {
		a := addr // Create a local copy
		p := propagationServer

		wg.Add(1)

		go func() {
			start1, stat1, ctx1 := util.NewStatFromContext(spanCtx, "ProcessTransaction", stat)
			defer func() {
				wg.Done()
				stat1.AddTime(start1)
			}()

			var retries int32
			backoff := d.backoff

			for {
				if _, err := p.ProcessTransaction(ctx1, &propagation_api.ProcessTransactionRequest{
					Tx: tx.ExtendedBytes(),
				}); err == nil {
					responseWrapperCh <- ResponseWrapper{
						Addr:     a,
						Retries:  retries,
						Duration: time.Since(start),
					}
					break
				} else {
					d.logger.Debugf("error sending transaction %s to %s: %v", tx.TxIDChainHash().String(), a, err)
					if retries < d.attempts {
						retries++
						time.Sleep(backoff)
						backoff *= 2
					} else {
						responseWrapperCh <- ResponseWrapper{
							Addr:     a,
							Retries:  retries,
							Duration: time.Since(start),
							Error:    err,
						}
						break
					}
				}
			}
		}()
	}
	wg.Wait()

	close(responseWrapperCh)

	// Read any errors from the channel
	responses := make([]*ResponseWrapper, len(d.propagationServers))
	var i int

	builderErrors := strings.Builder{}
	errorCount := 0

	for rw := range responseWrapperCh {
		responses[i] = &rw
		i++

		if rw.Error != nil {
			builderErrors.WriteString(fmt.Sprintf("\t%s: %v\n", rw.Addr, rw.Error))
			errorCount++
		}
	}

	if errorCount > 0 {
		d.logger.Errorf("error(s) distributing transaction %s:\n%s", tx.TxIDChainHash().String(), builderErrors.String())
	} else {
		d.logger.Debugf("successfully distributed transaction %s", tx.TxIDChainHash().String())
	}

	failurePercentage := float32(errorCount) / float32(len(d.propagationServers)) * 100
	if failurePercentage > float32(d.failureTolerance) {
		return responses, fmt.Errorf("error sending transaction %s to %.2f%% of the propagation servers", tx.TxIDChainHash().String(), failurePercentage)
	}

	return responses, nil
}
