// Package rpc implements the Bitcoin JSON-RPC API service for Teranode.
//
// The Distributor.go file contains the transaction distribution component that handles
// reliable propagation of transactions to multiple propagation service instances.
// This component provides fault-tolerant transaction broadcasting with retry logic,
// load balancing, and failure handling to ensure transactions reach the network
// even when individual propagation services are unavailable.
//
// Key Features:
//   - Multi-server transaction distribution with automatic failover
//   - Configurable retry logic with exponential backoff
//   - Failure tolerance allowing partial success scenarios
//   - Performance monitoring and response time tracking
//   - Concurrent transaction submission for improved throughput
//   - HTTP client connection pooling and reuse
//
// The Distributor is used by RPC handlers that need to submit transactions to the
// network, particularly the sendrawtransaction command, providing reliability
// and performance optimization for transaction propagation operations.
package rpc

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/services/propagation"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bitcoin-sv/teranode/util/tracing"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/ordishs/gocore"
)

// Error message constants to avoid duplication
const (
	errMsgCreateGRPCClient = "error creating grpc client for propagation server %s"
	errMsgCreateClient     = "error creating client for propagation server %s"
	errMsgConnecting       = "error connecting to propagation server %s"
	errMsgSendTransaction  = "error sending transaction %s to %s failed (deadline %s, duration %s), retrying: %v"
	errMsgSendToServers    = "error sending transaction %s to %.2f%% of the propagation servers: %v"
	errMsgDistributing     = "error(s) distributing transaction %s: %v"
	errMsgNoServers        = "no propagation server addresses found"
	errMsgAddress          = "address %s"
)

// Distributor manages reliable transaction propagation to multiple propagation service instances.
// It provides fault-tolerant transaction broadcasting with retry logic, load balancing,
// and failure handling to ensure transactions reach the Bitcoin SV network reliably.
//
// The Distributor maintains connections to multiple propagation services and attempts
// to submit transactions to all available instances. It handles partial failures
// gracefully, allowing transactions to succeed as long as a minimum number of
// propagation services accept them.
//
// Key capabilities:
//   - Concurrent submission to multiple propagation services
//   - Configurable retry attempts with exponential backoff
//   - Failure tolerance with configurable success thresholds
//   - Performance monitoring and response time tracking
//   - Connection pooling and reuse for optimal performance
//   - Graceful handling of service unavailability
//
// Thread Safety:
// The Distributor is designed for concurrent use and can safely handle multiple
// simultaneous transaction submissions. Internal state is protected appropriately
// and connections are managed safely across goroutines.
//
// Configuration:
// The Distributor behavior can be customized through functional options including
// retry attempts, backoff duration, failure tolerance, and timing parameters.
type Distributor struct {
	logger             ulogger.Logger
	settings           *settings.Settings
	propagationServers map[string]*propagation.Client
	attempts           int32
	backoff            time.Duration
	failureTolerance   int
	httpClient         *http.Client
	waitMsBetweenTxs   int
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

func NewDistributor(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, opts ...Option) (*Distributor, error) {
	propagationServers, err := getPropagationServers(ctx, logger, tSettings)
	if err != nil {
		return nil, err
	}

	d := &Distributor{
		logger:             logger,
		propagationServers: propagationServers,
		attempts:           1,
		failureTolerance:   tSettings.Coinbase.DistributorFailureTolerance,
		settings:           tSettings,
	}

	for _, opt := range opts {
		opt(d)
	}

	return d, nil
}

func NewDistributorFromAddress(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, address string, opts ...Option) (*Distributor, error) {
	propagationServer, err := getPropagationServerFromAddress(ctx, logger, tSettings, address)
	if err != nil {
		return nil, err
	}

	propagationServers := map[string]*propagation.Client{
		address: propagationServer,
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

func getPropagationServers(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings) (map[string]*propagation.Client, error) {
	addresses := tSettings.Propagation.GRPCAddresses

	if len(addresses) == 0 {
		return nil, errors.NewServiceError(errMsgNoServers)
	}

	propagationServers := make(map[string]*propagation.Client)

	for _, address := range addresses {
		pConn, err := util.GetGRPCClient(context.Background(), address, &util.ConnectionOptions{
			MaxRetries: 3,
		}, tSettings)
		if err != nil {
			return nil, errors.NewServiceError(errMsgCreateGRPCClient, address, err)
		}

		propagationServers[address], err = propagation.NewClient(ctx, logger, tSettings, pConn)
		if err != nil {
			return nil, errors.NewServiceError(errMsgCreateClient, address, err)
		}
	}

	return propagationServers, nil
}

func getPropagationServerFromAddress(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, address string) (*propagation.Client, error) {
	pConn, err := util.GetGRPCClient(context.Background(), address, &util.ConnectionOptions{
		MaxRetries: 3,
	}, tSettings)
	if err != nil {
		return nil, errors.NewServiceError(errMsgConnecting, address, err)
	}

	propagationServer, err := propagation.NewClient(ctx, logger, tSettings, pConn)
	if err != nil {
		return nil, errors.NewServiceError("error creating client for propagation server %s", address, err)
	}

	return propagationServer, nil
}

type ResponseWrapper struct {
	Addr     string        `json:"addr"`
	Duration time.Duration `json:"duration"`
	Retries  int32         `json:"retries"`
	Error    error         `json:"error,omitempty"`
}

// Clone returns a new instance of the Distributor with the same configuration, but with new connections
func (d *Distributor) Clone() (*Distributor, error) {
	propagationServers, err := getPropagationServers(context.Background(), d.logger, d.settings)
	if err != nil {
		return nil, err
	}

	newDist := &Distributor{
		logger:             d.logger,
		propagationServers: propagationServers,
		attempts:           d.attempts,
		backoff:            d.backoff,
		failureTolerance:   d.failureTolerance,
		waitMsBetweenTxs:   d.waitMsBetweenTxs,
		httpClient:         d.httpClient,
	}

	return newDist, nil
}

func (d *Distributor) GetPropagationGRPCAddresses() []string {
	addresses := make([]string, 0, len(d.propagationServers))
	for addr := range d.propagationServers {
		addresses = append(addresses, addr)
	}

	return addresses
}

func (d *Distributor) SendTransaction(ctx context.Context, tx *bt.Tx) ([]*ResponseWrapper, error) {
	start := time.Now()

	stat := gocore.NewStat("Distributor:SendTransaction")

	ctx, span, endSpan := tracing.Tracer("rpc").Start(ctx, "Distributor:SendTransaction")
	defer endSpan()

	var wg sync.WaitGroup

	responseWrapperCh := make(chan *ResponseWrapper, len(d.propagationServers))

	timeout := d.settings.Coinbase.DistributorTimeout

	for addr, propagationServer := range d.propagationServers {
		address := addr // Create a local copy
		propagationServerClient := propagationServer

		wg.Add(1)

		// addr := addr
		go func(address string, propagationServerClient *propagation.Client) {
			var err error

			ctx, _, endSpan1 := tracing.Tracer("rpc").Start(ctx, "Distributor:SendTransaction")
			defer endSpan1(err)

			start1, stat1, ctx1 := tracing.NewStatFromContext(ctx, addr, stat)
			defer func() {
				wg.Done()
				stat1.AddTime(start1)
			}()

			var retries int32

			backoff := d.backoff

			for {
				ctx1, cancel := context.WithTimeout(ctx1, timeout)
				err = propagationServerClient.ProcessTransaction(ctx1, tx)

				cancel()

				if err == nil {
					responseWrapperCh <- &ResponseWrapper{
						Addr:     address,
						Retries:  retries,
						Duration: time.Since(start),
					}

					break
				} else {
					if errors.Is(err, errors.ErrTxInvalid) {
						// There is no point retrying a bad transaction
						responseWrapperCh <- &ResponseWrapper{
							Addr:     address,
							Retries:  0,
							Duration: time.Since(start),
							Error:    err,
						}

						break
					}

					deadline, _ := ctx1.Deadline()
					d.logger.Warnf(errMsgSendTransaction, tx.TxIDChainHash().String(), address, time.Until(deadline), time.Since(start), err)

					if retries < d.attempts {
						retries++

						time.Sleep(backoff)

						backoff *= 2
					} else {
						responseWrapperCh <- &ResponseWrapper{
							Addr:     address,
							Retries:  retries,
							Duration: time.Since(start),
							Error:    err,
						}

						break
					}
				}
			}
		}(address, propagationServerClient)
	}

	wg.Wait()

	close(responseWrapperCh)

	// Read any errors from the channel
	responses := make([]*ResponseWrapper, len(d.propagationServers))

	var i int

	errorCount := 0

	var errs []error

	for rw := range responseWrapperCh {
		responses[i] = rw
		i++

		if rw.Error != nil {
			errs = append(errs, errors.NewServiceError(errMsgAddress, rw.Addr, rw.Error))
			errorCount++
		}
	}

	failurePercentage := float32(errorCount) / float32(len(d.propagationServers)) * 100
	if failurePercentage > float32(d.failureTolerance) || errorCount == len(d.propagationServers) {
		err := errors.NewProcessingError(errMsgSendToServers, tx.TxIDChainHash().String(), failurePercentage, errs)
		span.RecordError(err)

		return responses, err
	} else if errorCount > 0 {
		d.logger.Errorf(errMsgDistributing, tx.TxIDChainHash().String(), errs)
	}

	return responses, nil
}

func (d *Distributor) TriggerBatcher() {
	for _, propagationServer := range d.propagationServers {
		propagationServer.TriggerBatcher()
	}
}
