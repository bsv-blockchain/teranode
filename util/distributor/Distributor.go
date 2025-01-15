package distributor

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/services/propagation"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/tracing"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/libsv/go-bt/v2"
	"github.com/quic-go/quic-go/http3"
)

type Distributor struct {
	logger             ulogger.Logger
	settings           *settings.Settings
	propagationServers map[string]*propagation.Client
	attempts           int32
	backoff            time.Duration
	failureTolerance   int
	useQuic            bool
	quicAddresses      []string
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
		failureTolerance:   50,
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
		return nil, errors.NewServiceError("no propagation server addresses found")
	}

	propagationServers := make(map[string]*propagation.Client)

	for _, address := range addresses {
		pConn, err := util.GetGRPCClient(context.Background(), address, &util.ConnectionOptions{
			MaxRetries: 3,
		}, tSettings)
		if err != nil {
			return nil, errors.NewServiceError("error creating grpc client for propagation server %s", address, err)
		}

		propagationServers[address], err = propagation.NewClient(ctx, logger, tSettings, pConn)
		if err != nil {
			return nil, errors.NewServiceError("error creating client for propagation server %s", address, err)
		}
	}

	return propagationServers, nil
}

func getPropagationServerFromAddress(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, address string) (*propagation.Client, error) {
	pConn, err := util.GetGRPCClient(context.Background(), address, &util.ConnectionOptions{
		MaxRetries: 3,
	}, tSettings)
	if err != nil {
		return nil, errors.NewServiceError("error connecting to propagation server %s", address, err)
	}

	propagationServer, err := propagation.NewClient(ctx, logger, tSettings, pConn)
	if err != nil {
		return nil, errors.NewServiceError("error creating client for propagation server %s", address, err)
	}

	return propagationServer, nil
}

func NewQuicDistributor(logger ulogger.Logger, tSettings *settings.Settings, opts ...Option) (*Distributor, error) {
	var quicAddresses []string

	quicAddresses = tSettings.Propagation.QuicAddresses
	if len(quicAddresses) == 0 {
		return nil, errors.NewConfigurationError("propagation_quicAddresses not set in config")
	}

	waitMsBetweenTxs := tSettings.Coinbase.DistributerWaitTime
	logger.Infof("wait time between txs: %d ms\n", waitMsBetweenTxs)

	tlsConf := &tls.Config{
		//nolint:gosec // G402 (CWE-295): TLS InsecureSkipVerify set true
		InsecureSkipVerify: true,
		NextProtos:         []string{"txblaster2"},
	}

	client := &http.Client{
		Transport: &http3.RoundTripper{
			TLSClientConfig: tlsConf,
		},
	}
	defer client.CloseIdleConnections()

	d := &Distributor{
		logger:             logger,
		propagationServers: nil,
		attempts:           1,
		failureTolerance:   50,
		useQuic:            true,
		quicAddresses:      quicAddresses,
		waitMsBetweenTxs:   waitMsBetweenTxs,
		httpClient:         client,
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
		useQuic:            d.useQuic,
		quicAddresses:      d.quicAddresses,
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
	ctx, stat, deferFn := tracing.StartTracing(ctx, "Distributor:SendTransaction")

	defer deferFn()

	if d.useQuic {
		var err error

		// Write the length of the transaction to buffer
		var buf bytes.Buffer
		//nolint:gosec
		err = binary.Write(&buf, binary.BigEndian, uint32(tx.Size()))
		if err != nil {
			d.logger.Errorf("Error writing transaction length: %v", err)
			return nil, err
		}

		// Write raw transaction to buffer
		_, err = buf.Write(tx.ExtendedBytes())
		if err != nil {
			d.logger.Errorf("Failed to write transaction data: %v", err)
			return nil, err
		}

		for _, qa := range d.quicAddresses {
			qa = fmt.Sprintf("%s/tx", qa)
			// send data
			_, err := d.httpClient.Post(qa, "application/octet-stream", bytes.NewReader(buf.Bytes()))
			if err != nil {
				d.logger.Errorf("Failed to post data: %v", err)
				return nil, err
			}
		}

		time.Sleep(time.Duration(d.waitMsBetweenTxs) * time.Millisecond) //

		return nil, nil
	} else { // use grpc
		var wg sync.WaitGroup

		responseWrapperCh := make(chan *ResponseWrapper, len(d.propagationServers))

		timeout := d.settings.Coinbase.DistributorTimeout

		for addr, propagationServer := range d.propagationServers {
			address := addr // Create a local copy
			propagationServerClient := propagationServer

			wg.Add(1)

			// addr := addr
			go func(address string, propagationServerClient *propagation.Client) {
				start1, stat1, ctx1 := tracing.NewStatFromContext(ctx, addr, stat)
				defer func() {
					wg.Done()
					stat1.AddTime(start1)
				}()

				var err error

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
						d.logger.Warnf("error sending transaction %s to %s failed (deadline %s, duration %s), retrying: %v", tx.TxIDChainHash().String(), address, time.Until(deadline), time.Since(start), err)

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
				errs = append(errs, errors.NewServiceError("address %s", rw.Addr, rw.Error))
				errorCount++
			}
		}

		failurePercentage := float32(errorCount) / float32(len(d.propagationServers)) * 100
		if failurePercentage > float32(d.failureTolerance) {
			return responses, errors.NewProcessingError("error sending transaction %s to %.2f%% of the propagation servers: %v", tx.TxIDChainHash().String(), failurePercentage, errs)
		} else if errorCount > 0 {
			d.logger.Errorf("error(s) distributing transaction %s: %v", tx.TxIDChainHash().String(), errs)
		}

		return responses, nil
	}
}

func (d *Distributor) TriggerBatcher() {
	for _, propagationServer := range d.propagationServers {
		propagationServer.TriggerBatcher()
	}
}
