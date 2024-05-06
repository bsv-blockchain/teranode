package distributor

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/bitcoin-sv/ubsv/services/propagation"
	"github.com/bitcoin-sv/ubsv/services/propagation/propagation_api"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/opentracing/opentracing-go"
	"github.com/ordishs/gocore"
	"github.com/quic-go/quic-go/http3"
)

type Distributor struct {
	logger             ulogger.Logger
	propagationServers map[string]propagation_api.PropagationAPIClient
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

func NewDistributor(logger ulogger.Logger, opts ...Option) (*Distributor, error) {
	propagationServers, err := getPropagationServers()
	if err != nil {
		return nil, err
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

func getPropagationServers() (map[string]propagation_api.PropagationAPIClient, error) {
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

	return propagationServers, nil
}

func NewQuicDistributor(logger ulogger.Logger, opts ...Option) (*Distributor, error) {

	var quicAddresses []string

	quicAddresses, _ = gocore.Config().GetMulti("propagation_quicAddresses", "|")
	if len(quicAddresses) == 0 {
		return nil, fmt.Errorf("propagation_quicAddresses not set in config")
	}

	waitMsBetweenTxs, _ := gocore.Config().GetInt("distributer_wait_time", 0)
	logger.Infof("wait time between txs: %d ms\n", waitMsBetweenTxs)

	tlsConf := &tls.Config{
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
	propagationServers, err := getPropagationServers()
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
	start, stat, ctx := util.StartStatFromContext(ctx, "Distributor:SendTransaction")
	span, spanCtx := opentracing.StartSpanFromContext(ctx, "Distributor:SendTransaction")
	defer func() {
		stat.AddTime(start)
		span.Finish()
	}()

	txBytes := tx.ExtendedBytes()

	if d.useQuic {
		var err error

		// Write the length of the transaction to buffer
		txLength := uint32(len(txBytes))
		var buf bytes.Buffer
		err = binary.Write(&buf, binary.BigEndian, txLength)
		if err != nil {
			d.logger.Errorf("Error writing transaction length: %v", err)
			return nil, err
		}

		// Write raw transaction to buffer
		_, err = buf.Write(txBytes)
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

		for addr, propagationServer := range d.propagationServers {
			a := addr // Create a local copy
			p := propagationServer

			wg.Add(1)

			addr := addr
			go func() {
				start1, stat1, ctx1 := util.NewStatFromContext(spanCtx, addr, stat)
				defer func() {
					wg.Done()
					stat1.AddTime(start1)
				}()

				var retries int32
				backoff := d.backoff

				for {
					timeout, err, _ := gocore.Config().GetDuration("distributor_timeout", 30*time.Second)
					if err != nil {
						d.logger.Fatalf("Invalid timeout format (valid examples 5s, 1m, 100ms, etc) - distributor_timeout : %v", err)
					}
					ctx1, cancel := context.WithTimeout(ctx1, timeout)
					_, err = p.ProcessTransaction(ctx1, &propagation_api.ProcessTransactionRequest{
						Tx: txBytes,
					})
					cancel()

					if err == nil {
						responseWrapperCh <- &ResponseWrapper{
							Addr:     a,
							Retries:  retries,
							Duration: time.Since(start),
						}
						break
					} else {
						if errors.Is(err, propagation.ErrBadRequest) {
							// There is no point retrying a bad transaction
							responseWrapperCh <- &ResponseWrapper{
								Addr:     a,
								Retries:  0,
								Duration: time.Since(start),
								Error:    err,
							}
							break
						}

						d.logger.Errorf("error sending transaction %s to %s: %v", tx.TxIDChainHash().String(), a, err)
						if retries < d.attempts {
							retries++
							time.Sleep(backoff)
							backoff *= 2
						} else {
							responseWrapperCh <- &ResponseWrapper{
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
		errorCount := 0
		var errs []error
		for rw := range responseWrapperCh {
			responses[i] = rw
			i++

			if rw.Error != nil {
				errs = append(errs, fmt.Errorf("%s: %v", rw.Addr, rw.Error))
				errorCount++
			}
		}

		failurePercentage := float32(errorCount) / float32(len(d.propagationServers)) * 100
		if failurePercentage > float32(d.failureTolerance) {
			return responses, errors.Join(propagation.ErrInternal, fmt.Errorf("error sending transaction %s to %.2f%% of the propagation servers: %v", tx.TxIDChainHash().String(), failurePercentage, errs))
		} else if errorCount > 0 {
			d.logger.Errorf("error(s) distributing transaction %s: %v", tx.TxIDChainHash().String(), errs)
		}

		return responses, nil
	}
}
