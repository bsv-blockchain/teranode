package propagation

import (
	"context"
	"time"

	"github.com/bitcoin-sv/ubsv/services/propagation/propagation_api"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"google.golang.org/grpc"
)

type timing struct {
	total time.Duration
	count int
}

type transaction struct {
	txBytes []byte
	errCh   chan error
}

type StreamingClient struct {
	logger        ulogger.Logger
	transactionCh chan *transaction
	timingsCh     chan chan timing
	conn          *grpc.ClientConn
	stream        propagation_api.PropagationAPI_ProcessTransactionStreamClient
	totalTime     time.Duration
	count         int
	testMode      bool
}

func NewStreamingClient(ctx context.Context, logger ulogger.Logger, bufferSize int, testMode ...bool) (*StreamingClient, error) {
	sc := &StreamingClient{
		logger:        logger,
		transactionCh: make(chan *transaction, bufferSize),
		timingsCh:     make(chan chan timing),
	}

	if len(testMode) > 0 {
		sc.testMode = testMode[0]
	}

	initResolver(logger)

	go sc.handler(ctx)

	return sc, nil
}

func (sc *StreamingClient) handler(ctx context.Context) {
	defer sc.closeResources()

	for {
		select {
		case <-ctx.Done():
			return

		case getTimeCh := <-sc.timingsCh:
			getTimeCh <- timing{
				total: sc.totalTime,
				count: sc.count,
			}

		case tx := <-sc.transactionCh:
			if sc.testMode {
				tx.errCh <- nil
				continue
			}

			if sc.stream == nil {
				if err := sc.initStream(ctx); err != nil {
					tx.errCh <- err
					return
				}
			}

			if err := sc.stream.Send(&propagation_api.ProcessTransactionRequest{
				Tx: tx.txBytes,
			}); err != nil {
				tx.errCh <- err
				return
			}

			if _, err := sc.stream.Recv(); err != nil {
				tx.errCh <- err
				return
			}

			tx.errCh <- nil

		case <-time.After(10 * time.Second):
			sc.closeResources()
		}
	}
}

func (sc *StreamingClient) closeResources() {
	if sc.stream != nil {
		_ = sc.stream.CloseSend()
	}
	if sc.conn != nil {
		_ = sc.conn.Close()
	}
	sc.stream = nil
	sc.conn = nil
}

func (sc *StreamingClient) ProcessTransaction(txBytes []byte) error {
	start := time.Now()

	errCh := make(chan error)

	sc.transactionCh <- &transaction{
		txBytes: txBytes,
		errCh:   errCh,
	}

	err := <-errCh

	sc.totalTime += time.Since(start)
	sc.count++

	return err
}

func (sc *StreamingClient) ProcessTransactionBatch(txs [][]byte) error {
	for _, tx := range txs {
		if err := sc.ProcessTransaction(tx); err != nil {
			return err
		}
	}

	return nil
}

func (sc *StreamingClient) GetTimings() (time.Duration, int) {
	ch := make(chan timing)
	sc.timingsCh <- ch

	timings := <-ch

	return timings.total, timings.count
}

func (sc *StreamingClient) initStream(ctx context.Context) error {
	var err error

	client, _, err := getClientConn(ctx)
	if err != nil {
		return err
	}

	sc.stream, err = client.ProcessTransactionStream(ctx)
	if err != nil {
		return err
	}
	return nil
}
