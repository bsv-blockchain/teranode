package validator

import (
	"context"
	"strings"
	"sync/atomic"
	"time"

	_ "github.com/bitcoin-sv/ubsv/k8sresolver"
	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/validator/validator_api"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/ordishs/gocore"
	"github.com/sercand/kuberesolver/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/resolver"
)

type Client struct {
	client       validator_api.ValidatorAPIClient
	running      *atomic.Bool
	conn         *grpc.ClientConn
	logger       ulogger.Logger
	batchCh      chan *validator_api.ValidateTransactionRequest
	batchSize    int
	batchTimeout int
}

func NewClient(ctx context.Context, logger ulogger.Logger) (*Client, error) {

	grpcResolver, _ := gocore.Config().Get("grpc_resolver")
	if grpcResolver == "k8s" {
		logger.Infof("[VALIDATOR] Using k8s resolver for clients")
		resolver.SetDefaultScheme("k8s")
	} else if grpcResolver == "kubernetes" {
		logger.Infof("[VALIDATOR] Using kubernetes resolver for clients")
		kuberesolver.RegisterInClusterWithSchema("k8s")
	}

	validator_grpcAddress, _ := gocore.Config().Get("validator_grpcAddress")
	conn, err := util.GetGRPCClient(ctx, validator_grpcAddress, &util.ConnectionOptions{
		OpenTracing: gocore.Config().GetBool("use_open_tracing", true),
		Prometheus:  gocore.Config().GetBool("use_prometheus_grpc_metrics", true),
		MaxRetries:  3,
	})
	if err != nil {
		return nil, err
	}

	grpcClient := validator_api.NewValidatorAPIClient(conn)

	sendBatchSize, _ := gocore.Config().GetInt("validator_sendBatchSize", 0)
	sendBatchTimeout, _ := gocore.Config().GetInt("validator_sendBatchTimeout", 100)
	sendBatchWorkers, _ := gocore.Config().GetInt("validator_sendBatchWorkers", 1)

	if sendBatchSize > 0 && sendBatchWorkers <= 0 {
		logger.Fatalf("expecting validator_sendBatchWorkers > 0 when validator_sendBatchSize = %d", sendBatchSize)
	}

	running := atomic.Bool{}
	running.Store(true)

	client := &Client{
		client:       grpcClient,
		logger:       logger,
		running:      &running,
		conn:         conn,
		batchCh:      make(chan *validator_api.ValidateTransactionRequest),
		batchSize:    sendBatchSize,
		batchTimeout: sendBatchTimeout,
	}

	if sendBatchSize > 0 {
		for i := 0; i < sendBatchWorkers; i++ {
			go client.batchWorker(ctx)
		}
	}

	return client, nil
}

func (c *Client) Stop() {
	// TODO
}

func (c *Client) Health(ctx context.Context) (int, string, error) {
	_, err := c.client.HealthGRPC(ctx, &validator_api.EmptyMessage{})
	if err != nil {
		return -1, "Validator", err
	}

	return 0, "Validator", nil
}

func (c *Client) GetBlockHeight() (uint32, error) {
	resp, err := c.client.GetBlockHeight(context.Background(), &validator_api.EmptyMessage{})
	if err != nil {
		return 0, err
	}

	return resp.Height, nil
}

func (c *Client) Validate(ctx context.Context, tx *bt.Tx) error {
	if c.batchSize == 0 {

		if _, err := c.client.ValidateTransaction(ctx, &validator_api.ValidateTransactionRequest{
			TransactionData: tx.ExtendedBytes(),
		}); err != nil {
			return err
		}

	} else {

		/* batch mode */
		c.batchCh <- &validator_api.ValidateTransactionRequest{
			TransactionData: tx.ExtendedBytes(),
		}

	}

	return nil
}

func (c *Client) batchWorker(ctx context.Context) {
	duration := time.Duration(c.batchTimeout) * time.Millisecond
	ringBuffer := make([]*validator_api.ValidateTransactionRequest, c.batchSize)
	i := 0
	for {
		select {
		case req := <-c.batchCh:
			ringBuffer[i] = req
			i++
			if i == c.batchSize {
				c.sendBatchToValidator(ctx, ringBuffer)
				i = 0
			}
		case <-time.After(duration):
			if i > 0 {
				c.sendBatchToValidator(ctx, ringBuffer[:i])
				i = 0
			}
		}
	}
}

func (c *Client) sendBatchToValidator(ctx context.Context, batch []*validator_api.ValidateTransactionRequest) {
	txBatch := &validator_api.ValidateTransactionBatchRequest{
		Transactions: batch,
	}
	resp, err := c.client.ValidateTransactionBatch(ctx, txBatch)
	if err != nil {
		c.logger.Errorf("%v", err)
		return
	}
	if len(resp.Reasons) > 0 {
		c.logger.Errorf("batch send to validator returned %d failed transactions from %d batch", len(resp.Reasons), len(batch))
	}
}

func (c *Client) Subscribe(ctx context.Context, source string) (chan *model.RejectedTxNotification, error) {
	ch := make(chan *model.RejectedTxNotification)

	go func() {
		<-ctx.Done()
		c.logger.Infof("[Asset] context done, closing subscription: %s", source)
		c.running.Store(false)
		err := c.conn.Close()
		if err != nil {
			c.logger.Errorf("[Asset] failed to close connection", err)
		}
	}()

	go func() {
		defer close(ch)

		for c.running.Load() {
			stream, err := c.client.Subscribe(ctx, &validator_api.SubscribeRequest{
				Source: source,
			})
			if err != nil {
				time.Sleep(1 * time.Second)
				continue
			}

			for c.running.Load() {
				resp, err := stream.Recv()
				if err != nil {
					if !strings.Contains(err.Error(), context.Canceled.Error()) {
						c.logger.Errorf("[Validator] failed to receive notification: %v", err)
					}
					time.Sleep(1 * time.Second)
					break
				}

				c.logger.Debugf("[Validator] received notification %+v", resp)
				ch <- &model.RejectedTxNotification{
					TxId:   resp.TxId,
					Reason: resp.Reason,
				}
			}
		}
	}()

	return ch, nil
}
