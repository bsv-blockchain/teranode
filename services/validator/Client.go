package validator

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/bitcoin-sv/ubsv/errors"
	_ "github.com/bitcoin-sv/ubsv/k8sresolver"
	"github.com/bitcoin-sv/ubsv/services/validator/validator_api"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	batcher "github.com/bitcoin-sv/ubsv/util/batcher_temp"
	"github.com/libsv/go-bt/v2"
	"github.com/ordishs/gocore"
	"github.com/sercand/kuberesolver/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/resolver"
)

type batchItem struct {
	req  *validator_api.ValidateTransactionRequest
	done chan error
}
type Client struct {
	client       validator_api.ValidatorAPIClient
	running      *atomic.Bool
	conn         *grpc.ClientConn
	logger       ulogger.Logger
	batchCh      chan *batchItem
	batchSize    int
	batchTimeout int
	batcher      batcher.Batcher2[batchItem]
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
		MaxRetries: 3,
	})
	if err != nil {
		return nil, err
	}

	grpcClient := validator_api.NewValidatorAPIClient(conn)

	sendBatchSize, _ := gocore.Config().GetInt("validator_sendBatchSize", 0)
	sendBatchTimeout, _ := gocore.Config().GetInt("validator_sendBatchTimeout", 100)

	running := atomic.Bool{}
	running.Store(true)

	client := &Client{
		client:       grpcClient,
		logger:       logger,
		running:      &running,
		conn:         conn,
		batchCh:      make(chan *batchItem),
		batchSize:    sendBatchSize,
		batchTimeout: sendBatchTimeout,
	}

	if sendBatchSize > 0 {
		sendBatch := func(batch []*batchItem) {
			client.sendBatchToValidator(ctx, batch)
		}
		duration := time.Duration(sendBatchTimeout) * time.Millisecond
		client.batcher = *batcher.New[batchItem](sendBatchSize, duration, sendBatch, true)
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

func (c *Client) GetBlockHeight() uint32 {
	resp, err := c.client.GetBlockHeight(context.Background(), &validator_api.EmptyMessage{})
	if err != nil {
		return 0
	}

	return resp.Height
}

func (c *Client) GetMedianBlockTime() uint32 {
	resp, err := c.client.GetMedianBlockTime(context.Background(), &validator_api.EmptyMessage{})
	if err != nil {
		return 0
	}

	return resp.MedianTime
}

func (c *Client) Validate(ctx context.Context, tx *bt.Tx, blockHeight uint32) error {
	if c.batchSize == 0 {
		if _, err := c.client.ValidateTransaction(ctx, &validator_api.ValidateTransactionRequest{
			TransactionData: tx.ExtendedBytes(),
			BlockHeight:     blockHeight,
		}); err != nil {
			return errors.UnwrapGRPC(err)
		}
	} else {
		doneCh := make(chan error)
		/* batch mode */
		c.batchCh <- &batchItem{
			req: &validator_api.ValidateTransactionRequest{
				TransactionData: tx.ExtendedBytes(),
				BlockHeight:     blockHeight,
			},
			done: doneCh,
		}

		return <-doneCh
	}

	return nil
}

func (c *Client) sendBatchToValidator(ctx context.Context, batch []*batchItem) {
	requests := make([]*validator_api.ValidateTransactionRequest, 0, len(batch))
	for _, item := range batch {
		requests = append(requests, item.req)
	}
	txBatch := &validator_api.ValidateTransactionBatchRequest{
		Transactions: requests,
	}

	resp, err := c.client.ValidateTransactionBatch(ctx, txBatch)
	if err != nil {
		c.logger.Errorf("%v", err)

		for _, item := range batch {
			item.done <- err
		}

		return
	}

	reasonsLength := len(resp.Reasons)

	if reasonsLength > 0 {
		c.logger.Errorf("batch send to validator returned %d failed transactions from %d batch", len(resp.Reasons), len(batch))
	}

	for i, item := range batch {
		if reasonsLength > 0 && resp.Reasons[i].Reason != "" {
			item.done <- errors.NewError(resp.Reasons[i].Reason)
		} else {
			item.done <- nil
		}
	}
}
