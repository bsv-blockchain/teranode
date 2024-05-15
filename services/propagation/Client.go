package propagation

import (
	"context"
	"fmt"
	"time"

	_ "github.com/bitcoin-sv/ubsv/k8sresolver"
	"github.com/bitcoin-sv/ubsv/services/propagation/propagation_api"
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
	tx   *bt.Tx
	done chan error
}

type Client struct {
	client    propagation_api.PropagationAPIClient
	conn      *grpc.ClientConn
	batchSize int
	batchCh   chan []*batchItem
	batcher   batcher.Batcher2[batchItem]
}

func NewClient(ctx context.Context, logger ulogger.Logger, conn ...*grpc.ClientConn) (*Client, error) {

	initResolver(logger)

	var useConn *grpc.ClientConn
	if len(conn) > 0 {
		useConn = conn[0]
	} else {
		localConn, err := getClientConn(ctx)
		if err != nil {
			return nil, err
		}
		useConn = localConn
	}

	client := propagation_api.NewPropagationAPIClient(useConn)

	batchSize, _ := gocore.Config().GetInt("propagation_sendBatchSize", 100)
	sendBatchTimeout, _ := gocore.Config().GetInt("propagation_sendBatchTimeout", 5)

	if batchSize > 0 {
		logger.Infof("Using batch mode to send transactions to block assembly, batches: %d, timeout: %d", batchSize, sendBatchTimeout)
	}

	duration := time.Duration(sendBatchTimeout) * time.Millisecond

	c := &Client{
		client:    client,
		conn:      useConn,
		batchSize: batchSize,
		batchCh:   make(chan []*batchItem),
	}

	sendBatch := func(batch []*batchItem) {
		if err := c.ProcessTransactionBatch(ctx, batch); err != nil {
			logger.Errorf("Error sending batch: %s", err)
		}
	}
	c.batcher = *batcher.New[batchItem](batchSize, duration, sendBatch, true)

	return c, nil
}

func (c *Client) Stop() {
	if c.client != nil && c.conn != nil {
		_ = c.conn.Close()
	}
}

func (c *Client) ProcessTransaction(ctx context.Context, tx *bt.Tx) error {
	if c.batchSize > 0 {
		done := make(chan error)
		c.batcher.Put(&batchItem{tx: tx, done: done})
		err := <-done
		return err
	}

	_, err := c.client.ProcessTransaction(ctx, &propagation_api.ProcessTransactionRequest{
		Tx: tx.ExtendedBytes(),
	})
	if err != nil {
		return err
	}

	return nil
}

func (c *Client) ProcessTransactionBatch(ctx context.Context, batch []*batchItem) error {
	txs := make([][]byte, 0, len(batch))
	for _, tx := range batch {
		txs = append(txs, tx.tx.ExtendedBytes())
	}

	response, err := c.client.ProcessTransactionBatch(ctx, &propagation_api.ProcessTransactionBatchRequest{
		Tx: txs,
	})
	if err != nil {
		for _, tx := range batch {
			tx.done <- err
		}
		return err
	}

	for i, errStr := range response.Error {
		if errStr != "" {
			batch[i].done <- fmt.Errorf(errStr)
		} else {
			batch[i].done <- nil
		}
	}

	return nil
}

func initResolver(logger ulogger.Logger) {
	grpcResolver, _ := gocore.Config().Get("grpc_resolver")
	switch grpcResolver {
	case "k8s":
		logger.Infof("[VALIDATOR] Using k8s resolver for clients")
		resolver.SetDefaultScheme("k8s")
	case "kubernetes":
		logger.Infof("[VALIDATOR] Using kubernetes resolver for clients")
		kuberesolver.RegisterInClusterWithSchema("k8s")
	default:
		logger.Infof("[VALIDATOR] Using default resolver for clients")
	}
}

func getClientConn(ctx context.Context) (*grpc.ClientConn, error) {
	propagation_grpcAddresses, _ := gocore.Config().GetMulti("propagation_grpcAddresses", "|")
	conn, err := util.GetGRPCClient(ctx, propagation_grpcAddresses[0], &util.ConnectionOptions{
		OpenTracing: gocore.Config().GetBool("use_open_tracing", true),
		Prometheus:  gocore.Config().GetBool("use_prometheus_grpc_metrics", true),
		MaxRetries:  3,
	})
	if err != nil {
		return nil, err
	}

	return conn, nil
}
