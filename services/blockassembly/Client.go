package blockassembly

import (
	"context"
	"time"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/blockassembly/blockassembly_api"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	batcher "github.com/bitcoin-sv/ubsv/util/batcher_temp"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
)

type batchItem struct {
	req  *blockassembly_api.AddTxRequest
	done chan error
}

type Client struct {
	client    blockassembly_api.BlockAssemblyAPIClient
	logger    ulogger.Logger
	batchSize int
	batchCh   chan []*batchItem
	batcher   batcher.Batcher2[batchItem]
}

func NewClient(ctx context.Context, logger ulogger.Logger) *Client {
	blockAssemblyGrpcAddress, ok := gocore.Config().Get("blockassembly_grpcAddress")
	if !ok {
		panic("no blockassembly_grpcAddress setting found")
	}

	baConn, err := util.GetGRPCClient(ctx, blockAssemblyGrpcAddress, &util.ConnectionOptions{
		OpenTracing: gocore.Config().GetBool("use_open_tracing", true),
		Prometheus:  gocore.Config().GetBool("use_prometheus_grpc_metrics", true),
		MaxRetries:  3,
	})
	if err != nil {
		panic(err)
	}

	batchSize, _ := gocore.Config().GetInt("blockassembly_sendBatchSize", 0)
	sendBatchTimeout, _ := gocore.Config().GetInt("blockassembly_sendBatchTimeout", 100)

	if batchSize > 0 {
		logger.Infof("Using batch mode to send transactions to block assembly, batches: %d, timeout: %d", batchSize, sendBatchTimeout)
	}

	duration := time.Duration(sendBatchTimeout) * time.Millisecond

	client := &Client{
		client:    blockassembly_api.NewBlockAssemblyAPIClient(baConn),
		logger:    logger,
		batchSize: batchSize,
		batchCh:   make(chan []*batchItem),
	}

	sendBatch := func(batch []*batchItem) {
		client.sendBatchToBlockAssembly(ctx, batch)
	}
	client.batcher = *batcher.New[batchItem](batchSize, duration, sendBatch, true)

	return client
}

func NewClientFromAddress(blockAssemblyGrpcAddress string, ctx context.Context, logger ulogger.Logger) *Client {
	// blockAssemblyGrpcAddress, ok := gocore.Config().Get("blockassembly_grpcAddress")
	// if !ok {
	// 	panic("no blockassembly_grpcAddress setting found")
	// }

	baConn, err := util.GetGRPCClient(ctx, blockAssemblyGrpcAddress, &util.ConnectionOptions{
		OpenTracing: gocore.Config().GetBool("use_open_tracing", true),
		Prometheus:  gocore.Config().GetBool("use_prometheus_grpc_metrics", true),
		MaxRetries:  3,
	})
	if err != nil {
		panic(err)
	}

	batchSize, _ := gocore.Config().GetInt("blockassembly_sendBatchSize", 0)
	sendBatchTimeout, _ := gocore.Config().GetInt("blockassembly_sendBatchTimeout", 100)

	if batchSize > 0 {
		logger.Infof("Using batch mode to send transactions to block assembly, batches: %d, timeout: %d", batchSize, sendBatchTimeout)
	}

	duration := time.Duration(sendBatchTimeout) * time.Millisecond

	client := &Client{
		client:    blockassembly_api.NewBlockAssemblyAPIClient(baConn),
		logger:    logger,
		batchSize: batchSize,
		batchCh:   make(chan []*batchItem),
	}

	sendBatch := func(batch []*batchItem) {
		client.sendBatchToBlockAssembly(ctx, batch)
	}
	client.batcher = *batcher.New[batchItem](batchSize, duration, sendBatch, true)

	return client
}

func (s *Client) Store(ctx context.Context, hash *chainhash.Hash, fee, size uint64) (bool, error) {
	req := &blockassembly_api.AddTxRequest{
		Txid: hash[:],
		Fee:  fee,
		Size: size,
	}

	if s.batchSize == 0 {
		if _, err := s.client.AddTx(ctx, req); err != nil {
			return false, err
		}
	} else {
		/* batch mode */
		done := make(chan error)
		s.batcher.Put(&batchItem{
			req:  req,
			done: done,
		})
		err := <-done
		if err != nil {
			return false, err
		}
	}

	return true, nil
}

func (s *Client) RemoveTx(ctx context.Context, hash *chainhash.Hash) error {
	_, err := s.client.RemoveTx(ctx, &blockassembly_api.RemoveTxRequest{
		Txid: hash[:],
	})

	return err
}

func (s *Client) GetMiningCandidate(ctx context.Context) (*model.MiningCandidate, error) {
	req := &blockassembly_api.EmptyMessage{}

	res, err := s.client.GetMiningCandidate(ctx, req)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (s *Client) SubmitMiningSolution(ctx context.Context, solution *model.MiningSolution) error {
	_, err := s.client.SubmitMiningSolution(ctx, &blockassembly_api.SubmitMiningSolutionRequest{
		Id:         solution.Id,
		Nonce:      solution.Nonce,
		CoinbaseTx: solution.Coinbase,
		Time:       solution.Time,
		Version:    solution.Version,
	})
	return err
}

func (s *Client) sendBatchToBlockAssembly(ctx context.Context, batch []*batchItem) {
	txRequests := make([]*blockassembly_api.AddTxRequest, len(batch))
	for i, item := range batch {
		txRequests[i] = item.req
	}

	txBatch := &blockassembly_api.AddTxBatchRequest{
		TxRequests: txRequests,
	}

	_, err := s.client.AddTxBatch(ctx, txBatch)
	if err != nil {
		s.logger.Errorf("%v", err)
		for _, item := range batch {
			item.done <- err
		}
		return
	}

	for _, item := range batch {
		item.done <- nil
	}
}

func (s *Client) DeDuplicateBlockAssembly(_ context.Context) error {
	_, err := s.client.DeDuplicateBlockAssembly(context.Background(), &blockassembly_api.EmptyMessage{})
	return err
}

func (s *Client) ResetBlockAssembly(_ context.Context) error {
	_, err := s.client.ResetBlockAssembly(context.Background(), &blockassembly_api.EmptyMessage{})
	return err
}

func (s *Client) GetBlockAssemblyState(ctx context.Context) (*blockassembly_api.StateMessage, error) {
	return s.client.GetBlockAssemblyState(ctx, &blockassembly_api.EmptyMessage{})
}
