package blockassembly

import (
	"context"
	"time"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/blockassembly/blockassembly_api"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/go-utils/batcher"
	"github.com/ordishs/gocore"
)

type Client struct {
	client        blockassembly_api.BlockAssemblyAPIClient
	frpcClient    *blockassembly_api.Client
	frpcConnected bool
	logger        ulogger.Logger
	batchSize     int
	batchCh       chan []*blockassembly_api.AddTxRequest
	batcher       batcher.Batcher[blockassembly_api.AddTxRequest]
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
	sendBatchWorkers, _ := gocore.Config().GetInt("blockassembly_sendBatchWorkers", 1)
	sendBatchRPCTimeoutSeconds, _ := gocore.Config().GetInt("blockassembly_sendBatchRPCTimeoutSeconds", 5)
	sendBatchRPCTimeout := time.Duration(sendBatchRPCTimeoutSeconds) * time.Second

	if batchSize > 0 && sendBatchWorkers <= 0 {
		logger.Fatalf("expecting blockassembly_sendBatchWorkers > 0 when blockassembly_sendBatchSize = %d", batchSize)
	}
	if batchSize > 0 {
		logger.Infof("Using batch mode to send transactions to block assembly, batches: %d, workers: %d, timeout: %d", batchSize, sendBatchWorkers, sendBatchTimeout)
	}

	duration := time.Duration(sendBatchTimeout) * time.Millisecond
	batchCh := make(chan []*blockassembly_api.AddTxRequest)
	sendBatch := func(batch []*blockassembly_api.AddTxRequest) {
		batchCh <- batch
	}
	batcherInstance := *batcher.New[blockassembly_api.AddTxRequest](batchSize, duration, sendBatch, false)

	client := &Client{
		client:    blockassembly_api.NewBlockAssemblyAPIClient(baConn),
		logger:    logger,
		batchSize: batchSize,
		batchCh:   batchCh,
		batcher:   batcherInstance,
	}

	client.NewFRPCClient()

	if batchSize > 0 {
		for i := 0; i < sendBatchWorkers; i++ {
			go client.batchWorker(ctx, sendBatchRPCTimeout)
		}
	}

	return client
}

func (s *Client) NewFRPCClient() {
	_, ok := gocore.Config().Get("blockassembly_frpcAddress")
	if !ok {
		return
	}
	frpcClient, err := blockassembly_api.NewClient(nil, nil)
	if err != nil {
		s.logger.Fatalf("Error creating new fRPC client in blockassembly: %s", err)
	}
	s.frpcClient = frpcClient
	s.frpcConnected = false
}

func (s *Client) connectFRPC(ctx context.Context) {
	if s.frpcConnected {
		return
	}

	blockAssemblyFRPCAddress, ok := gocore.Config().Get("blockassembly_frpcAddress")
	if !ok {
		return
	}

	connected := false
	maxRetries := 5
	retryInterval := 5 * time.Second

	for i := 0; i < maxRetries; i++ {
		s.logger.Infof("Attempting to create fRPC connection to blockassembly, attempt %d", i+1)

		err := s.frpcClient.Connect(blockAssemblyFRPCAddress)
		if err != nil {
			s.logger.Infof("Error connecting to fRPC server in blockassembly: %s", err)
			if i+1 == maxRetries {
				break
			}
			time.Sleep(retryInterval)
			retryInterval *= 2
		} else {
			s.logger.Infof("Connected to blockassembly fRPC server")
			connected = true
			break
		}
	}

	if !connected {
		s.logger.Fatalf("Failed to connect to blockassembly fRPC server after %d attempts", maxRetries)
	}

	s.frpcConnected = true

	/* listen for close channel and reconnect */
	s.logger.Infof("Listening for close channel on fRPC client")
	go func() {
		<-s.frpcClient.CloseChannel()
		s.logger.Infof("fRPC client close channel received, reconnecting...")
		s.NewFRPCClient()
	}()
}

func (s *Client) Store(ctx context.Context, hash *chainhash.Hash, fee, size uint64, locktime uint32, utxoHashes []*chainhash.Hash, parentTxHashes []*chainhash.Hash) (bool, error) {
	utxoBytes := make([][]byte, len(utxoHashes))
	for i, h := range utxoHashes {
		utxoBytes[i] = h[:]
	}

	parentBytes := make([][]byte, len(parentTxHashes))
	for i, h := range parentTxHashes {
		parentBytes[i] = h[:]
	}

	req := &blockassembly_api.AddTxRequest{
		Txid:     hash[:],
		Fee:      fee,
		Size:     size,
		Locktime: locktime,
		Utxos:    utxoBytes,
		Parents:  parentBytes,
	}

	if s.batchSize == 0 {
		if s.frpcClient != nil {

			s.connectFRPC(ctx)
			if _, err := s.frpcClient.BlockAssemblyAPI.AddTx(ctx, &blockassembly_api.BlockassemblyApiAddTxRequest{
				Txid:     hash[:],
				Fee:      fee,
				Size:     size,
				Locktime: locktime,
				Utxos:    utxoBytes,
				Parents:  parentBytes,
			}); err != nil {
				return false, err
			}

		} else {

			if _, err := s.client.AddTx(ctx, req); err != nil {
				return false, err
			}

		}
	} else {

		/* batch mode */
		s.batcher.Put(req)

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

func (s *Client) batchWorker(ctx context.Context, timeout time.Duration) {
	for {
		select {
		case <-ctx.Done():
			return
		case batch := <-s.batchCh:
			timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
			s.sendBatchToBlockAssembly(timeoutCtx, batch)
			cancel()
		}
	}
}

func (s *Client) sendBatchToBlockAssembly(ctx context.Context, batch []*blockassembly_api.AddTxRequest) {
	if s.frpcClient != nil {

		s.connectFRPC(ctx)
		fBatch := make([]*blockassembly_api.BlockassemblyApiAddTxRequest, len(batch))
		for i, req := range batch {
			fBatch[i] = &blockassembly_api.BlockassemblyApiAddTxRequest{
				Txid:     req.Txid,
				Fee:      req.Fee,
				Locktime: req.Locktime,
				Size:     req.Size,
				Utxos:    req.Utxos,
			}
		}
		txBatch := &blockassembly_api.BlockassemblyApiAddTxBatchRequest{
			TxRequests: fBatch,
		}
		resp, err := s.frpcClient.BlockAssemblyAPI.AddTxBatch(ctx, txBatch)
		if err != nil {
			s.logger.Errorf("%v", err)
			return
		}
		if len(resp.TxIdErrors) > 0 {
			s.logger.Errorf("batch send to blockassembly returned %d failed transactions from %d batch", len(resp.TxIdErrors), len(batch))
		}

	} else {

		txBatch := &blockassembly_api.AddTxBatchRequest{
			TxRequests: batch,
		}
		resp, err := s.client.AddTxBatch(ctx, txBatch)
		if err != nil {
			s.logger.Errorf("%v", err)
			return
		}
		if len(resp.TxIdErrors) > 0 {
			s.logger.Errorf("batch send to blockassembly returned %d failed transactions from %d batch", len(resp.TxIdErrors), len(batch))
		}

	}
}
