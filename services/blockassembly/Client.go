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
	client     blockassembly_api.BlockAssemblyAPIClient
	frpcClient *blockassembly_api.Client
	logger     ulogger.Logger
	batchSize  int
	batchCh    chan []*blockassembly_api.AddTxRequest
	batcher    batcher.Batcher[blockassembly_api.AddTxRequest]
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

	// Connect to experimental fRPC server if configured
	// fRPC has only been implemented for AddTx / Store
	client.connectFRPC()

	if batchSize > 0 {
		for i := 0; i < sendBatchWorkers; i++ {
			go client.batchWorker(ctx)
		}
	}

	if client.frpcClient != nil {
		/* listen for close channel and reconnect */
		client.logger.Infof("Listening for close channel on fRPC client")
		go func() {
			for {
				select {
				case <-ctx.Done():
					client.logger.Infof("fRPC client context done, closing channel")
					return
				case <-client.frpcClient.CloseChannel():
					client.logger.Infof("fRPC client close channel received, reconnecting...")
					client.connectFRPC()
				}
			}
		}()
	}

	return client
}

func (s *Client) connectFRPC() {
	func() {
		err := recover()
		if err != nil {
			s.logger.Errorf("Error connecting to blockassembly fRPC: %s", err)
		}
	}()

	blockAssemblyFRPCAddress, ok := gocore.Config().Get("blockassembly_frpcAddress")
	if ok {
		maxRetries := 5
		retryInterval := 5 * time.Second

		for i := 0; i < maxRetries; i++ {
			s.logger.Infof("Attempting to create fRPC connection to blockassembly, attempt %d", i+1)

			client, err := blockassembly_api.NewClient(nil, nil)
			if err != nil {
				s.logger.Fatalf("Error creating new fRPC client in blockassembly: %s", err)
			}

			err = client.Connect(blockAssemblyFRPCAddress)
			if err != nil {
				s.logger.Infof("Error connecting to fRPC server in blockassembly: %s", err)
				if i+1 == maxRetries {
					break
				}
				time.Sleep(retryInterval)
				retryInterval *= 2
			} else {
				s.logger.Infof("Connected to blockassembly fRPC server")
				s.frpcClient = client
				break
			}
		}

		if s.frpcClient == nil {
			s.logger.Fatalf("Failed to connect to blockassembly fRPC server after %d attempts", maxRetries)
		}
	}
}

func (s *Client) Store(ctx context.Context, hash *chainhash.Hash, fee, size uint64, locktime uint32, utxoHashes []*chainhash.Hash) (bool, error) {
	utxoBytes := make([][]byte, len(utxoHashes))
	for i, h := range utxoHashes {
		utxoBytes[i] = h[:]
	}
	req := &blockassembly_api.AddTxRequest{
		Txid:     hash[:],
		Fee:      fee,
		Size:     size,
		Locktime: locktime,
		Utxos:    utxoBytes,
	}

	if s.batchSize == 0 {
		if s.frpcClient != nil {

			if _, err := s.frpcClient.BlockAssemblyAPI.AddTx(ctx, &blockassembly_api.BlockassemblyApiAddTxRequest{
				Txid:     hash[:],
				Fee:      fee,
				Size:     size,
				Locktime: locktime,
				Utxos:    utxoBytes,
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
	if err != nil {
		return err
	}

	return nil
}

func (s *Client) batchWorker(ctx context.Context) {
	for {
		batch := <-s.batchCh
		s.sendBatchToBlockAssembly(ctx, batch)
	}
}

func (s *Client) sendBatchToBlockAssembly(ctx context.Context, batch []*blockassembly_api.AddTxRequest) {
	if s.frpcClient != nil {

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
