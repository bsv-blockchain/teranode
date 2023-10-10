package blockassembly

import (
	"context"
	"net"
	"time"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/blockassembly/blockassembly_api"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"storj.io/drpc/drpcconn"
)

type Client struct {
	client       blockassembly_api.BlockAssemblyAPIClient
	drpcClient   blockassembly_api.DRPCBlockAssemblyAPIClient
	frpcClient   *blockassembly_api.Client
	logger       utils.Logger
	batchCh      chan *blockassembly_api.AddTxRequest
	batchSize    int
	batchTimeout int
}

func NewClient(ctx context.Context, logger utils.Logger) *Client {
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

	sendBatchSize, _ := gocore.Config().GetInt("blockassembly_sendBatchSize", 0)
	sendBatchTimeout, _ := gocore.Config().GetInt("blockassembly_sendBatchTimeout", 100)
	sendBatchWorkers, _ := gocore.Config().GetInt("blockassembly_sendBatchWorkers", 1)

	if sendBatchSize > 0 && sendBatchWorkers <= 0 {
		logger.Fatalf("expecting blockassembly_sendBatchWorkers > 0 when blockassembly_sendBatchSize = %d", sendBatchSize)
	}

	client := &Client{
		client:       blockassembly_api.NewBlockAssemblyAPIClient(baConn),
		logger:       logger,
		batchCh:      make(chan *blockassembly_api.AddTxRequest),
		batchSize:    sendBatchSize,
		batchTimeout: sendBatchTimeout,
	}

	// Connect to experimental DRPC server if configured
	go client.connectDRPC()

	// Connect to experimental fRPC server if configured
	// fRPC has only been implemented for AddTx / Store
	go client.connectFRPC()

	if sendBatchSize > 0 {
		for i := 0; i < sendBatchWorkers; i++ {
			go client.batchWorker(ctx)
		}
	}

	return client
}

func (s *Client) connectDRPC() {
	func() {
		err := recover()
		if err != nil {
			s.logger.Errorf("Error connecting to blockassembly DRPC: %s", err)
		}
	}()

	blockAssemblyDrpcAddress, ok := gocore.Config().Get("blockassembly_drpcAddress")
	if ok {
		s.logger.Infof("Using DRPC connection to blockassembly")
		time.Sleep(5 * time.Second) // allow everything to come up and find a better way to do this
		rawConn, err := net.Dial("tcp", blockAssemblyDrpcAddress)
		if err != nil {
			s.logger.Errorf("Error connecting to blockassembly: %s", err)
		}
		conn := drpcconn.New(rawConn)
		s.drpcClient = blockassembly_api.NewDRPCBlockAssemblyAPIClient(conn)
		s.logger.Infof("Connected to blockassembly DRPC server")
	}
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
		s.logger.Infof("Using fRPC connection to blockassembly")
		time.Sleep(5 * time.Second) // allow everything to come up and find a better way to do this

		client, err := blockassembly_api.NewClient(nil, nil)
		if err != nil {
			s.logger.Errorf("error creating new fRPC client in blockassembly: %s", err)
		}

		err = client.Connect(blockAssemblyFRPCAddress)
		if err != nil {
			s.logger.Errorf("error connecting to fRPC server in blockassembly: %s", err)
		} else {
			s.frpcClient = client
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

		} else if s.drpcClient != nil {

			if _, err := s.drpcClient.AddTx(ctx, req); err != nil {
				return false, err
			}

		} else {

			if _, err := s.client.AddTx(ctx, req); err != nil {
				return false, err
			}

		}
	} else {

		/* batch mode */
		s.batchCh <- req

	}

	return true, nil
}

func (s *Client) GetMiningCandidate(ctx context.Context) (*model.MiningCandidate, error) {
	req := &blockassembly_api.EmptyMessage{}

	var err error
	var res *model.MiningCandidate

	if s.drpcClient != nil {
		res, err = s.drpcClient.GetMiningCandidate(ctx, req)
	} else {
		res, err = s.client.GetMiningCandidate(ctx, req)
	}
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (s *Client) SubmitMiningSolution(ctx context.Context, solution *model.MiningSolution) error {
	var err error
	if s.drpcClient != nil {
		_, err = s.drpcClient.SubmitMiningSolution(ctx, &blockassembly_api.SubmitMiningSolutionRequest{
			Id:         solution.Id,
			Nonce:      solution.Nonce,
			CoinbaseTx: solution.Coinbase,
			Time:       solution.Time,
			Version:    solution.Version,
		})
	} else {
		_, err = s.client.SubmitMiningSolution(ctx, &blockassembly_api.SubmitMiningSolutionRequest{
			Id:         solution.Id,
			Nonce:      solution.Nonce,
			CoinbaseTx: solution.Coinbase,
			Time:       solution.Time,
			Version:    solution.Version,
		})
	}
	if err != nil {
		return err
	}

	return nil
}

func (s *Client) batchWorker(ctx context.Context) {
	duration := time.Duration(s.batchTimeout) * time.Millisecond
	ringBuffer := make([]*blockassembly_api.AddTxRequest, s.batchSize)
	i := 0
	for {
		select {
		case req := <-s.batchCh:
			ringBuffer[i] = req
			i++
			if i == s.batchSize {
				s.sendBatchToBlockAssembly(ctx, ringBuffer)
				i = 0
			}
		case <-time.After(duration):
			if i > 0 {
				s.sendBatchToBlockAssembly(ctx, ringBuffer[:i])
				i = 0
			}
		}
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

	} else if s.drpcClient != nil {

		txBatch := &blockassembly_api.AddTxBatchRequest{
			TxRequests: batch,
		}
		resp, err := s.drpcClient.AddTxBatch(ctx, txBatch)
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
