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
	client     blockassembly_api.BlockAssemblyAPIClient
	drpcClient blockassembly_api.DRPCBlockAssemblyAPIClient
	frpcClient *blockassembly_api.Client
	logger     utils.Logger
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

	client := &Client{
		client: blockassembly_api.NewBlockAssemblyAPIClient(baConn),
		logger: logger,
	}

	// Connect to experimental DRPC server if configured
	go client.connectDRPC()

	// Connect to experimental fRPC server if configured
	// fRPC has only been implemented for AddTx / Store
	go client.connectFRPC()

	return client
}

func (s Client) connectDRPC() {
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
		rawconn, err := net.Dial("tcp", blockAssemblyDrpcAddress)
		if err != nil {
			s.logger.Errorf("Error connecting to blockassembly: %s", err)
		}
		conn := drpcconn.New(rawconn)
		s.drpcClient = blockassembly_api.NewDRPCBlockAssemblyAPIClient(conn)
		s.logger.Infof("Connected to blockassembly DRPC server")
	}
}

func (s Client) connectFRPC() {
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

func (s Client) Store(ctx context.Context, hash *chainhash.Hash) (bool, error) {
	req := &blockassembly_api.AddTxRequest{
		Txid: hash[:],
	}

	if s.frpcClient != nil {
		if _, err := s.frpcClient.BlockAssemblyAPI.AddTx(ctx, &blockassembly_api.BlockassemblyApiAddTxRequest{
			Txid: hash[:],
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

	return true, nil
}

func (s Client) GetMiningCandidate(ctx context.Context) (*model.MiningCandidate, error) {
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

func (s Client) SubmitMiningSolution(ctx context.Context, solution *model.MiningSolution) error {
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
