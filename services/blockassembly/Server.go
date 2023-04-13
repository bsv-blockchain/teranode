package blockassembly

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/TAAL-GmbH/ubsv/services/blockassembly/blockassembly_api"
	"github.com/TAAL-GmbH/ubsv/services/propagation/store"
	"github.com/TAAL-GmbH/ubsv/tracing"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
	"github.com/libsv/go-p2p/wire"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	prometheusBlockAssemblyAddTx prometheus.Counter
)

func init() {
	prometheusBlockAssemblyAddTx = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "blockassembly_add_tx",
			Help: "Number of txs added to the blockassembly service",
		},
	)
}

// BlockAssembly type carries the logger within it
type BlockAssembly struct {
	blockassembly_api.BlockAssemblyAPIServer
	logger     utils.Logger
	grpcServer *grpc.Server
	mu         sync.Mutex
	txIDs      []*chainhash.Hash
}

func Enabled() bool {
	_, found := gocore.Config().Get("blockassembly_grpcAddress")
	return found
}

// New will return a server instance with the logger stored within it
func New(logger utils.Logger, txStore store.TransactionStore) (*BlockAssembly, error) {
	bAss := &BlockAssembly{
		logger: logger,
		txIDs:  make([]*chainhash.Hash, 0),
	}

	go func() {
		for {
			time.Sleep(2 * time.Minute)
			bAss.mu.Lock()

			if len(bAss.txIDs) > 0 {
				bAss.logger.Infof("Mining block for %d txs", len(bAss.txIDs))

				// get previous block header

				blockHeader := wire.NewBlockHeader(1, &chainhash.Hash{}, &chainhash.Hash{}, 0, 0)

				blockMsg := wire.NewMsgBlock(blockHeader)
				for _, txID := range bAss.txIDs {
					tx, err := txStore.Get(context.Background(), txID[:])
					if err != nil {
						bAss.logger.Errorf("Failed to get tx %s from store: %v", txID, err)
						continue
					}
					msgTx := wire.NewMsgTx(1)
					err = msgTx.Deserialize(bytes.NewReader(tx))
					if err != nil {
						bAss.logger.Errorf("Failed to deserialize tx %s: %v", txID, err)
						continue
					}
					err = blockMsg.AddTransaction(msgTx)
					if err != nil {
						bAss.logger.Errorf("Failed to add tx %s to block: %v", txID, err)
						continue
					}
				}

				// broadcast the block

				bAss.txIDs = make([]*chainhash.Hash, 0)
			}
			bAss.mu.Unlock()
		}
	}()

	return bAss, nil
}

// Start function
func (u *BlockAssembly) Start() error {

	address, _, ok := gocore.Config().GetURL("blockassembly_grpcAddress")
	if !ok {
		return errors.New("no blockassembly_grpcAddress setting found")
	}

	// LEVEL 0 - no security / no encryption
	var opts []grpc.ServerOption
	_, prometheusOn := gocore.Config().Get("prometheusEndpoint")
	if prometheusOn {
		opts = append(opts,
			grpc.ChainStreamInterceptor(grpc_prometheus.StreamServerInterceptor),
			grpc.ChainUnaryInterceptor(grpc_prometheus.UnaryServerInterceptor),
		)
	}

	u.grpcServer = grpc.NewServer(tracing.AddGRPCServerOptions(opts)...)

	gocore.SetAddress(address.Host)

	lis, err := net.Listen("tcp", address.Host)
	if err != nil {
		return fmt.Errorf("GRPC server failed to listen [%w]", err)
	}

	blockassembly_api.RegisterBlockAssemblyAPIServer(u.grpcServer, u)

	// Register reflection service on gRPC server.
	reflection.Register(u.grpcServer)

	u.logger.Infof("GRPC server listening on %s", address)

	if err = u.grpcServer.Serve(lis); err != nil {
		return fmt.Errorf("GRPC server failed [%w]", err)
	}

	return nil
}

func (u *BlockAssembly) Stop(ctx context.Context) {
	_, cancel := context.WithCancel(ctx)
	defer cancel()

	u.grpcServer.GracefulStop()
}

func (u *BlockAssembly) Health(_ context.Context, _ *emptypb.Empty) (*blockassembly_api.HealthResponse, error) {
	return &blockassembly_api.HealthResponse{
		Ok:        true,
		Timestamp: timestamppb.New(time.Now()),
	}, nil
}

func (u *BlockAssembly) AddTxID(_ context.Context, req *blockassembly_api.AddTxIDRequest) (*blockassembly_api.AddTxIDResponse, error) {
	prometheusBlockAssemblyAddTx.Inc()

	u.mu.Lock()
	defer u.mu.Unlock()

	hash, err := chainhash.NewHash(req.Txid)
	if err != nil {
		return nil, err
	}

	u.txIDs = append(u.txIDs, hash)

	return &blockassembly_api.AddTxIDResponse{
		Ok: true,
	}, nil
}
