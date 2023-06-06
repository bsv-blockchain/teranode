package blockassembly

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/TAAL-GmbH/ubsv/services/blockassembly/blockassembly_api"
	"github.com/TAAL-GmbH/ubsv/services/blockassembly/merkle"
	"github.com/TAAL-GmbH/ubsv/services/validator/utxo"
	utxostore "github.com/TAAL-GmbH/ubsv/stores/utxo"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
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
	blockassembly_api.UnimplementedBlockAssemblyAPIServer
	addTxChan         chan *blockassembly_api.AddTxRequest
	newHeightChan     chan *blockassembly_api.NewChaintipAndHeightRequest
	rotateChan        chan string
	incomingBlockChan chan string
	utxoStore         utxostore.UTXOStore
	logger            utils.Logger
	grpcServer        *grpc.Server
	merkleContainer   *merkle.Container
}

func Enabled() bool {
	_, found := gocore.Config().Get("blockassembly_grpcAddress")
	return found
}

// New will return a server instance with the logger stored within it
func New(logger utils.Logger) *BlockAssembly {
	utxostoreURL, err, found := gocore.Config().GetURL("utxostore")
	if err != nil {
		panic(err)
	}
	if !found {
		panic("no utxostore setting found")
	}

	s, err := utxo.NewStore(logger, utxostoreURL)
	if err != nil {
		panic(err)
	}

	// TODO - Get current chaintip and height and open merkle container for it.

	chaintip := &chainhash.Hash{}

	maxItemsPerFile, _ := gocore.Config().GetInt("merkle_items_per_subtree", 1_000_000)

	merkleContainer, err := merkle.OpenForWriting(chaintip, 0, uint32(maxItemsPerFile))
	if err != nil {
		panic(err)
	}

	return &BlockAssembly{
		utxoStore:       s,
		logger:          logger,
		addTxChan:       make(chan *blockassembly_api.AddTxRequest, 100_000),
		newHeightChan:   make(chan *blockassembly_api.NewChaintipAndHeightRequest),
		merkleContainer: merkleContainer,
	}
}

// Start function
func (ba *BlockAssembly) Start() error {
	address, ok := gocore.Config().Get("blockassembly_grpcAddress")
	if !ok {
		return errors.New("no blockassembly_grpcAddress setting found")
	}

	var err error
	ba.grpcServer, err = utils.GetGRPCServer(&utils.ConnectionOptions{
		OpenTracing: gocore.Config().GetBool("use_open_tracing", true),
	})
	if err != nil {
		return fmt.Errorf("could not create GRPC server [%w]", err)
	}

	gocore.SetAddress(address)

	lis, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("GRPC server failed to listen [%w]", err)
	}

	blockassembly_api.RegisterBlockAssemblyAPIServer(ba.grpcServer, ba)

	// Register reflection service on gRPC server.
	reflection.Register(ba.grpcServer)

	ba.logger.Infof("GRPC server listening on %s", address)

	go func() {
		for {
			select {
			case <-ba.rotateChan:

			case <-ba.incomingBlockChan:

			case heightReq := <-ba.newHeightChan:
				chaintip, err := chainhash.NewHash(heightReq.Chaintip)
				if err != nil {
					panic(err)
				}

				ba.merkleContainer.Close()

				txidFile, err := merkle.OpenForWriting(chaintip, heightReq.Height, 4)
				if err != nil {
					panic(err)
				}

				ba.merkleContainer = txidFile

			case txReq := <-ba.addTxChan:
				prometheusBlockAssemblyAddTx.Inc()

				hash, err := chainhash.NewHash(txReq.Txid)
				if err != nil {
					panic(err)
				}

				if err := ba.merkleContainer.AddTxID(hash, txReq.Fees); err != nil {
					panic(err)
				}

				// Add all the utxo hashes to the utxostore
				for _, hash := range txReq.UtxoHashes {
					h, err := chainhash.NewHash(hash)
					if err != nil {
						panic(err)
					}

					if resp, err := ba.utxoStore.Store(context.Background(), h); err != nil {
						panic(fmt.Errorf("error storing utxo (%v): %w", resp, err))
					}
				}
			}
		}
	}()

	if err = ba.grpcServer.Serve(lis); err != nil {
		return fmt.Errorf("GRPC server failed [%w]", err)
	}

	return nil
}

func (ba *BlockAssembly) Stop(ctx context.Context) {
	_, cancel := context.WithCancel(ctx)
	defer cancel()

	ba.grpcServer.GracefulStop()
}

func (ba *BlockAssembly) Health(_ context.Context, _ *emptypb.Empty) (*blockassembly_api.HealthResponse, error) {
	return &blockassembly_api.HealthResponse{
		Ok:        true,
		Timestamp: timestamppb.New(time.Now()),
	}, nil
}

func (ba *BlockAssembly) NewChaintipAndHeight(ctx context.Context, req *blockassembly_api.NewChaintipAndHeightRequest) (*emptypb.Empty, error) {
	ba.newHeightChan <- req

	return &emptypb.Empty{}, nil
}

func (ba *BlockAssembly) AddTx(ctx context.Context, req *blockassembly_api.AddTxRequest) (*blockassembly_api.AddTxResponse, error) {
	ba.addTxChan <- req

	return &blockassembly_api.AddTxResponse{
		Ok: true,
	}, nil
}
