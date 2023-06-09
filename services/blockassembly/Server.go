package blockassembly

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/TAAL-GmbH/ubsv/services/blockassembly/blockassembly_api"
	"github.com/TAAL-GmbH/ubsv/services/blockassembly/subtreeprocessor"
	"github.com/TAAL-GmbH/ubsv/services/txstatus"
	"github.com/TAAL-GmbH/ubsv/services/validator/utxo"
	utxostore "github.com/TAAL-GmbH/ubsv/stores/utxo"
	"github.com/TAAL-GmbH/ubsv/util"
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
	logger utils.Logger

	utxoStore        utxostore.UTXOStore
	txStatusClient   txstatus.Client
	subtreeProcessor *subtreeprocessor.SubtreeProcessor
	grpcServer       *grpc.Server
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

	txStatusClient, err := txstatus.NewClient(context.Background(), logger)
	if err != nil {
		panic(err)
	}

	newSubTreeChan := make(chan *util.SubTree)

	ba := &BlockAssembly{
		logger:           logger,
		utxoStore:        s,
		txStatusClient:   *txStatusClient,
		subtreeProcessor: subtreeprocessor.NewSubtreeProcessor(newSubTreeChan),
	}

	go func() {
		for {
			<-newSubTreeChan
			// merkleRoot := stp.currentSubTree.ReplaceRootNode(*coinbaseHash)
			// assert.Equal(t, expectedMerkleRoot, utils.ReverseAndHexEncodeHash(merkleRoot))

			logger.Infof("Received new subTree notification")
		}
	}()

	return ba
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

// func (ba *BlockAssembly) NewChaintipAndHeight(ctx context.Context, req *blockassembly_api.NewChaintipAndHeightRequest) (*emptypb.Empty, error) {
// 	return &emptypb.Empty{}, nil
// }

func (ba *BlockAssembly) AddTx(ctx context.Context, req *blockassembly_api.AddTxRequest) (*blockassembly_api.AddTxResponse, error) {
	// Look up the new utxos for this txid, add them to the utxostore, and add the tx to the subtree builder...
	txid, err := chainhash.NewHash(req.Txid)
	if err != nil {
		return nil, err
	}

	txMetadata, err := ba.txStatusClient.Get(ctx, txid)
	if err != nil {
		return nil, err
	}

	// Add all the utxo hashes to the utxostore
	for _, hash := range txMetadata.UtxoHashes {
		if resp, err := ba.utxoStore.Store(context.Background(), hash); err != nil {
			return nil, fmt.Errorf("error storing utxo (%v): %w", resp, err)
		}
	}

	ba.subtreeProcessor.Add(*txid, txMetadata.Fee)

	prometheusBlockAssemblyAddTx.Inc()

	return &blockassembly_api.AddTxResponse{
		Ok: true,
	}, nil
}

// func (ba *BlockAssembly) GetMiningCandidate(ctx context.Context, _ *emptypb.Empty) (*blockassembly_api.GetMiningCandidateResponse, error) {

// 	// TODO - Get current chaintip and height and open merkle container for it.
// 	chaintip := &chainhash.Hash{}
// 	height := uint32(0)
// 	previousHash := &chainhash.Hash{}

// 	// Get the list of completed containers for the current chaintip and height...
// 	subTrees, err := merkle.GetContainersForCandidate(chaintip, height)
// 	if err != nil {
// 		panic(err)
// 	}

// 	return &blockassembly_api.GetMiningCandidateResponse{
// 		Id:             fmt.Sprintf("%d-%d", height, time.Now().UnixNano()),
// 		PreviousHash:   previousHash.CloneBytes(),
// 		CoinbaseValue:  0, // TODO ba.Fees,
// 		Version:        1,
// 		NBits:          uint32(0x1d00ffff),
// 		Height:         uint32(height),
// 		Time:           uint32(time.Now().Unix()),
// 		NumTx:          uint64(0),
// 		MerkleProof:    []string{},
// 		MerkleSubtrees: subTrees,
// 	}, nil
// }
