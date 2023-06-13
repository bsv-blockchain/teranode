package blockassembly

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/TAAL-GmbH/ubsv/model"
	"github.com/TAAL-GmbH/ubsv/services/blockassembly/blockassembly_api"
	"github.com/TAAL-GmbH/ubsv/services/blockassembly/subtreeprocessor"
	"github.com/TAAL-GmbH/ubsv/services/blockchain"
	"github.com/TAAL-GmbH/ubsv/services/txstatus"
	"github.com/TAAL-GmbH/ubsv/services/txstatus/store"
	"github.com/TAAL-GmbH/ubsv/services/validator/utxo"
	"github.com/TAAL-GmbH/ubsv/stores/blob"
	txstatus_store "github.com/TAAL-GmbH/ubsv/stores/txstatus"
	utxostore "github.com/TAAL-GmbH/ubsv/stores/utxo"
	"github.com/TAAL-GmbH/ubsv/util"
	"github.com/libsv/go-bt/v2"
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
	jobStore                     map[chainhash.Hash]*blockassembly_api.GetMiningCandidateResponse
)

func init() {
	prometheusBlockAssemblyAddTx = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "blockassembly_add_tx",
			Help: "Number of txs added to the blockassembly service",
		},
	)
	jobStore = make(map[chainhash.Hash]*blockassembly_api.GetMiningCandidateResponse)
}

// BlockAssembly type carries the logger within it
type BlockAssembly struct {
	blockassembly_api.UnimplementedBlockAssemblyAPIServer
	logger utils.Logger

	utxoStore        utxostore.UTXOStore
	txStatusClient   txstatus_store.Store
	subtreeProcessor *subtreeprocessor.SubtreeProcessor
	grpcServer       *grpc.Server
	blockchainClient *blockchain.Client
	blockStore       blob.Store
}

func Enabled() bool {
	_, found := gocore.Config().Get("blockassembly_grpcAddress")
	return found
}

// New will return a server instance with the logger stored within it
func New(logger utils.Logger, blockStore blob.Store) *BlockAssembly {
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

	txStatusURL, err, found := gocore.Config().GetURL("txstatus_store")
	if err != nil {
		panic(err)
	}
	if !found {
		panic("no txstatus_store setting found")
	}

	// TODO abstract into a factory
	var txStatusStore txstatus_store.Store
	if txStatusURL.Scheme == "memory" {
		// the memory store is reached through a grpc client
		txStatusStore, err = txstatus.NewClient(context.Background(), logger)
		if err != nil {
			panic(err)
		}
	} else {
		txStatusStore, err = store.New(logger, txStatusURL)
		if err != nil {
			panic(err)
		}
	}

	blockchainClient, err := blockchain.NewClient()
	if err != nil {
		panic(err)
	}

	newSubtreeChan := make(chan *util.Subtree)

	ba := &BlockAssembly{
		logger:           logger,
		utxoStore:        s,
		txStatusClient:   txStatusStore,
		subtreeProcessor: subtreeprocessor.NewSubtreeProcessor(newSubtreeChan),
		blockchainClient: blockchainClient,
		blockStore:       blockStore,
	}

	go func() {
		var subtreeBytes []byte
		for {
			subtree := <-newSubtreeChan
			// merkleRoot := stp.currentSubtree.ReplaceRootNode(*coinbaseHash)
			// assert.Equal(t, expectedMerkleRoot, utils.ReverseAndHexEncodeHash(merkleRoot))

			if subtreeBytes, err = subtree.Serialize(); err != nil {
				logger.Errorf("Failed to serialize subtree [%s]", err)
				continue
			}

			if err = ba.blockStore.Set(context.Background(), subtree.RootHash()[:], subtreeBytes); err != nil {
				logger.Errorf("Failed to store subtree [%s]", err)
				continue
			}

			logger.Infof("Received new subtree notification for: %s", subtree.RootHash().String())
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

func (ba *BlockAssembly) GetMiningCandidate(ctx context.Context, _ *emptypb.Empty) (*blockassembly_api.GetMiningCandidateResponse, error) {

	chainTip, height, err := ba.blockchainClient.GetChainTip(ctx)
	if err != nil {
		return nil, fmt.Errorf("error getting best block header: %w", err)
	}

	// Get the list of completed containers for the current chaintip and height...
	subtrees := ba.subtreeProcessor.GetCompletedSubtreesForMiningCandidate()
	var coinbaseValue uint64

	for _, subtree := range subtrees {
		coinbaseValue += subtree.Fees
	}
	coinbaseValue += util.GetBlockSubsidyForHeight(height)

	id := subtrees[len(subtrees)-1].RootHash()

	// TODO does this not need to be calculated?
	nbits := chainTip.Bits
	//nbits, err := hex.DecodeString("0x1d00ffff")
	//if err != nil {
	//	return nil, err
	//}

	job := &blockassembly_api.GetMiningCandidateResponse{
		Id:            id[:],
		PreviousHash:  chainTip.HashPrevBlock.CloneBytes(),
		CoinbaseValue: coinbaseValue,
		Version:       1,
		NBits:         nbits,
		Height:        uint32(height), // should this be an uint64?
		Time:          uint32(time.Now().Unix()),
		MerkleProof:   ba.subtreeProcessor.GetMerkleProofForCoinbase(),
	}

	storeId, err := chainhash.NewHash(id[:])
	if err != nil {
		return nil, err
	}

	jobStore[*storeId] = job

	return job, nil
}

func (ba *BlockAssembly) SubmitMiningSolution(ctx context.Context, req *blockassembly_api.SubmitMiningSolutionRequest) (*blockassembly_api.SubmitMiningSolutionResponse, error) {

	storeId, err := chainhash.NewHash(req.Id[:])
	if err != nil {
		return &blockassembly_api.SubmitMiningSolutionResponse{
			Ok: false,
		}, err
	}

	job, ok := jobStore[*storeId]
	if !ok {
		return &blockassembly_api.SubmitMiningSolutionResponse{
			Ok: false,
		}, fmt.Errorf("job not found")
	}

	hashPrevBlock, err := chainhash.NewHash(job.PreviousHash)
	if err != nil {
		return nil, fmt.Errorf("failed to convert hashPrevBlock: %w", err)
	}

	coinbaseTx, err := bt.NewTxFromBytes(req.CoinbaseTx)
	if err != nil {
		return nil, fmt.Errorf("failed to convert coinbaseTx: %w", err)
	}
	coinbaseTxIDHash, err := chainhash.NewHashFromStr(coinbaseTx.TxID())
	if err != nil {
		return nil, fmt.Errorf("failed to convert coinbaseTxHash: %w", err)
	}

	subtreesInJob := ba.subtreeProcessor.GetCompleteSubreesForJob(job.Id)
	subtreesInJob[0].ReplaceRootNode(coinbaseTxIDHash)

	subtreeHashes := make([]*chainhash.Hash, len(subtreesInJob))
	transactionCount := uint64(0)
	for i, subtree := range subtreesInJob {
		rootHash := subtree.RootHash()
		subtreeHashes[i], _ = chainhash.NewHash(rootHash[:])
		transactionCount += uint64(subtree.Length())
	}

	// Create a new subtree with the subtreeHashes of the subtrees
	topTree := util.NewTreeByLeafCount(util.CeilPowerOfTwo(len(subtreesInJob)))
	for _, hash := range subtreeHashes {
		err = topTree.AddNode(hash, 1)
		if err != nil {
			return nil, err
		}
	}

	calculatedMerkleRoot := topTree.RootHash()
	hashMerkleRoot, err := chainhash.NewHash(calculatedMerkleRoot[:])
	if err != nil {
		return nil, err
	}

	block := &model.Block{
		Header: &model.BlockHeader{
			Version:        req.Version,
			HashPrevBlock:  hashPrevBlock,
			HashMerkleRoot: hashMerkleRoot,
			Timestamp:      req.Time,
			Bits:           job.NBits,
			Nonce:          req.Nonce,
		},
		CoinbaseTx:       coinbaseTx,
		TransactionCount: transactionCount,
		Subtrees:         subtreeHashes,
	}

	// check fully valid, including whether difficulty in header is low enough
	if ok = block.Valid(); !ok {
		ba.logger.Errorf("Invalid block: %v", block.Header)
		return &blockassembly_api.SubmitMiningSolutionResponse{
			Ok: false,
		}, nil
	}

	if err = ba.blockchainClient.AddBlock(ctx, block); err != nil {
		return nil, fmt.Errorf("failed to add block: %w", err)
	}

	return &blockassembly_api.SubmitMiningSolutionResponse{
		Ok: true,
	}, nil
}
