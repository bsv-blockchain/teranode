package blockvalidation

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/TAAL-GmbH/ubsv/model"
	"github.com/TAAL-GmbH/ubsv/services/blockchain"
	blockvalidation_api "github.com/TAAL-GmbH/ubsv/services/blockvalidation/blockvalidation_api"
	"github.com/TAAL-GmbH/ubsv/util"
	"github.com/libsv/go-bc"
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
	prometheusBlockValidationBlockFound prometheus.Counter
)

func init() {
	prometheusBlockValidationBlockFound = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "blockvalidation_block_found",
			Help: "Number of blocks found",
		},
	)
}

// BlockValidation type carries the logger within it
type BlockValidation struct {
	blockvalidation_api.UnimplementedBlockValidationAPIServer
	logger           utils.Logger
	grpcServer       *grpc.Server
	blockchainClient *blockchain.Client
}

func Enabled() bool {
	_, found := gocore.Config().Get("blockvalidation_grpcAddress")
	return found
}

// New will return a server instance with the logger stored within it
func New(logger utils.Logger) (*BlockValidation, error) {
	// utxostoreURL, err, found := gocore.Config().GetURL("utxostore")
	// if err != nil {
	// 	panic(err)
	// }
	// if !found {
	// 	panic("no utxostore setting found")
	// }

	// s, err := utxo.NewStore(logger, utxostoreURL)
	// if err != nil {
	// 	panic(err)
	// }

	blockchainClient, err := blockchain.NewClient()
	if err != nil {
		return nil, err
	}

	bVal := &BlockValidation{
		// utxoStore: s,
		logger:           logger,
		blockchainClient: blockchainClient,
	}

	return bVal, nil
}

// Start function
func (u *BlockValidation) Start() error {

	address, ok := gocore.Config().Get("blockvalidation_grpcAddress")
	if !ok {
		return errors.New("no blockvalidation_grpcAddress setting found")
	}

	var err error
	u.grpcServer, err = utils.GetGRPCServer(&utils.ConnectionOptions{
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

	blockvalidation_api.RegisterBlockValidationAPIServer(u.grpcServer, u)

	// Register reflection service on gRPC server.
	reflection.Register(u.grpcServer)

	u.logger.Infof("GRPC server listening on %s", address)

	if err = u.grpcServer.Serve(lis); err != nil {
		return fmt.Errorf("GRPC server failed [%w]", err)
	}

	return nil
}

func (u *BlockValidation) Stop(ctx context.Context) {
	_, cancel := context.WithCancel(ctx)
	defer cancel()

	u.grpcServer.GracefulStop()
}

func (u *BlockValidation) Health(_ context.Context, _ *emptypb.Empty) (*blockvalidation_api.HealthResponse, error) {
	return &blockvalidation_api.HealthResponse{
		Ok:        true,
		Timestamp: timestamppb.New(time.Now()),
	}, nil
}

func (u *BlockValidation) BlockFound(ctx context.Context, req *blockvalidation_api.BlockFoundRequest) (*emptypb.Empty, error) {
	prometheusBlockValidationBlockFound.Inc()

	waitGroup := sync.WaitGroup{}

	for _, subtreeRoot := range req.SubtreeHashes {
		go func(chunk []byte) {
			isValid := validateSubtree(chunk)
			if !isValid {
				// an invalid subtree has been found.
				// logging, cleanup
				return
			} else {
				waitGroup.Done()
			}
		}(subtreeRoot)
	}

	waitGroup.Wait()

	// check merkle root
	// check the solution meets the difficulty requirements
	// check the block is valid by consensus rules.
	// persist block
	// inform block assembler that a new block has been found

	return &emptypb.Empty{}, nil
}

func validateSubtree(subtree []byte) bool {
	// get subtree from store
	// if not in store get it from the network
	// validate the subtree
	// is the txid in the store?
	// no - get it from the network
	// yes - is the txid blessed?
	// does the merkle tree give the correct root?
	// if all txs in tree are blessed, then bless the tree

	return true
}

func (u *BlockValidation) ValidateBlock(ctx context.Context, block *model.Block) error {
	// 1. Check that the block header hash is less than the target difficulty.
	if err := u.CheckPOW(ctx, block); err != nil {
		return err
	}

	// 2. Check that the block timestamp is not more than two hours in the future.

	// 3. Check that the median time past of the block is after the median time past of the last 11 blocks.

	// 4. Check that the coinbase transaction is valid (reward checked later).
	// if err := b.checkValidCoinbase(); err != nil {
	// 	return err
	// }

	// 5. Check that the coinbase transaction includes the correct block height.
	// if err := b.checkCoinbaseHeight(); err != nil {
	// 	return err
	// }

	// 6. Get and validate any missing subtrees.
	// if err := b.getAndValidateSubtrees(ctx); err != nil {
	// 	return err
	// }

	// 7. Check that the first transaction in the first subtree is a coinbase placeholder (zeros)
	// if err := b.checkCoinbasePlaceholder(); err != nil {
	// 	return err
	// }

	// 8. Calculate the merkle root of the list of subtrees and check it matches the MR in the block header.
	if err := u.CheckMerkleRoot(block); err != nil {
		return err
	}

	// 4. Check that the coinbase transaction includes the correct block height.

	// 3. Check that each subtree is know and if not, get and process it.
	// 4. Add up the fees of each subtree.

	// 5. Check that the total fees of the block are less than or equal to the block reward.
	// 4. Check that the coinbase transaction includes the correct block reward.

	// 5. Check the there are no duplicate transactions in the block.
	// 6. Check that all transactions are valid (or blessed)

	return nil
}

func (u *BlockValidation) CheckPOW(ctx context.Context, block *model.Block) error {
	// TODO Check the nBits value is correct for this block

	// TODO - replace the following with a call to the blockchain service that gets the correct nBits value for the block
	_, _ = u.blockchainClient.GetBlock(ctx, block.Header.HashPrevBlock)

	header := bc.BlockHeader{}
	header.Valid()

	// Check that the block header hash is less than the target difficulty.
	ok := block.Header.Valid()

	if !ok {
		return model.ErrInvalidPOW
	}

	return nil
}

func (u *BlockValidation) CheckMerkleRoot(block *model.Block) error {
	hashes := make([][32]byte, len(block.Subtrees))

	for i, subtree := range block.Subtrees {
		// TODO this cannot be done here anymore, since the block only contains the subtree hashes
		//
		//if i == 0 {
		//	// We need to inject the coinbase txid into the first position of the first subtree
		//	var coinbaseHash [32]byte
		//	copy(coinbaseHash[:], bt.ReverseBytes(block.CoinbaseTx.TxIDBytes()))
		//	// get the full subtree from the store
		//	fullSubTree := util.SubTree{}
		//	fullSubTree.ReplaceRootNode(coinbaseHash)
		//}

		hashes[i] = [32]byte(subtree[:])
	}

	// Create a new subtree with the hashes of the subtrees
	st := util.NewTreeByLeafCount(util.CeilPowerOfTwo(len(block.Subtrees)))
	for _, hash := range hashes {
		err := st.AddNode(hash, 1)
		if err != nil {
			return err
		}
	}

	calculatedMerkleRoot := st.RootHash()
	calculatedMerkleRootHash, err := chainhash.NewHash(calculatedMerkleRoot[:])
	if err != nil {
		return err
	}

	if !block.Header.HashMerkleRoot.IsEqual(calculatedMerkleRootHash) {
		log.Printf("Expected %x, got %x", block.Header.HashMerkleRoot, calculatedMerkleRoot)
		return errors.New("merkle root does not match")
	}

	return nil
}
