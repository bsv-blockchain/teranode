package blockvalidation

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/blockchain"
	blockvalidation_api "github.com/bitcoin-sv/ubsv/services/blockvalidation/blockvalidation_api"
	"github.com/bitcoin-sv/ubsv/services/validator"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	txmeta_store "github.com/bitcoin-sv/ubsv/stores/txmeta"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	prometheusBlockValidationBlockFound   prometheus.Counter
	prometheusBlockValidationSubtreeFound prometheus.Counter
)

func init() {
	prometheusBlockValidationBlockFound = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "blockvalidation_block_found",
			Help: "Number of blocks found",
		},
	)
	prometheusBlockValidationSubtreeFound = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "blockvalidation_subtree_found",
			Help: "Number of subtrees found",
		},
	)
}

type processBlockFound struct {
	hash    *chainhash.Hash
	baseURL string
}

type processBlockCatchup struct {
	block   *model.Block
	baseURL string
}

// BlockValidationServer type carries the logger within it
type BlockValidationServer struct {
	blockvalidation_api.UnimplementedBlockValidationAPIServer
	logger           utils.Logger
	blockchainClient blockchain.ClientI
	utxoStore        utxostore.Interface
	subtreeStore     blob.Store
	txMetaStore      txmeta_store.Store
	validatorClient  *validator.Client

	blockFoundCh        chan processBlockFound
	catchupCh           chan processBlockCatchup
	blockValidation     *BlockValidation
	processingSubtreeMu sync.Mutex
	processingSubtree   map[chainhash.Hash]bool
}

func Enabled() bool {
	_, found := gocore.Config().Get("blockvalidation_grpcListenAddress")
	return found
}

// New will return a server instance with the logger stored within it
func New(logger utils.Logger, utxoStore utxostore.Interface, subtreeStore blob.Store, txMetaStore txmeta_store.Store,
	validatorClient *validator.Client) *BlockValidationServer {

	bVal := &BlockValidationServer{
		utxoStore:         utxoStore,
		logger:            logger,
		subtreeStore:      subtreeStore,
		txMetaStore:       txMetaStore,
		validatorClient:   validatorClient,
		blockFoundCh:      make(chan processBlockFound, 100),
		catchupCh:         make(chan processBlockCatchup, 100),
		processingSubtree: make(map[chainhash.Hash]bool),
	}

	return bVal
}

func (u *BlockValidationServer) Init(ctx context.Context) (err error) {
	if u.blockchainClient, err = blockchain.NewClient(ctx); err != nil {
		return fmt.Errorf("failed to create blockchain client [%w]", err)
	}

	u.blockValidation = NewBlockValidation(u.logger, u.blockchainClient, u.subtreeStore, u.txMetaStore, u.validatorClient)

	// process blocks found from channel
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case c := <-u.catchupCh:
				{
					if err = u.catchup(ctx, c.block, c.baseURL); err != nil {
						u.logger.Errorf("failed to catchup from [%s] [%v]", c.block.Hash().String(), err)
					}
				}
			case b := <-u.blockFoundCh:
				{
					if err = u.processBlockFound(ctx, b.hash, b.baseURL); err != nil {
						u.logger.Errorf("failed to process block [%s] [%v]", b.hash.String(), err)
					}
				}
			}
		}
	}()

	return nil
}

// Start function
func (u *BlockValidationServer) Start(ctx context.Context) error {
	hash, _ := chainhash.NewHashFromStr("00d65139b2f625f2aee8e69a68a67573033e483cb94c20b5309541a817d27603")
	headers, err := u.getBlockHeaders(ctx, hash, "http://13.49.21.218:18290")
	//headers, err := u.getBlockHeaders(ctx, hash, "http://localhost:8090")
	if err != nil {
		u.logger.Errorf("failed to get block headers %s from peer [%w]", hash.String(), err)
	}

	for _, header := range headers {
		u.logger.Debugf("header: %s", header.String())
	}
	<-time.After(10 * time.Second)

	// this will block
	if err := util.StartGRPCServer(ctx, u.logger, "blockvalidation", func(server *grpc.Server) {
		blockvalidation_api.RegisterBlockValidationAPIServer(server, u)
	}); err != nil {
		return err
	}

	return nil
}

func (u *BlockValidationServer) Stop(_ context.Context) error {
	return nil
}

func (u *BlockValidationServer) Health(_ context.Context, _ *emptypb.Empty) (*blockvalidation_api.HealthResponse, error) {
	return &blockvalidation_api.HealthResponse{
		Ok:        true,
		Timestamp: timestamppb.New(time.Now()),
	}, nil
}

func (u *BlockValidationServer) BlockFound(ctx context.Context, req *blockvalidation_api.BlockFoundRequest) (*emptypb.Empty, error) {
	prometheusBlockValidationBlockFound.Inc()

	hash, err := chainhash.NewHash(req.Hash)
	if err != nil {
		return nil, err
	}

	// first check if the block exists, it is very expensive to do all the checks below
	exists, err := u.blockchainClient.GetBlockExists(ctx, hash)
	if err != nil {
		return nil, fmt.Errorf("failed to check if block exists [%w]", err)
	}

	if exists {
		u.logger.Warnf("block found that already exists [%s]", hash.String())
		return &emptypb.Empty{}, nil
	}

	// process the block in the background, in the order we receive them, but without blocking the grpc call
	go func() {
		u.blockFoundCh <- processBlockFound{
			hash:    hash,
			baseURL: req.GetBaseUrl(),
		}
	}()

	return &emptypb.Empty{}, nil
}

func (u *BlockValidationServer) processBlockFound(ctx context.Context, hash *chainhash.Hash, baseUrl string) error {
	u.logger.Infof("processing block found [%s]", hash.String())

	// first check if the block exists, it might have already been processed
	exists, err := u.blockchainClient.GetBlockExists(ctx, hash)
	if err != nil {
		return fmt.Errorf("failed to check if block exists [%w]", err)
	}
	if exists {
		u.logger.Warnf("not processing block that already was found [%s]", hash.String())
		return nil
	}

	block, err := u.getBlock(ctx, hash, baseUrl)
	if err != nil {
		return err
	}

	// catchup if we are missing the parent block
	parentExists, err := u.blockchainClient.GetBlockExists(ctx, block.Header.HashPrevBlock)
	if err != nil {
		return fmt.Errorf("failed to check if parent block %s exists [%w]", block.Header.HashPrevBlock.String(), err)
	}

	if !parentExists {
		// add to catchup channel, which will block processing any new blocks until we have caught up
		go func() {
			u.catchupCh <- processBlockCatchup{
				block:   block,
				baseURL: baseUrl,
			}
		}()
		return nil

	}

	// validate the block
	err = u.blockValidation.BlockFound(ctx, block, baseUrl)
	if err != nil {
		u.logger.Errorf("failed block validation BlockFound [%s] [%v]", block.String(), err)
	}

	return nil
}

func (u *BlockValidationServer) getBlock(ctx context.Context, hash *chainhash.Hash, baseUrl string) (*model.Block, error) {
	blockBytes, err := util.DoHTTPRequest(ctx, fmt.Sprintf("%s/block/%s", baseUrl, hash.String()))
	if err != nil {
		return nil, fmt.Errorf("failed to get block %s from peer [%w]", hash.String(), err)
	}

	block, err := model.NewBlockFromBytes(blockBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to create block %s from bytes [%w]", hash.String(), err)
	}

	if block == nil {
		return nil, fmt.Errorf("block could not be created from bytes: %v", blockBytes)
	}

	return block, nil
}

func (u *BlockValidationServer) getBlockHeaders(ctx context.Context, hash *chainhash.Hash, baseUrl string) ([]*model.BlockHeader, error) {
	blockHeadersBytes, err := util.DoHTTPRequest(ctx, fmt.Sprintf("%s/headers/%s", baseUrl, hash.String()))
	if err != nil {
		return nil, fmt.Errorf("failed to get block headers %s from peer [%w]", hash.String(), err)
	}

	blockHeaders := make([]*model.BlockHeader, 0, len(blockHeadersBytes)/model.BlockHeaderSize)

	var blockHeader *model.BlockHeader
	for i := 0; i < len(blockHeadersBytes); i += model.BlockHeaderSize {
		blockHeader, err = model.NewBlockHeaderFromBytes(blockHeadersBytes[i : i+model.BlockHeaderSize])
		if err != nil {
			return nil, fmt.Errorf("failed to create block header %s from bytes [%w]", hash.String(), err)
		}
		blockHeaders = append(blockHeaders, blockHeader)
	}

	return blockHeaders, nil
}

func (u *BlockValidationServer) catchup(ctx context.Context, fromBlock *model.Block, baseURL string) error {
	u.logger.Infof("catching up from %s on server %s", fromBlock.Hash().String(), baseURL)

	catchupBlockHeaders := []*model.BlockHeader{fromBlock.Header}
	var exists bool

	fromBlockHeaderHash := fromBlock.Header.HashPrevBlock

LOOP:
	for {
		u.logger.Debugf("getting block headers for catchup from [%s]", fromBlockHeaderHash.String())
		blockHeaders, err := u.getBlockHeaders(ctx, fromBlockHeaderHash, baseURL)
		if err != nil {
			return err
		}

		if len(blockHeaders) == 0 {
			return fmt.Errorf("failed to get block headers from [%s]", fromBlockHeaderHash.String())
		}

		for _, blockHeader := range blockHeaders {
			u.logger.Debugf("checking if block exists [%s]", blockHeader.String())
			exists, err = u.blockchainClient.GetBlockExists(ctx, blockHeader.Hash())
			if err != nil {
				return fmt.Errorf("failed to check if block exists [%w]", err)
			}
			if exists {
				break LOOP
			}

			catchupBlockHeaders = append(catchupBlockHeaders, blockHeader)

			fromBlockHeaderHash = blockHeader.HashPrevBlock
			if fromBlockHeaderHash.IsEqual(&chainhash.Hash{}) {
				return fmt.Errorf("failed to find parent block header, last was: %s", blockHeader.String())
			}
		}
	}

	u.logger.Infof("catching up from [%s] to [%s]", catchupBlockHeaders[len(catchupBlockHeaders)-1].String(), catchupBlockHeaders[0].String())

	// process the catchup block headers in reverse order
	for i := len(catchupBlockHeaders) - 1; i >= 0; i-- {
		blockHeader := catchupBlockHeaders[i]

		block, err := u.getBlock(ctx, blockHeader.Hash(), baseURL)
		if err != nil {
			return errors.Join(fmt.Errorf("failed to get block [%s] [%v]", blockHeader.String(), err))
		}

		if err = u.blockValidation.BlockFound(ctx, block, baseURL); err != nil {
			return errors.Join(fmt.Errorf("failed block validation BlockFound [%s] [%v]", block.String(), err))
		}
	}

	return nil
}

func (u *BlockValidationServer) SubtreeFound(ctx context.Context, req *blockvalidation_api.SubtreeFoundRequest) (*emptypb.Empty, error) {
	prometheusBlockValidationSubtreeFound.Inc()
	u.logger.Infof("processing subtree found [%s]", utils.ReverseAndHexEncodeSlice(req.Hash))

	subtreeHash, err := chainhash.NewHash(req.Hash)
	if err != nil {
		return nil, fmt.Errorf("failed to create subtree hash from bytes [%w]", err)
	}

	exists, err := u.subtreeStore.Exists(ctx, subtreeHash[:])
	if err != nil {
		return nil, fmt.Errorf("failed to check if subtree exists [%w]", err)
	}

	if exists {
		u.logger.Warnf("subtree found that already exists [%s]", subtreeHash.String())
		return &emptypb.Empty{}, nil
	}

	if req.GetBaseUrl() == "" {
		return nil, fmt.Errorf("base url is empty")
	}

	// validate the subtree in the background
	go func() {
		// check if we are already processing this subtree
		u.processingSubtreeMu.Lock()
		processing, ok := u.processingSubtree[*subtreeHash]
		if ok && processing {
			u.processingSubtreeMu.Unlock()
			return
		}

		// add to processing map
		u.processingSubtree[*subtreeHash] = true
		u.processingSubtreeMu.Unlock()

		// remove from processing map when done
		defer func() {
			u.processingSubtreeMu.Lock()
			delete(u.processingSubtree, *subtreeHash)
			u.processingSubtreeMu.Unlock()
		}()

		err = u.blockValidation.validateSubtree(context.TODO(), subtreeHash, req.GetBaseUrl())
		if err != nil {
			u.logger.Errorf("invalid subtree found [%s]: %v", subtreeHash.String(), err)
			return
		}
	}()

	return &emptypb.Empty{}, nil
}
