package blockvalidation

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/blockchain"
	blockvalidation_api "github.com/bitcoin-sv/ubsv/services/blockvalidation/blockvalidation_api"
	"github.com/bitcoin-sv/ubsv/services/validator"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	txmeta_store "github.com/bitcoin-sv/ubsv/stores/txmeta"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/jellydator/ttlcache/v3"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type processBlockFound struct {
	hash    *chainhash.Hash
	baseURL string
}

type processBlockCatchup struct {
	block   *model.Block
	baseURL string
}

// Server type carries the logger within it
type Server struct {
	blockvalidation_api.UnimplementedBlockValidationAPIServer
	logger           utils.Logger
	blockchainClient blockchain.ClientI
	utxoStore        utxostore.Interface
	subtreeStore     blob.Store
	txStore          blob.Store
	txMetaStore      txmeta_store.Store
	validatorClient  validator.Interface

	blockFoundCh        chan processBlockFound
	catchupCh           chan processBlockCatchup
	catchingUp          atomic.Bool
	blockValidation     *BlockValidation
	processingSubtreeMu sync.Mutex
	processingSubtree   map[chainhash.Hash]bool

	// cache to prevent processing the same block / subtree multiple times
	// we are getting all message many times from the different miners and this prevents going to the stores multiple times
	processBlockNotify   *ttlcache.Cache[chainhash.Hash, bool]
	processSubtreeNotify *ttlcache.Cache[chainhash.Hash, bool]
}

func Enabled() bool {
	_, found := gocore.Config().Get("blockvalidation_grpcListenAddress")
	return found
}

// New will return a server instance with the logger stored within it
func New(logger utils.Logger, utxoStore utxostore.Interface, subtreeStore blob.Store, txStore blob.Store,
	txMetaStore txmeta_store.Store, validatorClient validator.Interface) *Server {

	initPrometheusMetrics()

	bVal := &Server{
		utxoStore:            utxoStore,
		logger:               logger,
		subtreeStore:         subtreeStore,
		txStore:              txStore,
		txMetaStore:          txMetaStore,
		validatorClient:      validatorClient,
		blockFoundCh:         make(chan processBlockFound, 100),
		catchupCh:            make(chan processBlockCatchup, 100),
		processingSubtree:    make(map[chainhash.Hash]bool),
		processBlockNotify:   ttlcache.New[chainhash.Hash, bool](),
		processSubtreeNotify: ttlcache.New[chainhash.Hash, bool](),
	}

	return bVal
}

func (u *Server) Init(ctx context.Context) (err error) {
	if u.blockchainClient, err = blockchain.NewClient(ctx); err != nil {
		return fmt.Errorf("failed to create blockchain client [%w]", err)
	}

	u.blockValidation = NewBlockValidation(u.logger, u.blockchainClient, u.subtreeStore, u.txStore, u.txMetaStore, u.validatorClient)

	// process blocks found from channel
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case c := <-u.catchupCh:
				{
					u.logger.Infof("processing catchup on channel [%s]", c.block.Hash().String())
					if err = u.catchup(ctx, c.block, c.baseURL); err != nil {
						u.logger.Errorf("failed to catchup from [%s] [%v]", c.block.Hash().String(), err)
					}
					u.logger.Infof("processing catchup on channel DONE [%s]", c.block.Hash().String())
				}
			case b := <-u.blockFoundCh:
				{
					// TODO optimize this for the valid chain, not processing everything ???
					u.logger.Infof("processing block found on channel [%s]", b.hash.String())
					if err = u.processBlockFound(ctx, b.hash, b.baseURL); err != nil {
						u.logger.Errorf("failed to process block [%s] [%v]", b.hash.String(), err)
					}
					u.logger.Infof("processing block found on channel DONE [%s]", b.hash.String())
				}
			}
		}
	}()

	return nil
}

// Start function
func (u *Server) Start(ctx context.Context) error {
	// this will block
	if err := util.StartGRPCServer(ctx, u.logger, "blockvalidation", func(server *grpc.Server) {
		blockvalidation_api.RegisterBlockValidationAPIServer(server, u)
	}); err != nil {
		return err
	}

	return nil
}

func (u *Server) Stop(_ context.Context) error {
	return nil
}

func (u *Server) Health(_ context.Context, _ *emptypb.Empty) (*blockvalidation_api.HealthResponse, error) {
	prometheusBlockValidationHealth.Inc()

	return &blockvalidation_api.HealthResponse{
		Ok:        true,
		Timestamp: timestamppb.New(time.Now()),
	}, nil
}

func (u *Server) BlockFound(ctx context.Context, req *blockvalidation_api.BlockFoundRequest) (*emptypb.Empty, error) {
	timeStart := time.Now()
	prometheusBlockValidationBlockFound.Inc()
	u.logger.Infof("BlockFound called [%s]", utils.ReverseAndHexEncodeSlice(req.Hash))

	if u.catchingUp.Load() {
		u.logger.Infof("BlockFound canceled, we are catching up [%s]", utils.ReverseAndHexEncodeSlice(req.Hash))
		return &emptypb.Empty{}, nil
	}

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
		//u.logger.Warnf("block found that already exists [%s]", hash.String())
		return &emptypb.Empty{}, nil
	}

	// process the block in the background, in the order we receive them, but without blocking the grpc call
	go func() {
		u.logger.Infof("BlockFound add on channel [%s]", hash.String())
		u.blockFoundCh <- processBlockFound{
			hash:    hash,
			baseURL: req.GetBaseUrl(),
		}
	}()

	prometheusBlockValidationBlockFoundDuration.Observe(float64(time.Since(timeStart).Microseconds()))

	return &emptypb.Empty{}, nil
}

func (u *Server) processBlockFound(ctx context.Context, hash *chainhash.Hash, baseUrl string) error {
	timeStart := time.Now()
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
			u.logger.Infof("processBlockFound add to catchup channel [%s]", hash.String())
			u.catchupCh <- processBlockCatchup{
				block:   block,
				baseURL: baseUrl,
			}
		}()
		return nil

	}

	// validate the block
	err = u.blockValidation.ValidateBlock(ctx, block, baseUrl)
	if err != nil {
		u.logger.Errorf("failed block validation BlockFound [%s] [%v]", block.String(), err)
	}

	prometheusBlockValidationProcessBlockFoundDuration.Observe(float64(time.Since(timeStart).Microseconds()))

	return nil
}

func (u *Server) getBlock(ctx context.Context, hash *chainhash.Hash, baseUrl string) (*model.Block, error) {
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

func (u *Server) getBlockHeaders(ctx context.Context, hash *chainhash.Hash, baseUrl string) ([]*model.BlockHeader, error) {
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

func (u *Server) catchup(ctx context.Context, fromBlock *model.Block, baseURL string) error {
	timeStart := time.Now()
	prometheusBlockValidationCatchup.Inc()

	u.logger.Infof("catching up from %s on server %s", fromBlock.Hash().String(), baseURL)
	u.catchingUp.Store(true)
	defer func() {
		u.catchingUp.Store(false)
	}()

	// first check whether this block already exists, which would mean we caught up from another peer
	exists, err := u.blockchainClient.GetBlockExists(ctx, fromBlock.Hash())
	if err != nil {
		return fmt.Errorf("failed to check if block exists [%w]", err)
	}
	if exists {
		return nil
	}

	catchupBlockHeaders := []*model.BlockHeader{fromBlock.Header}

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

		if err = u.blockValidation.ValidateBlock(ctx, block, baseURL); err != nil {
			return errors.Join(fmt.Errorf("failed block validation BlockFound [%s] [%v]", block.String(), err))
		}
	}

	prometheusBlockValidationCatchupDuration.Observe(float64(time.Since(timeStart).Microseconds()))

	return nil
}

func (u *Server) SubtreeFound(ctx context.Context, req *blockvalidation_api.SubtreeFoundRequest) (*emptypb.Empty, error) {
	timeStart := time.Now()
	prometheusBlockValidationSubtreeFound.Inc()

	u.logger.Infof("processing subtree found [%s]", utils.ReverseAndHexEncodeSlice(req.Hash))

	subtreeHash, err := chainhash.NewHash(req.Hash)
	if err != nil {
		return nil, fmt.Errorf("failed to create subtree hash from bytes [%w]", err)
	}

	if u.processSubtreeNotify.Get(*subtreeHash) != nil {
		return &emptypb.Empty{}, nil
	}
	// set the processing flag for 1 minute, so we don't process the same subtree multiple times
	u.processSubtreeNotify.Set(*subtreeHash, true, 1*time.Minute)

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
			u.logger.Warnf("subtree found that is already being processed [%s]", subtreeHash.String())
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

	prometheusBlockValidationSubtreeFoundDuration.Observe(float64(time.Since(timeStart).Microseconds()))

	return &emptypb.Empty{}, nil
}
