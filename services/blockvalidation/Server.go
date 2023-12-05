package blockvalidation

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/services/blockvalidation/blockvalidation_api"
	"github.com/bitcoin-sv/ubsv/services/status"
	"github.com/bitcoin-sv/ubsv/services/validator"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	txmeta_store "github.com/bitcoin-sv/ubsv/stores/txmeta"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/jellydator/ttlcache/v3"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/opentracing/opentracing-go"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var stats = gocore.NewStat("blockvalidation")

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
	logger           ulogger.Logger
	blockchainClient blockchain.ClientI
	utxoStore        utxostore.Interface
	subtreeStore     blob.Store
	txStore          blob.Store
	txMetaStore      txmeta_store.Store
	validatorClient  validator.Interface
	statusClient     status.ClientI

	blockFoundCh        chan processBlockFound
	catchupCh           chan processBlockCatchup
	blockValidation     *BlockValidation
	processingSubtreeMu sync.Mutex
	processingSubtree   map[chainhash.Hash]bool

	// cache to prevent processing the same block / subtree multiple times
	// we are getting all message many times from the different miners and this prevents going to the stores multiple times
	processSubtreeNotify *ttlcache.Cache[chainhash.Hash, bool]
}

func Enabled() bool {
	_, found := gocore.Config().Get("blockvalidation_grpcListenAddress")
	return found
}

// New will return a server instance with the logger stored within it
func New(logger ulogger.Logger, utxoStore utxostore.Interface, subtreeStore blob.Store, txStore blob.Store,
	txMetaStore txmeta_store.Store, validatorClient validator.Interface, statusClient status.ClientI) *Server {

	initPrometheusMetrics()

	bVal := &Server{
		utxoStore:            utxoStore,
		logger:               logger,
		subtreeStore:         subtreeStore,
		txStore:              txStore,
		validatorClient:      validatorClient,
		statusClient:         statusClient,
		blockFoundCh:         make(chan processBlockFound, 200), // this is excessive, but useful in testing
		catchupCh:            make(chan processBlockCatchup, 10),
		processingSubtree:    make(map[chainhash.Hash]bool),
		processSubtreeNotify: ttlcache.New[chainhash.Hash, bool](),
	}

	// create a caching tx meta store
	if gocore.Config().GetBool("blockvalidation_txMetaCacheEnabled", true) {
		logger.Infof("Using cached version of tx meta store")
		bVal.txMetaStore = newTxMetaCache(txMetaStore)
	} else {
		bVal.txMetaStore = txMetaStore
	}

	return bVal
}

func (u *Server) Init(ctx context.Context) (err error) {
	if u.blockchainClient, err = blockchain.NewClient(ctx, u.logger); err != nil {
		return fmt.Errorf("[Init] failed to create blockchain client [%w]", err)
	}

	u.blockValidation = NewBlockValidation(u.logger, u.blockchainClient, u.subtreeStore, u.txStore, u.txMetaStore, u.validatorClient)

	go u.processSubtreeNotify.Start()

	// process blocks found from channel
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case c := <-u.catchupCh:
				{
					_, _, ctx1 := util.NewStatFromContext(ctx, "catchupCh", stats, false)
					u.logger.Infof("[Init] processing catchup on channel [%s]", c.block.Hash().String())
					if err = u.catchup(ctx1, c.block, c.baseURL); err != nil {
						u.logger.Errorf("[Init] failed to catchup from [%s] [%v]", c.block.Hash().String(), err)
					}
					u.logger.Infof("[Init] processing catchup on channel DONE [%s]", c.block.Hash().String())
					prometheusBlockValidationCatchupCh.Set(float64(len(u.catchupCh)))
				}
			case b := <-u.blockFoundCh:
				{
					_, _, ctx1 := util.NewStatFromContext(ctx, "blockFoundCh", stats, false)
					// TODO optimize this for the valid chain, not processing everything ???
					u.logger.Infof("[Init] processing block found on channel [%s]", b.hash.String())
					if err = u.processBlockFound(ctx1, b.hash, b.baseURL); err != nil {
						u.logger.Errorf("[Init] failed to process block [%s] [%v]", b.hash.String(), err)
					}
					u.logger.Infof("[Init] processing block found on channel DONE [%s]", b.hash.String())
					prometheusBlockValidationBlockFoundCh.Set(float64(len(u.blockFoundCh)))
				}
			}
		}
	}()

	return nil
}

// Start function
func (u *Server) Start(ctx context.Context) error {

	frpcAddress, ok := gocore.Config().Get("blockvalidation_frpcListenAddress")
	if ok {
		err := u.frpcServer(ctx, frpcAddress)
		if err != nil {
			u.logger.Errorf("failed to start fRPC server: %v", err)
		}
	}

	// this will block
	if err := util.StartGRPCServer(ctx, u.logger, "blockvalidation", func(server *grpc.Server) {
		blockvalidation_api.RegisterBlockValidationAPIServer(server, u)
	}); err != nil {
		return err
	}

	return nil
}

func (u *Server) frpcServer(ctx context.Context, frpcAddress string) error {
	u.logger.Infof("Starting fRPC server on %s", frpcAddress)

	frpcBv := &fRPC_BlockValidation{
		blockValidation: u.blockValidation,
		logger:          u.logger,
	}

	s, err := blockvalidation_api.NewServer(frpcBv, nil, nil)
	if err != nil {
		return fmt.Errorf("failed to create fRPC server: %v", err)
	}

	concurrency, ok := gocore.Config().GetInt("blockvalidation_frpcConcurrency")
	if ok {
		u.logger.Infof("Setting fRPC server concurrency to %d", concurrency)
		s.SetConcurrency(uint64(concurrency))
	}

	// run the server
	go func() {
		err = s.Start(frpcAddress)
		if err != nil {
			u.logger.Errorf("failed to serve frpc: %v", err)
		}
	}()

	go func() {
		<-ctx.Done()
		err = s.Shutdown()
		if err != nil {
			u.logger.Errorf("failed to shutdown frpc server: %v", err)
		}
	}()

	return nil
}

func (u *Server) Stop(_ context.Context) error {
	u.processSubtreeNotify.Stop()

	return nil
}

func (u *Server) Health(_ context.Context, _ *blockvalidation_api.EmptyMessage) (*blockvalidation_api.HealthResponse, error) {
	start, stat, _ := util.NewStatFromContext(context.Background(), "Health", stats)
	defer func() {
		stat.AddTime(start)
	}()

	prometheusBlockValidationHealth.Inc()

	return &blockvalidation_api.HealthResponse{
		Ok:        true,
		Timestamp: uint32(time.Now().Unix()),
	}, nil
}

func (u *Server) BlockFound(ctx context.Context, req *blockvalidation_api.BlockFoundRequest) (*blockvalidation_api.EmptyMessage, error) {
	start, stat, ctx := util.NewStatFromContext(ctx, "BlockFound", stats)
	defer func() {
		stat.AddTime(start)
		prometheusBlockValidationBlockFoundDuration.Observe(util.TimeSince(start))
	}()

	prometheusBlockValidationBlockFound.Inc()
	u.logger.Infof("[BlockFound][%s] called from %s", utils.ReverseAndHexEncodeSlice(req.Hash), req.GetBaseUrl())

	hash, err := chainhash.NewHash(req.Hash)
	if err != nil {
		return nil, err
	}

	if u.statusClient != nil {
		baseUrl, _, _ := gocore.Config().GetURL("asset_httpAddress")

		u.statusClient.AnnounceStatus(ctx, &model.AnnounceStatusRequest{
			ServiceName: "BlockValidation",
			Type:        "BlockFound",
			StatusText:  "BlockFound - " + hash.String(),
			Timestamp:   timestamppb.Now(),
			BaseUrl:     baseUrl.String(),
		})

		defer func() {
			u.statusClient.AnnounceStatus(ctx, &model.AnnounceStatusRequest{
				ServiceName: "blockvalidation",
				StatusText:  "BlockFound - " + hash.String() + " - DONE",
				Type:        "BlockFound",
				Timestamp:   timestamppb.Now(),
				BaseUrl:     baseUrl.String(),
			})
		}()
	}

	// first check if the block exists, it is very expensive to do all the checks below
	exists, err := u.blockchainClient.GetBlockExists(ctx, hash)
	if err != nil {
		return nil, fmt.Errorf("[BlockFound][%s] failed to check if block exists [%w]", hash.String(), err)
	}
	if exists {
		//u.logger.Warnf("block found that already exists [%s]", hash.String())
		return &blockvalidation_api.EmptyMessage{}, nil
	}

	// process the block in the background, in the order we receive them, but without blocking the grpc call
	go func() {
		u.logger.Infof("[BlockFound][%s] add on channel", hash.String())
		u.blockFoundCh <- processBlockFound{
			hash:    hash,
			baseURL: req.GetBaseUrl(),
		}
		prometheusBlockValidationBlockFoundCh.Set(float64(len(u.blockFoundCh)))
	}()

	return &blockvalidation_api.EmptyMessage{}, nil
}

func (u *Server) processBlockFound(cntxt context.Context, hash *chainhash.Hash, baseUrl string) error {
	span, spanCtx := opentracing.StartSpanFromContext(cntxt, "BlockValidationServer:processBlockFound")
	span.LogKV("hash", hash.String())
	start, stat, ctx := util.NewStatFromContext(spanCtx, "processBlockFound", stats)
	defer func() {
		span.Finish()
		stat.AddTime(start)
		prometheusBlockValidationProcessBlockFoundDuration.Observe(util.TimeSince(start))
	}()

	u.logger.Infof("[processBlockFound][%s] processing block found from %s", hash.String(), baseUrl)

	// first check if the block exists, it might have already been processed
	exists, err := u.blockchainClient.GetBlockExists(ctx, hash)
	if err != nil {
		return fmt.Errorf("[processBlockFound][%s] failed to check if block exists [%w]", hash.String(), err)
	}
	if exists {
		u.logger.Warnf("[processBlockFound][%s] not processing block that already was found", hash.String())
		return nil
	}

	block, err := u.getBlock(ctx, hash, baseUrl)
	if err != nil {
		return err
	}

	// catchup if we are missing the parent block
	parentExists, err := u.blockchainClient.GetBlockExists(ctx, block.Header.HashPrevBlock)
	if err != nil {
		return fmt.Errorf("[processBlockFound][%s] failed to check if parent block %s exists [%w]", hash.String(), block.Header.HashPrevBlock.String(), err)
	}

	if !parentExists {
		// add to catchup channel, which will block processing any new blocks until we have caught up
		go func() {
			u.logger.Infof("[processBlockFound][%s] processBlockFound add to catchup channel", hash.String())
			u.catchupCh <- processBlockCatchup{
				block:   block,
				baseURL: baseUrl,
			}
			prometheusBlockValidationCatchupCh.Set(float64(len(u.catchupCh)))
		}()
		return nil

	}

	// validate the block
	u.logger.Infof("[processBlockFound][%s] validate block", hash.String())
	err = u.blockValidation.ValidateBlock(ctx, block, baseUrl)
	if err != nil {
		u.logger.Errorf("failed block validation BlockFound [%s] [%v]", block.String(), err)
	}

	return nil
}

func (u *Server) getBlock(ctx context.Context, hash *chainhash.Hash, baseUrl string) (*model.Block, error) {
	start, stat, ctx := util.NewStatFromContext(ctx, "getBlock", stats)
	span, spanCtx := opentracing.StartSpanFromContext(ctx, "BlockValidationServer:getBlock")
	defer func() {
		span.Finish()
		stat.AddTime(start)
	}()

	blockBytes, err := util.DoHTTPRequest(spanCtx, fmt.Sprintf("%s/block/%s", baseUrl, hash.String()))
	if err != nil {
		return nil, fmt.Errorf("[getBlock][%s] failed to get block from peer [%w]", hash.String(), err)
	}

	block, err := model.NewBlockFromBytes(blockBytes)
	if err != nil {
		return nil, fmt.Errorf("[getBlock][%s] failed to create block from bytes [%w]", hash.String(), err)
	}

	if block == nil {
		return nil, fmt.Errorf("[getBlock][%s] block could not be created from bytes: %v", hash.String(), blockBytes)
	}

	return block, nil
}

func (u *Server) getBlockHeaders(ctx context.Context, hash *chainhash.Hash, baseUrl string) ([]*model.BlockHeader, error) {
	start, stat, ctx := util.NewStatFromContext(ctx, "getBlockHeaders", stats)
	defer func() {
		stat.AddTime(start)
	}()

	blockHeadersBytes, err := util.DoHTTPRequest(ctx, fmt.Sprintf("%s/headers/%s", baseUrl, hash.String()))
	if err != nil {
		return nil, fmt.Errorf("[getBlockHeaders][%s] failed to get block headers from peer [%w]", hash.String(), err)
	}

	blockHeaders := make([]*model.BlockHeader, 0, len(blockHeadersBytes)/model.BlockHeaderSize)

	var blockHeader *model.BlockHeader
	for i := 0; i < len(blockHeadersBytes); i += model.BlockHeaderSize {
		blockHeader, err = model.NewBlockHeaderFromBytes(blockHeadersBytes[i : i+model.BlockHeaderSize])
		if err != nil {
			return nil, fmt.Errorf("[getBlockHeaders][%s] failed to create block header from bytes [%w]", hash.String(), err)
		}
		blockHeaders = append(blockHeaders, blockHeader)
	}

	return blockHeaders, nil
}

func (u *Server) catchup(ctx context.Context, fromBlock *model.Block, baseURL string) error {
	start, stat, ctx := util.NewStatFromContext(ctx, "catchup", stats)
	span, spanCtx := opentracing.StartSpanFromContext(ctx, "BlockValidationServer:catchup")
	defer func() {
		stat.AddTime(start)
		span.Finish()
		prometheusBlockValidationCatchupDuration.Observe(util.TimeSince(start))
	}()

	prometheusBlockValidationCatchup.Inc()

	u.logger.Infof("[catchup][%s] catching up on server %s", fromBlock.Hash().String(), baseURL)

	// first check whether this block already exists, which would mean we caught up from another peer
	exists, err := u.blockchainClient.GetBlockExists(spanCtx, fromBlock.Hash())
	if err != nil {
		return fmt.Errorf("[catchup][%s] failed to check if block exists [%w]", fromBlock.Hash().String(), err)
	}
	if exists {
		return nil
	}

	catchupBlockHeaders := []*model.BlockHeader{fromBlock.Header}

	fromBlockHeaderHash := fromBlock.Header.HashPrevBlock

	var blockHeaders []*model.BlockHeader
LOOP:
	for {
		u.logger.Debugf("[catchup][%s] getting block headers for catchup from [%s]", fromBlock.Hash().String(), fromBlockHeaderHash.String())
		blockHeaders, err = u.getBlockHeaders(spanCtx, fromBlockHeaderHash, baseURL)
		if err != nil {
			return err
		}

		if len(blockHeaders) == 0 {
			return fmt.Errorf("[catchup][%s] failed to get block headers from [%s]", fromBlock.Hash().String(), fromBlockHeaderHash.String())
		}

		for _, blockHeader := range blockHeaders {
			exists, err = u.blockchainClient.GetBlockExists(spanCtx, blockHeader.Hash())
			if err != nil {
				return fmt.Errorf("[catchup][%s] failed to check if block exists [%w]", fromBlock.Hash().String(), err)
			}
			if exists {
				break LOOP
			}

			catchupBlockHeaders = append(catchupBlockHeaders, blockHeader)

			fromBlockHeaderHash = blockHeader.HashPrevBlock
			if fromBlockHeaderHash.IsEqual(&chainhash.Hash{}) {
				return fmt.Errorf("[catchup][%s] failed to find parent block header, last was: %s", fromBlock.Hash().String(), blockHeader.String())
			}
		}
	}

	u.logger.Infof("[catchup][%s] catching up from [%s] to [%s]", fromBlock.Hash().String(), catchupBlockHeaders[len(catchupBlockHeaders)-1].String(), catchupBlockHeaders[0].String())

	// process the catchup block headers in reverse order
	var block *model.Block
	for i := len(catchupBlockHeaders) - 1; i >= 0; i-- {
		blockHeader := catchupBlockHeaders[i]

		block, err = u.getBlock(spanCtx, blockHeader.Hash(), baseURL)
		if err != nil {
			return errors.Join(fmt.Errorf("[catchup][%s] failed to get block [%s]", fromBlock.Hash().String(), blockHeader.String()), err)
		}

		if err = u.blockValidation.ValidateBlock(spanCtx, block, baseURL); err != nil {
			return errors.Join(fmt.Errorf("[catchup][%s] failed block validation BlockFound [%s]", fromBlock.Hash().String(), block.String()), err)
		}
	}
	return nil
}

func (u *Server) SubtreeFound(ctx context.Context, req *blockvalidation_api.SubtreeFoundRequest) (*blockvalidation_api.EmptyMessage, error) {
	start, stat, ctx := util.NewStatFromContext(ctx, "SubtreeFound", stats)
	span, spanCtx := opentracing.StartSpanFromContext(ctx, "BlockValidationServer:SubtreeFound")
	defer func() {
		stat.AddTime(start)
		span.Finish()
		prometheusBlockValidationSubtreeFoundDuration.Observe(util.TimeSince(start))
	}()

	prometheusBlockValidationSubtreeFound.Inc()
	u.logger.Infof("[SubtreeFound][%s] processing subtree found from %s", utils.ReverseAndHexEncodeSlice(req.Hash), req.GetBaseUrl())

	subtreeHash, err := chainhash.NewHash(req.Hash)
	if err != nil {
		return nil, fmt.Errorf("[SubtreeFound][%s] failed to create subtree hash from bytes: %w", utils.ReverseAndHexEncodeSlice(req.Hash), err)
	}

	if u.processSubtreeNotify.Get(*subtreeHash) != nil {
		u.logger.Warnf("[SubtreeFound][%s] already processing subtree", subtreeHash.String())
		return &blockvalidation_api.EmptyMessage{}, nil
	}
	// set the processing flag for 1 minute, so we don't process the same subtree multiple times
	u.processSubtreeNotify.Set(*subtreeHash, true, 1*time.Minute)

	start1 := gocore.CurrentTime()
	exists, err := u.subtreeStore.Exists(spanCtx, subtreeHash[:])
	stat.NewStat("subtreeStore.Exists").AddTime(start1)
	if err != nil {
		return nil, fmt.Errorf("[SubtreeFound][%s] failed to check if subtree exists [%w]", subtreeHash.String(), err)
	}

	if exists {
		u.logger.Warnf("[SubtreeFound][%s] subtree found that already exists", subtreeHash.String())
		return &blockvalidation_api.EmptyMessage{}, nil
	}

	if req.GetBaseUrl() == "" {
		return nil, fmt.Errorf("[SubtreeFound][%s] base url is empty", subtreeHash.String())
	}

	goroutineStat := stat.NewStat("go routine")

	// decouple the tracing context to not cancel the context when finalize the block processing in the background
	callerSpan := opentracing.SpanFromContext(spanCtx)
	setCtx := opentracing.ContextWithSpan(context.Background(), callerSpan)

	// validate the subtree in the background
	go func() {
		// check if we are already processing this subtree
		u.processingSubtreeMu.Lock()
		processing, ok := u.processingSubtree[*subtreeHash]
		if ok && processing {
			u.processingSubtreeMu.Unlock()
			u.logger.Warnf("[SubtreeFound][%s] subtree found that is already being processed", subtreeHash.String())
			return
		}

		// add to processing map
		u.processingSubtree[*subtreeHash] = true
		u.processingSubtreeMu.Unlock()

		// start a new span for the subtree validation
		start = gocore.CurrentTime()
		subtreeSpan, subtreeSpanCtx := opentracing.StartSpanFromContext(setCtx, "BlockValidationServer:SubtreeFound:validate")
		defer func() {
			goroutineStat.AddTime(start)
			subtreeSpan.Finish()

			// remove from processing map when done
			u.processingSubtreeMu.Lock()
			delete(u.processingSubtree, *subtreeHash)
			u.processingSubtreeMu.Unlock()
		}()

		subtreeSpan.LogKV("hash", subtreeHash.String())
		err = u.blockValidation.validateSubtree(subtreeSpanCtx, subtreeHash, req.GetBaseUrl())
		if err != nil {
			u.logger.Errorf("[SubtreeFound][%s] invalid subtree found: %v", subtreeHash.String(), err)
		}
	}()

	return &blockvalidation_api.EmptyMessage{}, nil
}

func (u *Server) Get(ctx context.Context, request *blockvalidation_api.GetSubtreeRequest) (*blockvalidation_api.GetSubtreeResponse, error) {
	subtree, err := u.subtreeStore.Get(ctx, request.Hash)
	if err != nil {
		return nil, err
	}

	return &blockvalidation_api.GetSubtreeResponse{
		Subtree: subtree,
	}, nil
}

func (u *Server) SetTxMeta(ctx context.Context, request *blockvalidation_api.SetTxMetaRequest) (*blockvalidation_api.SetTxMetaResponse, error) {
	start, stat, ctx := util.NewStatFromContext(ctx, "SetTxMeta", stats)
	defer func() {
		stat.AddTime(start)
	}()

	prometheusBlockValidationSetTXMetaCache.Inc()
	for _, meta := range request.Data {
		go func(meta []byte) {
			// first 32 bytes is hash
			hash := chainhash.Hash(meta[:32])

			txMetaData, err := txmeta_store.NewMetaDataFromBytes(meta[32:])
			if err != nil {
				u.logger.Errorf("failed to create tx meta data from bytes: %v", err)
			}

			if err = u.blockValidation.SetTxMetaCache(ctx, &hash, txMetaData); err != nil {
				u.logger.Errorf("failed to set tx meta data: %v", err)
			}
		}(meta)
	}

	return &blockvalidation_api.SetTxMetaResponse{
		Ok: true,
	}, nil
}
