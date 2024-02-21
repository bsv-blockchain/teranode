package blockvalidation

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"runtime"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/bitcoin-sv/ubsv/services/blockassembly"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/services/blockvalidation/blockvalidation_api"
	"github.com/bitcoin-sv/ubsv/services/validator"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	txmeta_store "github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/bitcoin-sv/ubsv/stores/txmetacache"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/jellydator/ttlcache/v3"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/opentracing/opentracing-go"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"google.golang.org/grpc"
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
	logger                       ulogger.Logger
	blockchainClient             blockchain.ClientI
	utxoStore                    utxostore.Interface
	subtreeStore                 blob.Store
	txStore                      blob.Store
	txMetaStore                  txmeta_store.Store
	validatorClient              validator.Interface
	blockFoundCh                 chan processBlockFound
	catchupCh                    chan processBlockCatchup
	blockValidation              *BlockValidation
	subtreeAssemblyKafkaProducer util.KafkaProducerI

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
	txMetaStore txmeta_store.Store, validatorClient validator.Interface) *Server {

	initPrometheusMetrics()

	bVal := &Server{
		utxoStore:            utxoStore,
		logger:               logger,
		subtreeStore:         subtreeStore,
		txStore:              txStore,
		validatorClient:      validatorClient,
		blockFoundCh:         make(chan processBlockFound, 200), // this is excessive, but useful in testing
		catchupCh:            make(chan processBlockCatchup, 10),
		processSubtreeNotify: ttlcache.New[chainhash.Hash, bool](),
	}

	// create a caching tx meta store
	if gocore.Config().GetBool("blockvalidation_txMetaCacheEnabled", true) {
		logger.Infof("Using cached version of tx meta store")
		bVal.txMetaStore = txmetacache.NewTxMetaCache(context.Background(), ulogger.TestLogger{}, txMetaStore)
	} else {
		bVal.txMetaStore = txMetaStore
	}

	return bVal
}

func (u *Server) Health(ctx context.Context) (int, string, error) {
	return 0, "", nil
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

	kafkaBrokersURL, err, ok := gocore.Config().GetURL("blockvalidation_kafkaBrokers")
	if err == nil && ok {
		u.startKafkaListener(ctx, kafkaBrokersURL)
	}

	frpcAddress, ok := gocore.Config().Get("blockvalidation_frpcListenAddress")
	if ok {
		err := u.frpcServer(ctx, frpcAddress)
		if err != nil {
			u.logger.Errorf("[BlockValidation] failed to start fRPC server: %v", err)
		}
	}

	httpAddress, ok := gocore.Config().Get("blockvalidation_httpListenAddress")
	if ok {
		err := u.httpServer(ctx, httpAddress)
		if err != nil {
			u.logger.Errorf("[BlockValidation] failed to start http server: %v", err)
		}
	}

	subtreeAssemblyKafkaBrokersURL, err, ok := gocore.Config().GetURL("subtreeassembly_kafkaBrokers")
	if err == nil && ok {
		_, u.subtreeAssemblyKafkaProducer, err = util.ConnectToKafka(subtreeAssemblyKafkaBrokersURL)
		if err != nil {
			u.logger.Errorf("[BlockValidation] unable to connect to kafka for subtree assembly: %v", err)
		} else {
			// start the blockchain subscriber
			go func() {
				subscription, err := u.blockchainClient.Subscribe(ctx, "subtreeassembly")
				if err != nil {
					u.logger.Errorf("[BlockValidation] failed starting subtree assembly subscription")
				}

				var block *model.Block
				for {
					select {
					case <-ctx.Done():
						u.logger.Infof("[BlockValidation] closing subtree assembly kafka producer")
						return
					case notification := <-subscription:
						if notification.Type == model.NotificationType_Block {
							block, err = u.blockchainClient.GetBlock(ctx, notification.Hash)
							if err != nil {
								u.logger.Errorf("[BlockValidation] failed getting block from blockchain service")
							}

							u.logger.Infof("[BlockValidation][%s] processing block into subtreeassembly kafka producer", block.Hash().String())

							for _, subtreeHash := range block.Subtrees {
								subtreeBytes := subtreeHash.CloneBytes()
								u.logger.Debugf("[BlockValidation][%s][%s] processing subtree into subtreeassembly kafka producer", block.Hash().String(), subtreeHash.String())
								if err = u.subtreeAssemblyKafkaProducer.Send(subtreeBytes, subtreeBytes); err != nil {
									u.logger.Errorf("[BlockValidation][%s][%s] failed to send subtree into subtreeassembly kafka producer", block.Hash().String(), subtreeHash.String())
								}
							}
						}
					}
				}
			}()
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
		u.logger.Infof("[Block Validation] Setting fRPC server concurrency to %d", concurrency)
		s.SetConcurrency(uint64(concurrency))
	}

	// run the server
	go func() {
		err := s.Start(frpcAddress)
		if err != nil {
			u.logger.Errorf("[Block Validation] failed to serve frpc: %v", err)
		}
	}()

	go func() {
		<-ctx.Done()
		err := s.Shutdown()
		if err != nil {
			u.logger.Errorf("[Block Validation] failed to shutdown frpc server: %v", err)
		}
	}()

	return nil
}

func (u *Server) httpServer(ctx context.Context, httpAddress string) error {
	startTime := time.Now()

	e := echo.New()
	e.HideBanner = true
	e.HidePort = true

	e.Use(middleware.Recover())

	e.Use(middleware.CORSWithConfig(middleware.CORSConfig{
		AllowOrigins: []string{"*"},
		AllowMethods: []string{echo.GET},
	}))

	e.GET("/alive", func(c echo.Context) error {
		return c.String(http.StatusOK, fmt.Sprintf("Asset service is alive. Uptime: %s\n", time.Since(startTime)))
	})

	e.GET("/health", func(c echo.Context) error {
		return c.String(http.StatusOK, "OK")
	})
	e.GET("/subtree/:hash", func(c echo.Context) error {
		txHashStr := c.Param("hash")
		txHash, err := chainhash.NewHashFromStr(txHashStr)
		if err != nil {
			return c.String(http.StatusBadRequest, fmt.Sprintf("invalid hash: %v", err))
		}
		subtreeBytes, err := u.subtreeStore.Get(c.Request().Context(), txHash[:])
		if err != nil {
			return c.String(http.StatusInternalServerError, fmt.Sprintf("failed to get subtree: %v", err))
		}

		return c.Blob(200, echo.MIMEOctetStream, subtreeBytes)
	})

	go func() {
		if err := e.Start(httpAddress); err != nil {
			u.logger.Errorf("[Block Validation] failed to start http server: %v", err)
		}
	}()

	go func() {
		<-ctx.Done()

		u.logger.Infof("[Block Validation] Shutting down block validation http server")
		if err := e.Shutdown(ctx); err != nil {
			u.logger.Errorf("[Block Validation] failed to shutdown http server: %v", err)
		}
	}()

	return nil
}

func (u *Server) Stop(_ context.Context) error {
	u.processSubtreeNotify.Stop()

	return nil
}

func (u *Server) HealthGRPC(_ context.Context, _ *blockvalidation_api.EmptyMessage) (*blockvalidation_api.HealthResponse, error) {
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
		prometheusBlockValidationBlockFoundDuration.Observe(float64(time.Since(start).Microseconds()) / 1_000_000)
		u.logger.Infof("[BlockFound][%s] DONE from %s", utils.ReverseAndHexEncodeSlice(req.Hash), req.GetBaseUrl())
	}()

	prometheusBlockValidationBlockFound.Inc()
	u.logger.Infof("[BlockFound][%s] called from %s", utils.ReverseAndHexEncodeSlice(req.Hash), req.GetBaseUrl())

	hash, err := chainhash.NewHash(req.Hash)
	if err != nil {
		return nil, err
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
		prometheusBlockValidationProcessBlockFoundDuration.Observe(float64(time.Since(start).Microseconds()) / 1_000_000)
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
		prometheusBlockValidationCatchupDuration.Observe(float64(time.Since(start).Microseconds()) / 1_000_000)
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

	validateBlocksChan := make(chan *model.Block, len(catchupBlockHeaders))

	// process the catchup block headers in reverse order and put them on the channel
	// this will allow the blocks to be validated while getting them from the other node
	g, gCtx := errgroup.WithContext(spanCtx)
	g.SetLimit(runtime.NumCPU())
	g.Go(func() error {
		var block *model.Block
		var blockHeader *model.BlockHeader
		for i := len(catchupBlockHeaders) - 1; i >= 0; i-- {
			blockHeader = catchupBlockHeaders[i]

			// TODO get blocks in batches
			block, err = u.getBlock(gCtx, blockHeader.Hash(), baseURL)
			if err != nil {
				return errors.Join(fmt.Errorf("[catchup][%s] failed to get block [%s]", fromBlock.Hash().String(), blockHeader.String()), err)
			}

			validateBlocksChan <- block
		}

		// close the channel to signal that all blocks have been processed
		close(validateBlocksChan)

		return nil
	})

	// validate the blocks while getting them from the other node
	// this will block until all blocks are validated
	for block := range validateBlocksChan {
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
		prometheusBlockValidationSubtreeFoundDuration.Observe(float64(time.Since(start).Microseconds()) / 1_000_000)
	}()

	subtreeHash, err := chainhash.NewHash(req.Hash)
	if err != nil {
		return nil, fmt.Errorf("[SubtreeFound][%s] failed to create subtree hash from bytes: %w", utils.ReverseAndHexEncodeSlice(req.Hash), err)
	}

	if req.GetBaseUrl() == "" {
		return nil, fmt.Errorf("[SubtreeFound][%s] base url is empty", subtreeHash.String())
	}

	prometheusBlockValidationSubtreeFound.Inc()
	u.logger.Infof("[SubtreeFound][%s] processing subtree found from %s", subtreeHash.String(), req.GetBaseUrl())

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

	// decouple the tracing context to not cancel the context when finalize the block processing in the background
	callerSpan := opentracing.SpanFromContext(spanCtx)
	setCtx := opentracing.ContextWithSpan(context.Background(), callerSpan)
	setCtx = util.ContextWithStat(setCtx, stat)
	goroutineStat := stat.NewStat("go routine")

	// validate the subtree in the background
	go func() {
		// start a new span for the subtree validation
		start := gocore.CurrentTime()
		subtreeSpan, subtreeSpanCtx := opentracing.StartSpanFromContext(setCtx, "BlockValidationServer:SubtreeFound:validate")
		defer func() {
			goroutineStat.AddTime(start)
			subtreeSpan.Finish()
		}()

		timeout, _ := gocore.Config().GetInt("blockvalidation_subtreeValidationTimeout", 60)
		timeoutCtx, timeoutCancel := context.WithTimeout(subtreeSpanCtx, time.Duration(timeout)*time.Second)
		defer func() {
			timeoutCancel()
			u.processSubtreeNotify.Delete(*subtreeHash)
		}()

		subtreeSpan.LogKV("hash", subtreeHash.String())
		err = u.blockValidation.validateSubtree(timeoutCtx, subtreeHash, req.GetBaseUrl())
		if err != nil {
			u.logger.Errorf("[SubtreeFound][%s] invalid subtree found: %v", subtreeHash.String(), err)
		}
	}()

	return &blockvalidation_api.EmptyMessage{}, nil
}

func (u *Server) Get(ctx context.Context, request *blockvalidation_api.GetSubtreeRequest) (*blockvalidation_api.GetSubtreeResponse, error) {
	start, stat, ctx := util.NewStatFromContext(ctx, "Get", stats)
	defer func() {
		stat.AddTime(start)
	}()

	subtree, err := u.subtreeStore.Get(ctx, request.Hash)
	if err != nil {
		return nil, err
	}

	return &blockvalidation_api.GetSubtreeResponse{
		Subtree: subtree,
	}, nil
}

func (u *Server) Exists(ctx context.Context, request *blockvalidation_api.ExistsSubtreeRequest) (*blockvalidation_api.ExistsSubtreeResponse, error) {
	start, stat, ctx := util.NewStatFromContext(ctx, "Exists", stats)
	defer func() {
		stat.AddTime(start)
	}()

	exists, err := u.subtreeStore.Exists(ctx, request.Hash)
	if err != nil {
		return nil, err
	}

	return &blockvalidation_api.ExistsSubtreeResponse{
		Exists: exists,
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

			data := meta[32:]
			txMetaData := &txmeta_store.Data{}
			txmeta_store.NewMetaDataFromBytes(&data, txMetaData)

			if err := u.blockValidation.SetTxMetaCache(ctx, &hash, txMetaData); err != nil {
				u.logger.Errorf("failed to set tx meta data: %v", err)
			}
		}(meta)
	}

	return &blockvalidation_api.SetTxMetaResponse{
		Ok: true,
	}, nil
}
func (u *Server) DelTxMeta(ctx context.Context, request *blockvalidation_api.DelTxMetaRequest) (*blockvalidation_api.DelTxMetaResponse, error) {
	start, stat, ctx := util.NewStatFromContext(ctx, "SetTxMeta", stats)
	defer func() {
		stat.AddTime(start)
	}()

	prometheusBlockValidationSetTXMetaCacheDel.Inc()
	hash, err := chainhash.NewHash(request.Hash[:])
	if err != nil {
		return nil, fmt.Errorf("failed to create hash from bytes: %v", err)
	}

	if err = u.blockValidation.DelTxMetaCacheMulti(ctx, hash); err != nil {
		u.logger.Errorf("failed to delete tx meta data: %v", err)
	}

	return &blockvalidation_api.DelTxMetaResponse{
		Ok: true,
	}, nil
}

func (u *Server) SetMinedMulti(ctx context.Context, request *blockvalidation_api.SetMinedMultiRequest) (*blockvalidation_api.SetMinedMultiResponse, error) {
	start, stat, ctx := util.NewStatFromContext(ctx, "SetMinedMulti", stats)
	defer func() {
		stat.AddTime(start)
	}()

	u.logger.Warnf("GRPC SetMinedMulti %d: %d", request.BlockId, len(request.Hashes))

	hashes := make([]*chainhash.Hash, 0, len(request.Hashes))
	for _, hash := range request.Hashes {
		hash32 := chainhash.Hash(hash)
		hashes = append(hashes, &hash32)
	}

	prometheusBlockValidationSetMinedMulti.Inc()
	err := u.blockValidation.SetTxMetaCacheMinedMulti(ctx, hashes, request.BlockId)
	if err != nil {
		return nil, err
	}

	return &blockvalidation_api.SetMinedMultiResponse{
		Ok: true,
	}, nil
}

func (u *Server) startKafkaListener(ctx context.Context, kafkaBrokersURL *url.URL) {
	workers, _ := gocore.Config().GetInt("blockvalidation_kafkaWorkers", 100)
	if workers < 1 {
		// no workers, nothing to do
		return
	}

	u.logger.Infof("[BlockValidation] Starting Kafka on address: %s, with %d workers", kafkaBrokersURL.String(), workers)

	util.StartKafkaListener(ctx, u.logger, kafkaBrokersURL, workers, "BlockValidation", "blockvalidation", func(ctx context.Context, key []byte, dataBytes []byte) error {
		startTime := time.Now()
		defer func() {
			prometheusBlockValidationSetTXMetaCacheKafka.Observe(float64(time.Since(startTime).Microseconds()) / 1_000_000)
		}()

		data, err := blockassembly.NewFromBytes(dataBytes)
		if err != nil {
			return fmt.Errorf("[BlockValidation] Failed to decode kafka message: %s", err)
		}

		if err = u.blockValidation.SetTxMetaCache(ctx, data.TxIDChainHash, &txmeta_store.Data{
			Fee:            data.Fee,
			SizeInBytes:    data.Size,
			ParentTxHashes: data.ParentTxHashes,
		}); err != nil {
			u.logger.Errorf("failed to set tx meta data: %v", err)
		}

		return nil
	})
}
