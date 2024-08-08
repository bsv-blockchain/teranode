package blockvalidation

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/services/blockchain/blockchain_api"
	"github.com/bitcoin-sv/ubsv/services/blockvalidation/blockvalidation_api"
	"github.com/bitcoin-sv/ubsv/services/subtreevalidation"
	"github.com/bitcoin-sv/ubsv/services/validator"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/tracing"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/jellydator/ttlcache/v3"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

type processBlockFound struct {
	hash    *chainhash.Hash
	baseURL string
	errCh   chan error
}

type processBlockCatchup struct {
	block   *model.Block
	baseURL string
}

// Server type carries the logger within it
type Server struct {
	blockvalidation_api.UnimplementedBlockValidationAPIServer
	logger                      ulogger.Logger
	blockchainClient            blockchain.ClientI
	subtreeStore                blob.Store
	txStore                     blob.Store
	utxoStore                   utxo.Store
	validatorClient             validator.Interface
	blockFoundCh                chan processBlockFound
	catchupCh                   chan processBlockCatchup
	blockValidation             *BlockValidation
	blockPersisterKafkaProducer util.KafkaProducerI
	SetTxMetaQ                  *util.LockFreeQ[[][]byte]

	// cache to prevent processing the same block / subtree multiple times
	// we are getting all message many times from the different miners and this prevents going to the stores multiple times
	processSubtreeNotify *ttlcache.Cache[chainhash.Hash, bool]
	// bloom filter stats for all blocks processed
	stats *gocore.Stat
}

// New will return a server instance with the logger stored within it
func New(logger ulogger.Logger, subtreeStore blob.Store, txStore blob.Store,
	utxoStore utxo.Store, validatorClient validator.Interface) *Server {

	initPrometheusMetrics()

	// TEMP limit to 1, to prevent multiple subtrees processing at the same time
	subtreeGroupConcurrency, _ := gocore.Config().GetInt("blockvalidation_subtreeGroupConcurrency", 1)

	subtreeGroup := errgroup.Group{}
	subtreeGroup.SetLimit(subtreeGroupConcurrency)

	blockFoundChBuffer, _ := gocore.Config().GetInt("blockvalidation_blockFoundCh_buffer_size", 1000) // during testing often mine 1000 blocks to begin with
	catchupChBuffer, _ := gocore.Config().GetInt("blockvalidation_catchupCh_buffer_size", 10)

	bVal := &Server{
		logger:               logger,
		subtreeStore:         subtreeStore,
		txStore:              txStore,
		utxoStore:            utxoStore,
		validatorClient:      validatorClient,
		blockFoundCh:         make(chan processBlockFound, blockFoundChBuffer),
		catchupCh:            make(chan processBlockCatchup, catchupChBuffer),
		processSubtreeNotify: ttlcache.New[chainhash.Hash, bool](),
		SetTxMetaQ:           util.NewLockFreeQ[[][]byte](),
		stats:                gocore.NewStat("blockvalidation"),
	}

	return bVal
}

func (u *Server) Health(_ context.Context) (int, string, error) {
	return 0, "", nil
}

func (u *Server) Init(ctx context.Context) (err error) {
	if u.blockchainClient, err = blockchain.NewClient(ctx, u.logger); err != nil {
		return errors.NewServiceError("[Init] failed to create blockchain client", err)
	}

	subtreeValidationClient := subtreevalidation.NewClient(ctx, u.logger)

	storeURL, err, found := gocore.Config().GetURL("utxostore")
	if err != nil || !found {
		return errors.NewConfigurationError("could not get utxostore URL", err)
	}

	expiration := uint64(0)
	expirationValue := storeURL.Query().Get("expiration")
	if expirationValue != "" {
		expiration, err = strconv.ParseUint(expirationValue, 10, 64)
		if err != nil {
			return errors.NewConfigurationError("could not parse expiration %s", expirationValue, err)
		}
	}

	u.blockValidation = NewBlockValidation(ctx, u.logger, u.blockchainClient, u.subtreeStore, u.txStore, u.utxoStore, u.validatorClient, subtreeValidationClient, time.Duration(expiration)*time.Second)

	go u.processSubtreeNotify.Start()

	go func() {
		for {
			select {
			case <-ctx.Done():
				u.logger.Infof("[Init] closing block found channel")
				return
			default:
				data := u.SetTxMetaQ.Dequeue()
				if data == nil {
					time.Sleep(100 * time.Millisecond)
					continue
				}

				go func(data *[][]byte) {
					prometheusBlockValidationSetTxMetaQueueCh.Dec()

					keys := make([][]byte, 0)
					values := make([][]byte, 0)

					for _, meta := range *data {
						if len(meta) < 32 {
							u.logger.Errorf("meta data is too short: %v", meta)
							return
						}

						// first 32 bytes is hash
						keys = append(keys, meta[:32])
						values = append(values, meta[32:])
					}

					if err := u.blockValidation.SetTxMetaCacheMulti(ctx, keys, values); err != nil {
						u.logger.Errorf("failed to set tx meta data: %v", err)
					}
				}(data)
			}
		}
	}()

	// process blocks found from channel
	go func() {
		for {
			_, _, ctx1 := tracing.NewStatFromContext(ctx, "catchupCh", u.stats, false)
			select {
			case <-ctx.Done():
				u.logger.Infof("[Init] closing block found channel")
				return
			case c := <-u.catchupCh:
				{
					// stop mining
					err = u.blockchainClient.SendFSMEvent(ctx1, blockchain_api.FSMEventType_CATCHUPBLOCKS)
					if err != nil {
						u.logger.Errorf("[BlockValidation Init] failed to send STOPMINING event [%v]", err)
					}

					u.logger.Infof("[BlockValidation Init] processing catchup on channel [%s]", c.block.Hash().String())
					if err := u.catchup(ctx1, c.block, c.baseURL); err != nil {
						u.logger.Errorf("[BlockValidation Init] failed to catchup from [%s] [%v]", c.block.Hash().String(), err)
					}

					u.logger.Infof("[BlockValidation Init] processing catchup on channel DONE [%s]", c.block.Hash().String())
					prometheusBlockValidationCatchupCh.Set(float64(len(u.catchupCh)))

					// start mining
					err = u.blockchainClient.SendFSMEvent(ctx1, blockchain_api.FSMEventType_MINE)
					if err != nil {
						u.logger.Errorf("[BlockValidation Init] failed to send MINE event [%v]", err)
					}
				}
			case blockFound := <-u.blockFoundCh:
				{
					if err := u.processBlockFoundChannel(ctx, blockFound); err != nil {
						u.logger.Errorf("[Init] failed to process block found [%s] [%v]", blockFound.hash.String(), err)
					}
				}
			}
		}
	}()

	return nil
}

func (u *Server) processBlockFoundChannel(ctx context.Context, blockFound processBlockFound) error {
	useCatchupWhenBehind := gocore.Config().GetBool("blockvalidation_useCatchupWhenBehind", false)
	if useCatchupWhenBehind && len(u.blockFoundCh) > 3 {
		// we are multiple blocks behind, process all the blocks per peer on the catchup channel
		u.logger.Infof("[Init] processing block found on channel [%s] - too many blocks behind", blockFound.hash.String())
		peerBlocks := make(map[string]processBlockFound)
		peerBlocks[blockFound.baseURL] = blockFound
		// get the newest block per peer, emptying the block found channel
		for len(u.blockFoundCh) > 0 {
			pb := <-u.blockFoundCh
			peerBlocks[pb.baseURL] = pb
		}

		u.logger.Infof("[Init] peerBlocks: %v", peerBlocks)
		// add that latest block of each peer to the catchup channel
		for _, pb := range peerBlocks {
			block, err := u.getBlock(ctx, pb.hash, pb.baseURL)
			if err != nil {
				return errors.NewProcessingError("[Init] failed to get block [%s]", pb.hash.String(), err)
			}

			u.catchupCh <- processBlockCatchup{
				block:   block,
				baseURL: pb.baseURL,
			}
		}
		return nil
	}

	_, _, ctx1 := tracing.NewStatFromContext(ctx, "blockFoundCh", u.stats, false)

	// TODO optimize this for the valid chain, not processing everything ???
	u.logger.Infof("[Init] processing block found on channel [%s]", blockFound.hash.String())
	err := u.processBlockFound(ctx1, blockFound.hash, blockFound.baseURL)
	if err != nil {
		if blockFound.errCh != nil {
			blockFound.errCh <- err
		}
		return errors.NewProcessingError("[Init] failed to process block [%s]", blockFound.hash.String(), err)
	}

	if blockFound.errCh != nil {
		blockFound.errCh <- nil
	}

	u.logger.Infof("[Init] processing block found on channel DONE [%s]", blockFound.hash.String())
	prometheusBlockValidationBlockFoundCh.Set(float64(len(u.blockFoundCh)))

	return nil
}

// Start function
func (u *Server) Start(ctx context.Context) error {
	httpAddress, ok := gocore.Config().Get("blockvalidation_httpListenAddress")
	if ok {
		err := u.httpServer(ctx, httpAddress)
		if err != nil {
			u.logger.Errorf("[BlockValidation] failed to start http server: %v", err)
		}
	}

	subtreesKafkaURL, err, ok := gocore.Config().GetURL("kafka_subtreesFinalConfig")
	if err == nil && ok {
		_, u.blockPersisterKafkaProducer, err = util.ConnectToKafka(subtreesKafkaURL)
		if err != nil {
			u.logger.Errorf("[BlockValidation] unable to connect to kafka for subtree assembly: %v", err)
		} else {
			// start the blockchain subscriber
			go func() {
				subscription, err := u.blockchainClient.Subscribe(ctx, "blockpersister")
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
							hash, err := chainhash.NewHash(notification.Hash)
							if err != nil {
								u.logger.Errorf("[BlockValidation] failed to parse block hash from notification: %v", err)
								continue
							}
							block, err = u.blockchainClient.GetBlock(ctx, hash)
							if err != nil {
								u.logger.Errorf("[BlockValidation] failed getting block from blockchain service - NOT sending subtree to blockpersister kafka producer")
							} else if block == nil {
								u.logger.Errorf("[BlockValidation] block is nil from blockchain service - NOT sending subtree to blockpersister kafka producer")
							} else {

								u.logger.Debugf("[BlockValidation][%s] processing block into blockpersister kafka producer", block.Hash().String())

								for _, subtreeHash := range block.Subtrees {
									subtreeBytes := subtreeHash.CloneBytes()
									u.logger.Debugf("[BlockValidation][%s][%s] processing subtree into blockpersister kafka producer", block.Hash().String(), subtreeHash.String())
									if err := u.blockPersisterKafkaProducer.Send(subtreeBytes, subtreeBytes); err != nil {
										u.logger.Errorf("[BlockValidation][%s][%s] failed to send subtree into blockpersister kafka producer", block.Hash().String(), subtreeHash.String())
									}
								}
							}
						}
					}
				}
			}()
		}
	}

	//kafkaBlocksValidateConfigURL, err, ok := gocore.Config().GetURL("kafka_blocksValidateConfig")
	//if err == nil && ok {
	//	u.logger.Infof("[BlockValidation] starting block validation Kafka client on address: %s, with %d workers", kafkaBlocksValidateConfigURL.String(), 1)
	//
	//	util.StartKafkaListener(ctx, u.logger, kafkaBlocksValidateConfigURL, 1, "BlockValidation", "blockvalidation", func(_ context.Context, blockHashBytes []byte, _ []byte) error {
	//		blockHash, err := chainhash.NewHash(blockHashBytes)
	//		if err != nil {
	//			u.logger.Errorf("[BlockValidation] failed to parse block hash from kafka: %v", err)
	//			return nil
	//		}
	//		return u.blockValidation.validateBlock(ctx, blockHash)
	//	})
	//}

	// this will block
	if err := util.StartGRPCServer(ctx, u.logger, "blockvalidation", func(server *grpc.Server) {
		blockvalidation_api.RegisterBlockValidationAPIServer(server, u)
	}); err != nil {
		return err
	}

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
		hashStr := c.Param("hash")
		hash, err := chainhash.NewHashFromStr(hashStr)
		if err != nil {
			return c.String(http.StatusBadRequest, fmt.Sprintf("invalid hash: %v", err))
		}
		subtreeBytes, err := u.subtreeStore.Get(c.Request().Context(), hash[:], options.WithFileExtension("subtree"))
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
	start, stat, _ := tracing.NewStatFromContext(context.Background(), "Health", u.stats)
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
	ctx, _, deferFn := tracing.StartTracing(ctx, "BlockFound",
		tracing.WithParentStat(u.stats),
		tracing.WithHistogram(prometheusBlockValidationBlockFound),
		tracing.WithLogMessage(u.logger, "[BlockFound][%s] called from %s", utils.ReverseAndHexEncodeSlice(req.Hash), req.GetBaseUrl()),
	)
	defer deferFn()

	hash, err := chainhash.NewHash(req.Hash)
	if err != nil {
		return nil, errors.WrapGRPC(
			errors.NewProcessingError("[BlockFound][%s] failed to create hash from bytes", utils.ReverseAndHexEncodeSlice(req.Hash), err))
	}

	// first check if the block exists, it is very expensive to do all the checks below
	exists, err := u.blockValidation.GetBlockExists(ctx, hash)
	if err != nil {
		return nil, errors.WrapGRPC(
			errors.NewServiceError("[BlockFound][%s] failed to check if block exists", hash.String(), err))
	}
	if exists {
		u.logger.Infof("[BlockFound][%s] already validated, skipping", utils.ReverseAndHexEncodeSlice(req.Hash))
		return &blockvalidation_api.EmptyMessage{}, nil
	}

	var errCh chan error

	if req.WaitToComplete {
		errCh = make(chan error)
	}

	// process the block in the background, in the order we receive them, but without blocking the grpc call
	go func() {
		u.logger.Infof("[BlockFound][%s] add on channel", hash.String())
		u.blockFoundCh <- processBlockFound{
			hash:    hash,
			baseURL: req.GetBaseUrl(),
			errCh:   errCh,
		}
		prometheusBlockValidationBlockFoundCh.Set(float64(len(u.blockFoundCh)))
	}()

	if req.WaitToComplete {
		err := <-errCh
		if err != nil {
			return nil, errors.WrapGRPC(err)
		}
	}

	return &blockvalidation_api.EmptyMessage{}, nil
}

func (u *Server) ProcessBlock(ctx context.Context, request *blockvalidation_api.ProcessBlockRequest) (*blockvalidation_api.EmptyMessage, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "ProcessBlock",
		tracing.WithParentStat(u.stats),
		tracing.WithLogMessage(u.logger, "[ProcessBlock][%s] process block called", request.Height),
	)
	defer deferFn()

	block, err := model.NewBlockFromBytes(request.Block)
	if err != nil {
		return nil, errors.WrapGRPC(errors.NewProcessingError("failed to create block from bytes", err))
	}

	// we need the height for the subsidy calculation
	height := request.Height

	if height <= 0 {
		// try to get the height from the previous block
		_, previousBlockMeta, err := u.blockchainClient.GetBlockHeader(ctx, block.Header.HashPrevBlock)
		if err != nil {
			return nil, errors.WrapGRPC(errors.NewServiceError("failed to get previous block header", err))
		}
		if previousBlockMeta != nil {
			height = previousBlockMeta.Height + 1
		}
	}

	if height <= 0 {
		return nil, errors.WrapGRPC(errors.NewProcessingError("invalid height: %d", height))
	}

	block.Height = request.Height

	// GOKHAN
	// TODO - check if hardcoding "legacy" is OK
	err = u.processBlockFound(ctx, block.Header.Hash(), "legacy", block)
	if err != nil {
		return nil, errors.WrapGRPC(errors.NewProcessingError("failed block validation ProcessBlock [%s]", block.String(), err))
	}

	return &blockvalidation_api.EmptyMessage{}, nil
}

func (u *Server) processBlockFound(ctx context.Context, hash *chainhash.Hash, baseUrl string, useBlock ...*model.Block) error {
	ctx, _, deferFn := tracing.StartTracing(ctx, "processBlockFound",
		tracing.WithParentStat(u.stats),
		tracing.WithHistogram(prometheusBlockValidationProcessBlockFound),
		tracing.WithLogMessage(u.logger, "[processBlockFound][%s] processing block found from %s", hash.String(), baseUrl),
	)
	defer deferFn()

	// first check if the block exists, it might have already been processed
	exists, err := u.blockValidation.GetBlockExists(ctx, hash)
	if err != nil {
		return errors.WrapGRPC(errors.NewServiceError("[processBlockFound][%s] failed to check if block exists", hash.String(), err))
	}
	if exists {
		u.logger.Warnf("[processBlockFound][%s] not processing block that already was found", hash.String())
		return nil
	}

	var block *model.Block
	if len(useBlock) > 0 {
		block = useBlock[0]
	} else {
		block, err = u.getBlock(ctx, hash, baseUrl)
		if err != nil {
			return errors.WrapGRPC(err)
		}
	}

	u.checkParentProcessingComplete(ctx, block, baseUrl)

	// catchup if we are missing the parent block.
	parentExists, err := u.blockValidation.GetBlockExists(ctx, block.Header.HashPrevBlock)
	if err != nil {
		return errors.WrapGRPC(
			errors.NewServiceError("[processBlockFound][%s] failed to check if parent block %s exists", hash.String(), block.Header.HashPrevBlock.String(), err))
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

	// this is a bit of a hack, but we need to turn off optimistic mining when in legacy mode
	useOptimisticMining := true
	if baseUrl == "legacy" {
		useOptimisticMining = false
	}
	err = u.blockValidation.ValidateBlock(ctx, block, baseUrl, u.blockValidation.bloomFilterStats, useOptimisticMining)
	if err != nil {
		return errors.WrapGRPC(errors.NewServiceError("failed block validation BlockFound [%s]", block.String(), err))
	}

	return nil
}

func (u *Server) checkParentProcessingComplete(ctx context.Context, block *model.Block, baseUrl string) {
	_, _, deferFn := tracing.StartTracing(ctx, "checkParentProcessingComplete",
		tracing.WithParentStat(u.stats),
		tracing.WithLogMessage(u.logger, "[checkParentProcessingComplete][%s] called from %s", block.Hash().String(), baseUrl),
	)
	defer deferFn()

	delay := 10 * time.Millisecond
	maxDelay := 10 * time.Second

	// check if the parent block is being validated, then wait for it to finish.
	blockBeingFinalized := u.blockValidation.blockHashesCurrentlyValidated.Exists(*block.Header.HashPrevBlock) ||
		u.blockValidation.blockBloomFiltersBeingCreated.Exists(*block.Header.HashPrevBlock)

	if blockBeingFinalized {
		u.logger.Infof("[processBlockFound][%s] parent block is being validated (hash: %s), waiting for it to finish: validated %v - bloom filters %v",
			block.Hash().String(),
			block.Header.HashPrevBlock.String(),
			u.blockValidation.blockHashesCurrentlyValidated.Exists(*block.Header.HashPrevBlock),
			u.blockValidation.blockBloomFiltersBeingCreated.Exists(*block.Header.HashPrevBlock),
		)
		retries := 0
		for {
			blockBeingFinalized = u.blockValidation.blockHashesCurrentlyValidated.Exists(*block.Header.HashPrevBlock) ||
				u.blockValidation.blockBloomFiltersBeingCreated.Exists(*block.Header.HashPrevBlock)

			if !blockBeingFinalized {
				break
			}

			if (retries % 10) == 0 {
				u.logger.Infof("[processBlockFound][%s] parent block is still (%d) being validated (hash: %s), waiting for it to finish: validated %v - bloom filters %v",
					block.Hash().String(),
					retries,
					block.Header.HashPrevBlock.String(),
					u.blockValidation.blockHashesCurrentlyValidated.Exists(*block.Header.HashPrevBlock),
					u.blockValidation.blockBloomFiltersBeingCreated.Exists(*block.Header.HashPrevBlock),
				)
			}

			time.Sleep(delay)
			delay *= 2
			if delay > maxDelay {
				delay = maxDelay
			}

			time.Sleep(delay)
			delay *= 2
			if delay > maxDelay {
				delay = maxDelay
			}

			retries++
		}
	}
}

func (u *Server) getBlock(ctx context.Context, hash *chainhash.Hash, baseUrl string) (*model.Block, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "getBlock",
		tracing.WithParentStat(u.stats),
	)
	defer deferFn()

	blockBytes, err := util.DoHTTPRequest(ctx, fmt.Sprintf("%s/block/%s", baseUrl, hash.String()))
	if err != nil {
		return nil, errors.NewProcessingError("[getBlock][%s] failed to get block from peer", hash.String(), err)
	}

	block, err := model.NewBlockFromBytes(blockBytes)
	if err != nil {
		return nil, errors.NewProcessingError("[getBlock][%s] failed to create block from bytes", hash.String(), err)
	}

	if block == nil {
		return nil, errors.NewProcessingError("[getBlock][%s] block could not be created from bytes: %v", hash.String(), blockBytes)
	}

	return block, nil
}

func (u *Server) getBlocks(ctx context.Context, hash *chainhash.Hash, n uint32, baseUrl string) ([]*model.Block, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "getBlocks",
		tracing.WithParentStat(u.stats),
	)
	defer deferFn()

	blockBytes, err := util.DoHTTPRequest(ctx, fmt.Sprintf("%s/blocks/%s?n=%d", baseUrl, hash.String(), n))
	if err != nil {
		return nil, errors.NewProcessingError("[getBlocks][%s] failed to get blocks from peer", hash.String(), err)
	}

	blockReader := bytes.NewReader(blockBytes)

	blocks := make([]*model.Block, 0)
	for {
		block, err := model.NewBlockFromReader(blockReader)
		if err != nil {
			if strings.Contains(err.Error(), "EOF") {
				// if strings.Contains(err.Error(), "EOF") || errors.Is(err, io.ErrUnexpectedEOF) { // doesn't catch the EOF!!!! //TODO
				break
			}
			return nil, errors.NewProcessingError("[getBlocks][%s] failed to create block from bytes", hash.String(), err)
		}
		blocks = append(blocks, block)
	}

	return blocks, nil
}

func (u *Server) getBlockHeaders(ctx context.Context, hash *chainhash.Hash, baseUrl string) ([]*model.BlockHeader, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "getBlockHeaders",
		tracing.WithParentStat(u.stats),
	)
	defer deferFn()

	blockHeadersBytes, err := util.DoHTTPRequest(ctx, fmt.Sprintf("%s/headers/%s", baseUrl, hash.String()))
	if err != nil {
		return nil, errors.NewProcessingError("[getBlockHeaders][%s] failed to get block headers from peer", hash.String(), err)
	}

	blockHeaders := make([]*model.BlockHeader, 0, len(blockHeadersBytes)/model.BlockHeaderSize)

	var blockHeader *model.BlockHeader
	for i := 0; i < len(blockHeadersBytes); i += model.BlockHeaderSize {
		blockHeader, err = model.NewBlockHeaderFromBytes(blockHeadersBytes[i : i+model.BlockHeaderSize])
		if err != nil {
			return nil, errors.NewProcessingError("[getBlockHeaders][%s] failed to create block header from bytes", hash.String(), err)
		}
		blockHeaders = append(blockHeaders, blockHeader)
	}

	return blockHeaders, nil
}

func (u *Server) catchup(ctx context.Context, fromBlock *model.Block, baseURL string) error {
	ctx, _, deferFn := tracing.StartTracing(ctx, "catchup",
		tracing.WithParentStat(u.stats),
		tracing.WithHistogram(prometheusBlockValidationCatchup),
		tracing.WithLogMessage(u.logger, "[catchup][%s] catching up on server %s", fromBlock.Hash().String(), baseURL),
	)
	defer deferFn()

	// first check whether this block already exists, which would mean we caught up from another peer
	exists, err := u.blockValidation.GetBlockExists(ctx, fromBlock.Hash())
	if err != nil {
		return errors.NewServiceError("[catchup][%s] failed to check if block exists", fromBlock.Hash().String(), err)
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
		blockHeaders, err = u.getBlockHeaders(ctx, fromBlockHeaderHash, baseURL)
		if err != nil {
			return err
		}

		if len(blockHeaders) == 0 {
			return errors.NewServiceError("[catchup][%s] failed to get block headers from [%s]", fromBlock.Hash().String(), fromBlockHeaderHash.String())
		}

		for _, blockHeader := range blockHeaders {
			// check if parent block is currently being validated, then wait for it to finish. If the parent block was being validated, when the for loop is done, GetBlockExists will return true.
			blockBeingFinalized := u.blockValidation.blockHashesCurrentlyValidated.Exists(*blockHeader.HashPrevBlock) ||
				u.blockValidation.blockBloomFiltersBeingCreated.Exists(*blockHeader.HashPrevBlock)

			if blockBeingFinalized {
				u.logger.Infof("[catchup][%s] parent block is being validated (hash: %s), waiting for it to finish: %v - %v", fromBlock.Hash().String(), blockHeader.HashPrevBlock.String(), u.blockValidation.blockHashesCurrentlyValidated.Exists(*blockHeader.HashPrevBlock), u.blockValidation.blockBloomFiltersBeingCreated.Exists(*blockHeader.HashPrevBlock))
				retries := 0
				for {
					blockBeingFinalized = u.blockValidation.blockHashesCurrentlyValidated.Exists(*blockHeader.HashPrevBlock) ||
						u.blockValidation.blockBloomFiltersBeingCreated.Exists(*blockHeader.HashPrevBlock)

					if !blockBeingFinalized {
						break
					}

					if (retries % 10) == 0 {
						u.logger.Infof("[catchup][%s] parent block is still (%d) being validated (hash: %s), waiting for it to finish: %v - %v", fromBlock.Hash().String(), retries, blockHeader.HashPrevBlock.String(), u.blockValidation.blockHashesCurrentlyValidated.Exists(*blockHeader.HashPrevBlock), u.blockValidation.blockBloomFiltersBeingCreated.Exists(*blockHeader.HashPrevBlock))
					}

					time.Sleep(1 * time.Second)
					retries++
				}
				u.logger.Infof("[catchup][%s] parent block is done being validated", fromBlock.Hash().String())
			}

			exists, err = u.blockValidation.GetBlockExists(ctx, blockHeader.Hash())
			if err != nil {
				return errors.NewServiceError("[catchup][%s] failed to check if parent block exists", fromBlock.Hash().String(), err)
			}

			if exists {
				break LOOP
			}
			u.logger.Warnf("[catchup][%s] parent block does not exist [%s]", fromBlock.Hash().String(), blockHeader.String())

			catchupBlockHeaders = append(catchupBlockHeaders, blockHeader)

			fromBlockHeaderHash = blockHeader.HashPrevBlock
			// TODO: check if its only useful for a chain with different genesis block?
			if fromBlockHeaderHash.IsEqual(&chainhash.Hash{}) {
				return errors.NewProcessingError("[catchup][%s] failed to find parent block header, last was: %s", fromBlock.Hash().String(), blockHeader.String())
			}
		}
	}

	u.logger.Infof("[catchup][%s] catching up (%d blocks) from [%s] to [%s]", fromBlock.Hash().String(), len(catchupBlockHeaders), catchupBlockHeaders[len(catchupBlockHeaders)-1].String(), catchupBlockHeaders[0].String())

	validateBlocksChan := make(chan *model.Block, len(catchupBlockHeaders))

	catchupConcurrency, _ := gocore.Config().GetInt("blockvalidation_catchupConcurrency", util.Max(4, runtime.NumCPU()/2))

	size := atomic.Uint32{}

	// process the catchup block headers in reverse order and put them on the channel
	// this will allow the blocks to be validated while getting them from the other node
	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(catchupConcurrency)
	g.Go(func() error {
		slices.Reverse(catchupBlockHeaders)
		batches := getBlockBatchGets(catchupBlockHeaders, 100)

		u.logger.Debugf("[catchup][%s] getting %d batches", fromBlock.Hash().String(), len(batches))

		blockCount := 0
		i := 0
		var blocks []*model.Block
		for _, batch := range batches {
			batch := batch
			i++
			u.logger.Debugf("[catchup][%s] [batch %d] getting %d blocks from %s", fromBlock.Hash().String(), i, batch.size, batch.hash.String())

			size.Add(batch.size)

			blocks, err = u.getBlocks(gCtx, &batch.hash, batch.size, baseURL)
			if err != nil {
				// TODO
				// we aren't waiting for the func to finish so we never catch this error and log it
				u.logger.Errorf("[catchup][%s] failed to get %d blocks [%s]:%v", fromBlock.Hash().String(), batch.size, batch.hash.String(), err)
				return errors.NewProcessingError("[catchup][%s] failed to get %d blocks [%s]", fromBlock.Hash().String(), batch.size, batch.hash.String(), err)
			}
			if uint32(len(blocks)) != batch.size {
				u.logger.Warnf("[catchup][%s] got %d blocks, expected %d", fromBlock.Hash().String(), len(blocks), batch.size)
			}

			u.logger.Debugf("[catchup][%s] got %d blocks from %s", fromBlock.Hash().String(), len(blocks), batch.hash.String())

			// reverse the blocks, so they are in the correct order, we get them newest to oldest from the other node
			slices.Reverse(blocks)
			for _, block := range blocks {
				blockCount++
				validateBlocksChan <- block
			}
		}

		u.logger.Infof("[catchup][%s] added %d blocks for validating", fromBlock.Hash().String(), blockCount)

		// close the channel to signal that all blocks have been processed
		close(validateBlocksChan)

		return nil
	})

	i := 0
	// validate the blocks while getting them from the other node
	// this will block until all blocks are validated
	for block := range validateBlocksChan {
		i++
		u.logger.Infof("[catchup][%s] validating block %d/%d", block.Hash().String(), i, size.Load())

		if err = u.blockValidation.ValidateBlock(ctx, block, baseURL, u.blockValidation.bloomFilterStats); err != nil {
			return errors.NewServiceError("[catchup][%s] failed block validation BlockFound [%s]", fromBlock.Hash().String(), block.String(), err)
		}
		u.logger.Debugf("[catchup][%s] validated block %d/%d", block.Hash().String(), i, size.Load())
	}

	u.logger.Infof("[catchup][%s] done validating catchup blocks", fromBlock.Hash().String())

	return nil
}

type blockBatchGet struct {
	hash chainhash.Hash
	size uint32
}

func getBlockBatchGets(catchupBlockHeaders []*model.BlockHeader, batchSize int) []blockBatchGet {
	batches := make([]blockBatchGet, 0)

	var useBlockHeaders []*model.BlockHeader
	for i := 0; i < len(catchupBlockHeaders); i += batchSize {
		start := i
		end := i + batchSize
		if end > len(catchupBlockHeaders)-1 {
			useBlockHeaders = catchupBlockHeaders[start:]
		} else {
			useBlockHeaders = catchupBlockHeaders[start:end]
		}

		lastHash := useBlockHeaders[len(useBlockHeaders)-1].Hash()
		batches = append(batches, blockBatchGet{
			hash: *lastHash,
			size: uint32(len(useBlockHeaders)),
		})
	}

	return batches
}

func (u *Server) SubtreeFound(_ context.Context, req *blockvalidation_api.SubtreeFoundRequest) (*blockvalidation_api.EmptyMessage, error) {
	// TODO - Delete or resurrect...

	// subtreeHash, err := chainhash.NewHash(req.Hash)
	// if err != nil {
	// 	return nil, errors.NewError("[SubtreeFound][%s] failed to create subtree hash from bytes", utils.ReverseAndHexEncodeSlice(req.Hash), err)
	// }

	// u.subtreeFoundQueue.enqueue(&queueItem{
	// 	hash:    *subtreeHash,
	// 	baseURL: req.GetBaseUrl(),
	// })

	return &blockvalidation_api.EmptyMessage{}, nil
}

func (u *Server) Get(ctx context.Context, request *blockvalidation_api.GetSubtreeRequest) (*blockvalidation_api.GetSubtreeResponse, error) {
	start, stat, ctx := tracing.NewStatFromContext(ctx, "Get", u.stats)
	defer func() {
		stat.AddTime(start)
	}()

	subtree, err := u.subtreeStore.Get(ctx, request.Hash, options.WithFileExtension("subtree"))
	if err != nil {
		return nil, errors.WrapGRPC(errors.NewStorageError("failed to get subtree: %s", utils.ReverseAndHexEncodeSlice(request.Hash), err))
	}

	return &blockvalidation_api.GetSubtreeResponse{
		Subtree: subtree,
	}, nil
}

func (u *Server) Exists(ctx context.Context, request *blockvalidation_api.ExistsSubtreeRequest) (*blockvalidation_api.ExistsSubtreeResponse, error) {
	start, stat, ctx := tracing.NewStatFromContext(ctx, "Exists", u.stats)
	defer func() {
		stat.AddTime(start)
	}()

	hash := chainhash.Hash(request.Hash)
	exists, err := u.blockValidation.GetSubtreeExists(ctx, &hash)
	if err != nil {
		return nil, errors.WrapGRPC(errors.NewServiceError("failed to check if subtree exists: %s", hash.String(), err))
	}

	return &blockvalidation_api.ExistsSubtreeResponse{
		Exists: exists,
	}, nil
}

func (u *Server) SetTxMeta(ctx context.Context, request *blockvalidation_api.SetTxMetaRequest) (*blockvalidation_api.SetTxMetaResponse, error) {
	start, stat, _ := tracing.NewStatFromContext(ctx, "SetTxMeta", u.stats)
	defer func() {
		stat.AddTime(start)
	}()

	// number of items added
	prometheusBlockValidationSetTXMetaCache.Add(float64(len(request.Data)))

	// queue size
	prometheusBlockValidationSetTxMetaQueueCh.Inc()

	u.SetTxMetaQ.Enqueue(request.Data)

	return &blockvalidation_api.SetTxMetaResponse{
		Ok: true,
	}, nil
}

func (u *Server) DelTxMeta(ctx context.Context, request *blockvalidation_api.DelTxMetaRequest) (*blockvalidation_api.DelTxMetaResponse, error) {
	start, stat, ctx := tracing.NewStatFromContext(ctx, "SetTxMeta", u.stats)
	defer func() {
		stat.AddTime(start)
	}()

	prometheusBlockValidationSetTXMetaCacheDel.Inc()
	hash, err := chainhash.NewHash(request.Hash[:])
	if err != nil {
		return nil, errors.WrapGRPC(errors.NewProcessingError("failed to create hash from bytes", err))
	}

	if err = u.blockValidation.DelTxMetaCacheMulti(ctx, hash); err != nil {
		u.logger.Errorf("failed to delete tx meta data: %v", err)
	}

	return &blockvalidation_api.DelTxMetaResponse{
		Ok: true,
	}, nil
}

func (u *Server) SetMinedMulti(ctx context.Context, request *blockvalidation_api.SetMinedMultiRequest) (*blockvalidation_api.SetMinedMultiResponse, error) {
	start, stat, ctx := tracing.NewStatFromContext(ctx, "SetMinedMulti", u.stats)
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
		return nil, errors.WrapGRPC(errors.NewProcessingError("failed to set tx meta data: %v", err))
	}

	return &blockvalidation_api.SetMinedMultiResponse{
		Ok: true,
	}, nil
}
