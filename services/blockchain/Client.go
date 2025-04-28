// Package blockchain provides functionality for managing the Bitcoin blockchain.
package blockchain

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/services/blockchain/blockchain_api"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/google/uuid"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/go-utils"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

// clientSubscriber represents a subscriber to blockchain notifications.
type clientSubscriber struct {
	source string                            // Source identifier of the subscriber
	ch     chan *blockchain_api.Notification // Channel for receiving notifications
	id     string                            // Unique identifier for the subscriber
}

// Client represents a blockchain service client.
type Client struct {
	client        blockchain_api.BlockchainAPIClient // gRPC client for blockchain service
	logger        ulogger.Logger                     // Logger instance
	settings      *settings.Settings                 // Configuration settings
	running       *atomic.Bool                       // Flag indicating if client is running
	conn          *grpc.ClientConn                   // gRPC connection
	fmsState      atomic.Pointer[FSMStateType]       // Current FSM state
	subscribers   []clientSubscriber                 // List of subscribers
	subscribersMu sync.Mutex                         // Mutex for subscribers list
}

// BestBlockHeader represents the best block header in the blockchain.
type BestBlockHeader struct {
	Header *model.BlockHeader // Block header
	Height uint32             // Block height
}

// Notification is an alias for blockchain_api.Notification
type Notification = blockchain_api.Notification

// NotificationMetadata is an alias for blockchain_api.NotificationMetadata
type NotificationMetadata = blockchain_api.NotificationMetadata

// FSMStateType is an alias for blockchain_api.FSMStateType
type FSMStateType = blockchain_api.FSMStateType

// FSMEventType is an alias for blockchain_api.FSMEventType
type FSMEventType = blockchain_api.FSMEventType

const (
	FSMStateIDLE           = blockchain_api.FSMStateType_IDLE
	FSMStateRUNNING        = blockchain_api.FSMStateType_RUNNING
	FSMStateCATCHINGBLOCKS = blockchain_api.FSMStateType_CATCHINGBLOCKS
	FSMStateLEGACYSYNCING  = blockchain_api.FSMStateType_LEGACYSYNCING

	FSMEventIDLE          = blockchain_api.FSMEventType_STOP
	FSMEventRUN           = blockchain_api.FSMEventType_RUN
	FSMEventCATCHUPBLOCKS = blockchain_api.FSMEventType_CATCHUPBLOCKS
	FSMEventLEGACYSYNC    = blockchain_api.FSMEventType_LEGACYSYNC
)

// NewClient creates a new blockchain client with default address settings.
func NewClient(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, source string) (ClientI, error) {
	logger = logger.New("blkcC")

	blockchainGrpcAddress := tSettings.BlockChain.GRPCAddress
	if blockchainGrpcAddress == "" {
		return nil, errors.NewConfigurationError("no blockchain_grpcAddress setting found")
	}

	return NewClientWithAddress(ctx, logger, tSettings, blockchainGrpcAddress, source)
}

// NewClientWithAddress creates a new blockchain client with a specified address.
func NewClientWithAddress(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, address string, source string) (ClientI, error) {
	var err error

	var baConn *grpc.ClientConn

	var baClient blockchain_api.BlockchainAPIClient

	// retry a few times to connect to the blockchain service
	maxRetries := tSettings.BlockChain.MaxRetries
	retrySleep := tSettings.BlockChain.RetrySleep

	retries := 0

	for {
		baConn, err = util.GetGRPCClient(ctx, address, &util.ConnectionOptions{
			MaxRetries: 3,
		}, tSettings)
		if err != nil {
			return nil, errors.NewServiceError("failed to init blockchain service connection for '%s'", source, err)
		}

		baClient = blockchain_api.NewBlockchainAPIClient(baConn)

		_, err = baClient.HealthGRPC(ctx, &emptypb.Empty{})
		if err != nil {
			if retries < maxRetries {
				retries++
				backoff := time.Duration(retries*retrySleep) * time.Millisecond
				logger.Debugf("[Blockchain] failed to connect to blockchain service for '%s', retrying %d in %s: %v", source, retries, backoff, err)
				time.Sleep(backoff)

				continue
			}

			logger.Errorf("[Blockchain] failed to connect to blockchain service for '%s', retried %d times: %v", source, maxRetries, err)

			return nil, err
		}

		break
	}

	running := atomic.Bool{}
	running.Store(true)

	c := &Client{
		client:      blockchain_api.NewBlockchainAPIClient(baConn),
		logger:      logger,
		settings:    tSettings,
		running:     &running,
		conn:        baConn,
		subscribers: make([]clientSubscriber, 0),
	}

	// start a subscription to the blockchain service
	subscriptionCh, err := c.SubscribeToServer(ctx, source)
	if err != nil {
		return nil, err
	}

	// start a go routine to listen for notifications
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case notification := <-subscriptionCh:
				if notification == nil {
					continue
				}

				c.logger.Debugf("[Blockchain] Received notification for %s: %s", source, notification.Stringify())

				switch notification.Type {
				case model.NotificationType_FSMState:
					c.logger.Infof("[Blockchain] Received FSM state notification for %s: %s", source, notification.GetMetadata().String())
					// update the local FSM state variable
					metadata := notification.Metadata.Metadata
					newState := FSMStateType(blockchain_api.FSMStateType_value[metadata["destination"]])
					c.fmsState.Store(&newState)
					c.logger.Infof("[Blockchain] Updated FSM state in c.fsmState: %s ", c.fmsState.Load())
				default:
					// send the notification to all subscribers
					c.subscribersMu.Lock()
					for _, s := range c.subscribers {
						go func(ch chan *blockchain_api.Notification, notification *blockchain_api.Notification) {
							utils.SafeSend(ch, notification)
						}(s.ch, notification)
					}
					c.subscribersMu.Unlock()
				}
			}
		}
	}()

	return c, nil
}

// Health checks the health status of the blockchain client.
func (c *Client) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	if checkLiveness {
		// Add liveness checks here. Don't include dependency checks.
		// If the service is stuck return http.StatusServiceUnavailable
		// to indicate a restart is needed
		return http.StatusOK, "OK", nil
	}

	// Add readiness checks here. Include dependency checks.
	// If any dependency is not ready, return http.StatusServiceUnavailable
	// If all dependencies are ready, return http.StatusOK
	// A failed dependency check does not imply the service needs restarting
	resp, err := c.client.HealthGRPC(ctx, &emptypb.Empty{})
	if err != nil || !resp.GetOk() {
		return http.StatusFailedDependency, resp.GetDetails(), errors.UnwrapGRPC(err)
	}

	return http.StatusOK, resp.GetDetails(), nil
}

// AddBlock sends a request to add a new block to the blockchain.
func (c *Client) AddBlock(ctx context.Context, block *model.Block, peerID string) error {
	external := peerID != ""
	req := &blockchain_api.AddBlockRequest{
		Header:           block.Header.Bytes(),
		CoinbaseTx:       block.CoinbaseTx.Bytes(),
		SubtreeHashes:    make([][]byte, 0, len(block.Subtrees)),
		TransactionCount: block.TransactionCount,
		SizeInBytes:      block.SizeInBytes,
		External:         external,
		PeerId:           peerID,
	}

	for _, subtreeHash := range block.Subtrees {
		req.SubtreeHashes = append(req.SubtreeHashes, subtreeHash[:])
	}

	if _, err := c.client.AddBlock(ctx, req); err != nil {
		return errors.UnwrapGRPC(err)
	}

	return nil
}

// GetBlock retrieves a block by its hash.
func (c *Client) GetBlock(ctx context.Context, blockHash *chainhash.Hash) (*model.Block, error) {
	resp, err := c.client.GetBlock(ctx, &blockchain_api.GetBlockRequest{
		Hash: blockHash[:],
	})
	if err != nil {
		return nil, errors.UnwrapGRPC(err)
	}

	header, err := model.NewBlockHeaderFromBytes(resp.Header)
	if err != nil {
		return nil, errors.NewProcessingError("[Blockchain:GetBlock][%s] error parsing block header from bytes", blockHash.String(), err)
	}

	var coinbaseTx *bt.Tx

	if len(resp.CoinbaseTx) > 0 {
		coinbaseTx, err = bt.NewTxFromBytes(resp.CoinbaseTx)
		if err != nil {
			return nil, errors.NewProcessingError("[Blockchain:GetBlock][%s] error parsing coinbase tx from bytes", blockHash.String(), err)
		}
	} else {
		c.logger.Warnf("[Blockchain:GetBlock][%s] coinbase tx is empty for block", blockHash.String())
	}

	subtreeHashes := make([]*chainhash.Hash, 0, len(resp.SubtreeHashes))

	for _, subtreeHash := range resp.SubtreeHashes {
		hash, err := chainhash.NewHash(subtreeHash)
		if err != nil {
			return nil, errors.NewProcessingError("[Blockchain:GetBlock][%s] error parsing subtree hash from bytes", blockHash.String(), err)
		}

		subtreeHashes = append(subtreeHashes, hash)
	}

	return model.NewBlock(header, coinbaseTx, subtreeHashes, resp.TransactionCount, resp.SizeInBytes, resp.Height, resp.Id, c.settings)
}

// GetBlocks retrieves multiple blocks starting from a specific hash.
func (c *Client) GetBlocks(ctx context.Context, blockHash *chainhash.Hash, numberOfBlocks uint32) ([]*model.Block, error) {
	resp, err := c.client.GetBlocks(ctx, &blockchain_api.GetBlocksRequest{
		Hash:  blockHash[:],
		Count: numberOfBlocks,
	})
	if err != nil {
		return nil, errors.UnwrapGRPC(err)
	}

	blocks := make([]*model.Block, 0, len(resp.Blocks))

	for _, blockBytes := range resp.Blocks {
		block, err := model.NewBlockFromBytes(blockBytes, c.settings)
		if err != nil {
			return nil, err
		}

		blocks = append(blocks, block)
	}

	return blocks, nil
}

// GetBlockByHeight retrieves a block at a specific height in the blockchain.
func (c *Client) GetBlockByHeight(ctx context.Context, height uint32) (*model.Block, error) {
	resp, err := c.client.GetBlockByHeight(ctx, &blockchain_api.GetBlockByHeightRequest{
		Height: height,
	})
	if err != nil {
		return nil, errors.UnwrapGRPC(err)
	}

	return c.blockFromResponse(resp)
}

// GetBlockByID retrieves a block by its ID.
func (c *Client) GetBlockByID(ctx context.Context, id uint64) (*model.Block, error) {
	resp, err := c.client.GetBlockByID(ctx, &blockchain_api.GetBlockByIDRequest{
		Id: id,
	})
	if err != nil {
		return nil, errors.UnwrapGRPC(err)
	}

	return c.blockFromResponse(resp)
}

func (c *Client) blockFromResponse(resp *blockchain_api.GetBlockResponse) (*model.Block, error) {
	header, err := model.NewBlockHeaderFromBytes(resp.Header)
	if err != nil {
		return nil, err
	}

	coinbaseTx, err := bt.NewTxFromBytes(resp.CoinbaseTx)
	if err != nil {
		return nil, err
	}

	subtreeHashes := make([]*chainhash.Hash, 0, len(resp.SubtreeHashes))

	for _, subtreeHash := range resp.SubtreeHashes {
		hash, err := chainhash.NewHash(subtreeHash)
		if err != nil {
			return nil, err
		}

		subtreeHashes = append(subtreeHashes, hash)
	}

	return model.NewBlock(header, coinbaseTx, subtreeHashes, resp.TransactionCount, resp.SizeInBytes, resp.Height, resp.Id, c.settings)
}

// GetBlockStats retrieves statistical information about the blockchain.
func (c *Client) GetBlockStats(ctx context.Context) (*model.BlockStats, error) {
	resp, err := c.client.GetBlockStats(ctx, &emptypb.Empty{})

	unwrappedErr := errors.UnwrapGRPC(err)
	if unwrappedErr == nil {
		return resp, nil
	}

	return resp, unwrappedErr
}

// GetBlockGraphData retrieves data points for blockchain visualization.
func (c *Client) GetBlockGraphData(ctx context.Context, periodMillis uint64) (*model.BlockDataPoints, error) {
	resp, err := c.client.GetBlockGraphData(ctx, &blockchain_api.GetBlockGraphDataRequest{
		PeriodMillis: periodMillis,
	})

	unwrappedErr := errors.UnwrapGRPC(err)
	if unwrappedErr == nil {
		return resp, nil
	}

	return resp, unwrappedErr
}

// GetLastNBlocks retrieves the most recent N blocks from the blockchain.
func (c *Client) GetLastNBlocks(ctx context.Context, n int64, includeOrphans bool, fromHeight uint32) ([]*model.BlockInfo, error) {
	resp, err := c.client.GetLastNBlocks(ctx, &blockchain_api.GetLastNBlocksRequest{
		NumberOfBlocks: n,
		IncludeOrphans: includeOrphans,
		FromHeight:     fromHeight,
	})
	if err != nil {
		return nil, errors.UnwrapGRPC(err)
	}

	return resp.Blocks, nil
}

func (c *Client) GetLastNInvalidBlocks(ctx context.Context, n int64) ([]*model.BlockInfo, error) {
	resp, err := c.client.GetLastNInvalidBlocks(ctx, &blockchain_api.GetLastNInvalidBlocksRequest{
		N: n,
	})
	if err != nil {
		return nil, errors.UnwrapGRPC(err)
	}

	return resp.Blocks, nil
}

// GetSuitableBlock finds a suitable block for mining purposes.
func (c *Client) GetSuitableBlock(ctx context.Context, blockHash *chainhash.Hash) (*model.SuitableBlock, error) {
	resp, err := c.client.GetSuitableBlock(ctx, &blockchain_api.GetSuitableBlockRequest{
		Hash: blockHash[:],
	})
	if err != nil {
		return nil, errors.UnwrapGRPC(err)
	}

	return resp.Block, nil
}

// GetHashOfAncestorBlock retrieves the hash of an ancestor block at a specific depth.
func (c *Client) GetHashOfAncestorBlock(ctx context.Context, blockHash *chainhash.Hash, depth int) (*chainhash.Hash, error) {
	depthUint32, err := util.SafeIntToUint32(depth)
	if err != nil {
		return nil, err
	}

	resp, err := c.client.GetHashOfAncestorBlock(ctx, &blockchain_api.GetHashOfAncestorBlockRequest{
		Hash:  blockHash[:],
		Depth: depthUint32,
	})
	if err != nil {
		return nil, errors.UnwrapGRPC(err)
	}

	hash, err := chainhash.NewHash(resp.Hash)
	if err != nil {
		return nil, err
	}

	return hash, nil
}

// GetNextWorkRequired calculates the required proof of work for the next block.
func (c *Client) GetNextWorkRequired(ctx context.Context, blockHash *chainhash.Hash) (*model.NBit, error) {
	resp, err := c.client.GetNextWorkRequired(ctx, &blockchain_api.GetNextWorkRequiredRequest{
		BlockHash: blockHash[:],
	})
	if err != nil {
		return nil, errors.UnwrapGRPC(err)
	}

	bits, err := model.NewNBitFromSlice(resp.Bits)

	return bits, err
}

// GetBlockExists checks if a block with the given hash exists in the blockchain.
func (c *Client) GetBlockExists(ctx context.Context, blockHash *chainhash.Hash) (bool, error) {
	resp, err := c.client.GetBlockExists(ctx, &blockchain_api.GetBlockRequest{
		Hash: blockHash[:],
	})
	if err != nil {
		return false, errors.UnwrapGRPC(err)
	}

	return resp.Exists, nil
}

// GetBestBlockHeader retrieves the header of the current best block.
func (c *Client) GetBestBlockHeader(ctx context.Context) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	resp, err := c.client.GetBestBlockHeader(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, nil, errors.UnwrapGRPC(err)
	}

	header, err := model.NewBlockHeaderFromBytes(resp.BlockHeader)
	if err != nil {
		return nil, nil, err
	}

	meta := &model.BlockHeaderMeta{
		Height:      resp.Height,
		TxCount:     resp.TxCount,
		SizeInBytes: resp.SizeInBytes,
		Miner:       resp.Miner,
		BlockTime:   resp.BlockTime,
		Timestamp:   resp.Timestamp,
		ChainWork:   resp.ChainWork,
	}

	return header, meta, nil
}

// CheckBlockIsInCurrentChain checks if ANY of the given blockIDs is in the current chain.
// It will return true if at least of the blockIDs is in the current chain.
// It will return false if none of the blockIDs is in the current chain.
func (c *Client) CheckBlockIsInCurrentChain(ctx context.Context, blockIDs []uint32) (bool, error) {
	resp, err := c.client.CheckBlockIsInCurrentChain(ctx, &blockchain_api.CheckBlockIsCurrentChainRequest{
		BlockIDs: blockIDs,
	})
	if err != nil {
		return false, errors.UnwrapGRPC(err)
	}

	return resp.GetIsPartOfCurrentChain(), nil
}

// GetBlockHeader retrieves the header of a specific block.
func (c *Client) GetBlockHeader(ctx context.Context, blockHash *chainhash.Hash) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	resp, err := c.client.GetBlockHeader(ctx, &blockchain_api.GetBlockHeaderRequest{
		BlockHash: blockHash[:],
	})

	if err != nil {
		return nil, nil, errors.UnwrapGRPC(err)
	}

	header, err := model.NewBlockHeaderFromBytes(resp.BlockHeader)
	if err != nil {
		return nil, nil, err
	}

	meta := &model.BlockHeaderMeta{
		Height:      resp.Height,
		TxCount:     resp.TxCount,
		SizeInBytes: resp.SizeInBytes,
		Miner:       resp.Miner,
		BlockTime:   resp.BlockTime,
		Timestamp:   resp.Timestamp,
	}

	return header, meta, nil
}

// GetBlockHeaders retrieves multiple block headers starting from a specific hash.
func (c *Client) GetBlockHeaders(ctx context.Context, blockHash *chainhash.Hash, numberOfHeaders uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	resp, err := c.client.GetBlockHeaders(ctx, &blockchain_api.GetBlockHeadersRequest{
		StartHash:       blockHash.CloneBytes(),
		NumberOfHeaders: numberOfHeaders,
	})
	if err != nil {
		return nil, nil, errors.UnwrapGRPC(err)
	}

	return c.returnBlockHeaders(resp)
}

func (c *Client) GetBlockHeadersToCommonAncestor(ctx context.Context, hashTarget *chainhash.Hash, blockLocatorHashes []*chainhash.Hash) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	locatorBytes := make([][]byte, 0, len(blockLocatorHashes))
	for _, hash := range blockLocatorHashes {
		locatorBytes = append(locatorBytes, hash.CloneBytes())
	}

	resp, err := c.client.GetBlockHeadersToCommonAncestor(ctx, &blockchain_api.GetBlockHeadersToCommonAncestorRequest{
		TargetHash:         hashTarget.CloneBytes(),
		BlockLocatorHashes: locatorBytes,
	})
	if err != nil {
		return nil, nil, errors.UnwrapGRPC(err)
	}

	return c.returnBlockHeaders(resp)
}

// GetBlockHeadersFromTill retrieves block headers between two specified blocks.
func (c *Client) GetBlockHeadersFromTill(ctx context.Context, blockHashFrom *chainhash.Hash, blockHashTill *chainhash.Hash) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	resp, err := c.client.GetBlockHeadersFromTill(ctx, &blockchain_api.GetBlockHeadersFromTillRequest{
		StartHash: blockHashFrom.CloneBytes(),
		EndHash:   blockHashTill.CloneBytes(),
	})
	if err != nil {
		return nil, nil, errors.UnwrapGRPC(err)
	}

	return c.returnBlockHeaders(resp)
}

// returnBlockHeaders is a helper function to process block header responses.
func (c *Client) returnBlockHeaders(resp *blockchain_api.GetBlockHeadersResponse) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	headers := make([]*model.BlockHeader, 0, len(resp.BlockHeaders))

	for _, headerBytes := range resp.BlockHeaders {
		header, err := model.NewBlockHeaderFromBytes(headerBytes)
		if err != nil {
			return nil, nil, err
		}

		headers = append(headers, header)
	}

	metas := make([]*model.BlockHeaderMeta, 0, len(resp.Metas))

	for _, metaBytes := range resp.Metas {
		header, err := model.NewBlockHeaderMetaFromBytes(metaBytes)
		if err != nil {
			return nil, nil, err
		}

		metas = append(metas, header)
	}

	return headers, metas, nil
}

// GetBlockHeadersFromHeight retrieves block headers starting from a specific height.
func (c *Client) GetBlockHeadersFromHeight(ctx context.Context, height, limit uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	resp, err := c.client.GetBlockHeadersFromHeight(ctx, &blockchain_api.GetBlockHeadersFromHeightRequest{
		StartHeight: height,
		Limit:       limit,
	})
	if err != nil {
		return nil, nil, errors.UnwrapGRPC(err)
	}

	headers := make([]*model.BlockHeader, 0, len(resp.BlockHeaders))

	for _, headerBytes := range resp.BlockHeaders {
		header, err := model.NewBlockHeaderFromBytes(headerBytes)
		if err != nil {
			return nil, nil, err
		}

		headers = append(headers, header)
	}

	metas := make([]*model.BlockHeaderMeta, 0, len(resp.Metas))

	for _, metaBytes := range resp.Metas {
		meta, err := model.NewBlockHeaderMetaFromBytes(metaBytes)
		if err != nil {
			return nil, nil, err
		}

		metas = append(metas, meta)
	}

	return headers, metas, nil
}

// GetBlockHeadersByHeight retrieves block headers between two specified heights.
func (c *Client) GetBlockHeadersByHeight(ctx context.Context, startHeight, endHeight uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	resp, err := c.client.GetBlockHeadersByHeight(ctx, &blockchain_api.GetBlockHeadersByHeightRequest{
		StartHeight: startHeight,
		EndHeight:   endHeight,
	})
	if err != nil {
		return nil, nil, errors.UnwrapGRPC(err)
	}

	headers := make([]*model.BlockHeader, 0, len(resp.BlockHeaders))

	for _, headerBytes := range resp.BlockHeaders {
		header, err := model.NewBlockHeaderFromBytes(headerBytes)
		if err != nil {
			return nil, nil, err
		}

		headers = append(headers, header)
	}

	metas := make([]*model.BlockHeaderMeta, 0, len(resp.Metas))

	for _, metaBytes := range resp.Metas {
		meta, err := model.NewBlockHeaderMetaFromBytes(metaBytes)
		if err != nil {
			return nil, nil, err
		}

		metas = append(metas, meta)
	}

	return headers, metas, nil
}

// InvalidateBlock marks a block as invalid in the blockchain.
func (c *Client) InvalidateBlock(ctx context.Context, blockHash *chainhash.Hash) error {
	_, err := c.client.InvalidateBlock(ctx, &blockchain_api.InvalidateBlockRequest{
		BlockHash: blockHash.CloneBytes(),
	})

	unwrappedErr := errors.UnwrapGRPC(err)
	if unwrappedErr == nil {
		return nil
	}

	return unwrappedErr
}

// RevalidateBlock restores a previously invalidated block.
func (c *Client) RevalidateBlock(ctx context.Context, blockHash *chainhash.Hash) error {
	_, err := c.client.RevalidateBlock(ctx, &blockchain_api.RevalidateBlockRequest{
		BlockHash: blockHash.CloneBytes(),
	})

	unwrappedErr := errors.UnwrapGRPC(err)
	if unwrappedErr == nil {
		return nil
	}

	return unwrappedErr
}

// GetBlockHeaderIDs retrieves block header IDs starting from a specific hash.
func (c *Client) GetBlockHeaderIDs(ctx context.Context, blockHash *chainhash.Hash, numberOfHeaders uint64) ([]uint32, error) {
	resp, err := c.client.GetBlockHeaderIDs(ctx, &blockchain_api.GetBlockHeadersRequest{
		StartHash:       blockHash.CloneBytes(),
		NumberOfHeaders: numberOfHeaders,
	})
	if err != nil {
		return nil, errors.UnwrapGRPC(err)
	}

	return resp.Ids, nil
}

// SendNotification sends a notification through the blockchain service.
func (c *Client) SendNotification(ctx context.Context, notification *blockchain_api.Notification) error {
	_, err := c.client.SendNotification(ctx, notification)
	if err != nil {
		return errors.UnwrapGRPC(err)
	}

	return nil
}

// Subscribe creates a new subscription to blockchain notifications.
// Returns a channel that will receive notifications until the context is cancelled.
func (c *Client) Subscribe(ctx context.Context, source string) (chan *blockchain_api.Notification, error) {
	// create a new buffered channel for the subscriber
	ch := make(chan *blockchain_api.Notification, 1_000)

	id := uuid.New().String()

	// add the subscriber to the list of subscribers
	c.subscribersMu.Lock()
	c.subscribers = append(c.subscribers, clientSubscriber{
		source: source,
		ch:     ch,
		id:     id,
	})
	c.subscribersMu.Unlock()

	// wait for the context to be done and then remove the subscriber and close the channel
	go func() {
		<-ctx.Done()
		c.logger.Infof("[Blockchain] context done, closing subscription: %s", source)

		c.subscribersMu.Lock()

		// remove from list of subscribers
		for i, s := range c.subscribers {
			if s.id == id {
				c.subscribers = append(c.subscribers[:i], c.subscribers[i+1:]...)

				break
			}
		}

		// TODO close the channel properly without a data race
		// close(ch)

		c.subscribersMu.Unlock()
	}()

	return ch, nil
}

// SubscribeToServer establishes a subscription to the blockchain server.
// Manages reconnection attempts and notification forwarding.
func (c *Client) SubscribeToServer(ctx context.Context, source string) (chan *blockchain_api.Notification, error) {
	ch := make(chan *blockchain_api.Notification)

	go func() {
		<-ctx.Done()
		c.logger.Infof("[Blockchain] server context done, closing subscription: %s", source)
		c.running.Store(false)

		err := c.conn.Close()
		if err != nil {
			c.logger.Errorf("[Blockchain] failed to close connection %v", err)
		}
	}()

	go func() {
		for c.running.Load() {
			c.logger.Infof("[Blockchain] Subscribing to blockchain service: %s", source)

			stream, err := c.client.Subscribe(ctx, &blockchain_api.SubscribeRequest{
				Source: source,
			})
			if err != nil {
				time.Sleep(1 * time.Second)
				continue
			}

			for c.running.Load() {
				resp, err := stream.Recv()
				if err != nil {
					if !strings.Contains(err.Error(), context.Canceled.Error()) {
						c.logger.Warnf("[Blockchain] failed to receive notification: %v", err)
					}

					c.logger.Infof("[Blockchain] retrying subscription in 1 second")

					time.Sleep(1 * time.Second)

					break
				}

				hash, err := chainhash.NewHash(resp.Hash)
				if err != nil {
					c.logger.Errorf("[Blockchain] failed to parse hash", err)
					continue
				}

				utils.SafeSend(ch, &blockchain_api.Notification{
					Type:     resp.Type,
					Hash:     hash[:],
					Base_URL: resp.Base_URL,
					Metadata: resp.Metadata,
				})
			}
		}
	}()

	return ch, nil
}

// GetState retrieves a value from the blockchain state storage by its key.
func (c *Client) GetState(ctx context.Context, key string) ([]byte, error) {
	resp, err := c.client.GetState(ctx, &blockchain_api.GetStateRequest{
		Key: key,
	})
	if err != nil {
		return nil, errors.UnwrapGRPC(err)
	}

	return resp.Data, nil
}

// SetState stores a value in the blockchain state storage with the specified key.
func (c *Client) SetState(ctx context.Context, key string, data []byte) error {
	_, err := c.client.SetState(ctx, &blockchain_api.SetStateRequest{
		Key:  key,
		Data: data,
	})
	if err != nil {
		return errors.UnwrapGRPC(err)
	}

	return nil
}

func (c *Client) GetBlockIsMined(ctx context.Context, blockHash *chainhash.Hash) (bool, error) {
	resp, err := c.client.GetBlockIsMined(ctx, &blockchain_api.GetBlockIsMinedRequest{
		BlockHash: blockHash[:],
	})
	if err != nil {
		return false, errors.UnwrapGRPC(err)
	}

	return resp.IsMined, nil
}

// SetBlockMinedSet marks a block as mined in the blockchain.
func (c *Client) SetBlockMinedSet(ctx context.Context, blockHash *chainhash.Hash) error {
	_, err := c.client.SetBlockMinedSet(ctx, &blockchain_api.SetBlockMinedSetRequest{
		BlockHash: blockHash[:],
	})
	if err != nil {
		return errors.UnwrapGRPC(err)
	}

	return nil
}

// GetBlocksMinedNotSet retrieves blocks that haven't been marked as mined.
func (c *Client) GetBlocksMinedNotSet(ctx context.Context) ([]*model.Block, error) {
	resp, err := c.client.GetBlocksMinedNotSet(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, errors.UnwrapGRPC(err)
	}

	blocks := make([]*model.Block, 0, len(resp.BlockBytes))

	for _, blockBytes := range resp.BlockBytes {
		block, err := model.NewBlockFromBytes(blockBytes, c.settings)
		if err != nil {
			return nil, err
		}

		blocks = append(blocks, block)
	}

	return blocks, nil
}

// SetBlockSubtreesSet marks a block's subtrees as set in the blockchain.
func (c *Client) SetBlockSubtreesSet(ctx context.Context, blockHash *chainhash.Hash) error {
	_, err := c.client.SetBlockSubtreesSet(ctx, &blockchain_api.SetBlockSubtreesSetRequest{
		BlockHash: blockHash[:],
	})
	if err != nil {
		return errors.UnwrapGRPC(err)
	}

	return nil
}

// GetBlocksSubtreesNotSet retrieves blocks whose subtrees haven't been set.
func (c *Client) GetBlocksSubtreesNotSet(ctx context.Context) ([]*model.Block, error) {
	resp, err := c.client.GetBlocksSubtreesNotSet(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, errors.UnwrapGRPC(err)
	}

	blocks := make([]*model.Block, 0, len(resp.BlockBytes))

	for _, blockBytes := range resp.BlockBytes {
		block, err := model.NewBlockFromBytes(blockBytes, c.settings)
		if err != nil {
			return nil, err
		}

		blocks = append(blocks, block)
	}

	return blocks, nil
}

// FSM related endpoints

// GetFSMCurrentState retrieves the current state of the finite state machine.
func (c *Client) GetFSMCurrentState(ctx context.Context) (*FSMStateType, error) {
	currentState := c.fmsState.Load()
	if currentState != nil {
		return currentState, nil
	}

	state, err := c.client.GetFSMCurrentState(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, errors.UnwrapGRPC(err)
	}

	return &state.State, nil
}

// IsFSMCurrentState checks if the current FSM state matches the provided state.
func (c *Client) IsFSMCurrentState(ctx context.Context, state FSMStateType) (bool, error) {
	currentState, err := c.GetFSMCurrentState(ctx)
	if err != nil {
		return false, err
	}

	return *currentState == state, nil
}

// WaitForFSMtoTransitionToGivenState waits for the FSM to reach a specific state.
func (c *Client) WaitForFSMtoTransitionToGivenState(ctx context.Context, targetState FSMStateType) error {
	if _, err := c.client.WaitFSMToTransitionToGivenState(ctx, &blockchain_api.WaitFSMToTransitionRequest{
		State: targetState,
	}); err != nil {
		return errors.UnwrapGRPC(err)
	}

	return nil
}

// WaitUntilFSMTransitionsFromIdleState waits for the FSM to transition from the IDLE state.
func (c *Client) WaitUntilFSMTransitionFromIdleState(ctx context.Context) error {
	c.logger.Infof("[Blockchain Client] Waiting for FSM to transition from IDLE state...")

	// Create a context with cancel function to stop the ticker when we're done
	waitCtx, cancelWait := context.WithCancel(ctx)
	defer cancelWait()

	// Start a ticker to log the waiting message every 10 seconds
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	// Use a goroutine to handle the periodic logging
	go func() {
		for {
			select {
			case <-ticker.C:
				c.logger.Infof("[Blockchain Client] Still waiting for FSM to transition from IDLE state...")
			case <-waitCtx.Done():
				return
			}
		}
	}()

	// Wait for the FSM to transition
	_, err := c.client.WaitUntilFSMTransitionFromIdleState(ctx, &emptypb.Empty{})

	// Cancel the ticker context to stop the logging
	cancelWait()

	if err != nil {
		c.logger.Errorf("[Blockchain Client] Failed to wait for FSM transition from IDLE state: %s", err)
		return err
	}

	// Log the new FSM state
	newState, stateErr := c.client.GetFSMCurrentState(ctx, &emptypb.Empty{})
	if stateErr != nil {
		c.logger.Errorf("[Blockchain Client] Failed to get new FSM state after transition: %s", stateErr)
	} else {
		c.logger.Infof("[Blockchain Client] FSM successfully transitioned from IDLE to %v", newState)
	}

	return nil
}

// GetFSMCurrentStateForE2ETestMode retrieves the current FSM state for end-to-end testing.
func (c *Client) GetFSMCurrentStateForE2ETestMode() FSMStateType {
	ctx := context.Background()

	currentState, err := c.client.GetFSMCurrentState(ctx, &emptypb.Empty{})
	if err != nil {
		c.logger.Errorf("[Blockchain Client] Failed to get FSM current state: %v", err)
		return FSMStateIDLE
	}

	return currentState.State
}

// SendFSMEvent sends an event to the finite state machine.
func (c *Client) SendFSMEvent(ctx context.Context, event blockchain_api.FSMEventType) error {
	c.logger.Infof("[Blockchain Client] Sending FSM event: %v", event)

	if _, err := c.client.SendFSMEvent(ctx, &blockchain_api.SendFSMEventRequest{
		Event: event,
	}); err != nil {
		return errors.UnwrapGRPC(err)
	}

	return nil
}

// Run sends a run FSM event to the blockchain service.
func (c *Client) Run(ctx context.Context, source string) error {
	currentState := ""

	state, _ := c.GetFSMCurrentState(ctx)
	if state != nil {
		// check whether the current state is the same as the target state
		if *state == FSMStateRUNNING {
			return nil
		}

		currentState = state.String()
	}

	c.logger.Infof("[Blockchain Client] Sending Run event %s (%s => Run)", source, currentState)

	_, err := c.client.Run(ctx, &emptypb.Empty{})
	if err != nil {
		return errors.UnwrapGRPC(err)
	}

	return nil
}

// CatchUpBlocks sends a catchup blocks FSM event to the blockchain service.
func (c *Client) CatchUpBlocks(ctx context.Context) error {
	currentState := c.fmsState.Load()
	if currentState != nil {
		// check whether the current state is the same as the target state
		if *currentState == FSMStateCATCHINGBLOCKS {
			return nil
		}
	}

	c.logger.Infof("[Blockchain Client] Sending Catchup Transactions event")

	_, err := c.client.CatchUpBlocks(ctx, &emptypb.Empty{})
	if err != nil {
		return errors.UnwrapGRPC(err)
	}

	return nil
}

// LegacySync sends a legacy sync FSM event to the blockchain service.
func (c *Client) LegacySync(ctx context.Context) error {
	currentState := c.fmsState.Load()
	if currentState != nil {
		// check whether the current state is the same as the target state
		if *currentState == FSMStateLEGACYSYNCING {
			return nil
		}
	}

	c.logger.Infof("[Blockchain Client] Sending Legacy Sync event")

	_, err := c.client.LegacySync(ctx, &emptypb.Empty{})
	if err != nil {
		return errors.UnwrapGRPC(err)
	}

	return nil
}

func (c *Client) Idle(ctx context.Context) error {
	currentState := c.fmsState.Load()
	if currentState != nil {
		// check whether the current state is the same as the target state
		if *currentState == FSMStateIDLE {
			return nil
		}
	}

	c.logger.Infof("[Blockchain Client] Sending IDLE event")

	_, err := c.client.Idle(ctx, &emptypb.Empty{})
	if err != nil {
		return errors.UnwrapGRPC(err)
	}

	return nil
}

//
// Legacy Endpoints
//

// GetBlockLocator returns a block locator for the given block header hash and height.
func (c *Client) GetBlockLocator(ctx context.Context, blockHeaderHash *chainhash.Hash, blockHeaderHeight uint32) ([]*chainhash.Hash, error) {
	req := &blockchain_api.GetBlockLocatorRequest{
		Hash:   blockHeaderHash[:],
		Height: blockHeaderHeight,
	}

	resp, err := c.client.GetBlockLocator(ctx, req)

	if err != nil {
		return nil, errors.UnwrapGRPC(err)
	}

	locator := make([]*chainhash.Hash, 0, len(resp.Locator))

	for _, hash := range resp.Locator {
		h, err := chainhash.NewHash(hash)
		if err != nil {
			return nil, err
		}

		locator = append(locator, h)
	}

	return locator, nil
}

// LocateBlockHeaders finds block headers using a locator.
func (c *Client) LocateBlockHeaders(ctx context.Context, locator []*chainhash.Hash, hashStop *chainhash.Hash, maxHashes uint32) ([]*model.BlockHeader, error) {
	locatorBytes := make([][]byte, 0, len(locator))
	for _, hash := range locator {
		locatorBytes = append(locatorBytes, hash.CloneBytes())
	}

	req := &blockchain_api.LocateBlockHeadersRequest{
		Locator:   locatorBytes,
		HashStop:  hashStop.CloneBytes(),
		MaxHashes: maxHashes,
	}

	resp, err := c.client.LocateBlockHeaders(ctx, req)
	if err != nil {
		return nil, errors.UnwrapGRPC(err)
	}

	blockHeaders := make([]*model.BlockHeader, 0, len(resp.BlockHeaders))

	for _, blockHeaderBytes := range resp.BlockHeaders {
		blockHeader, err := model.NewBlockHeaderFromBytes(blockHeaderBytes)
		if err != nil {
			return nil, err
		}

		blockHeaders = append(blockHeaders, blockHeader)
	}

	return blockHeaders, nil
}

// GetBestHeightAndTime retrieves the current best block height and median time.
func (c *Client) GetBestHeightAndTime(ctx context.Context) (uint32, uint32, error) {
	resp, err := c.client.GetBestHeightAndTime(ctx, &emptypb.Empty{})
	if err != nil {
		return 0, 0, errors.UnwrapGRPC(err)
	}

	return resp.Height, resp.Time, nil
}

// log2FloorMasks defines the masks to use when quickly calculating
// floor(log2(x)) in a constant log2(32) = 5 steps, where x is a uint32, using
// shifts.  They are derived from (2^(2^x) - 1) * (2^(2^x)), for x in 4..0.
var log2FloorMasks = []uint32{0xffff0000, 0xff00, 0xf0, 0xc, 0x2}

// fastLog2Floor calculates and returns floor(log2(x)) in a constant 5 steps.
func fastLog2Floor(n uint32) uint8 {
	rv := uint8(0)
	exponent := uint8(16)

	for i := 0; i < 5; i++ {
		if n&log2FloorMasks[i] != 0 {
			rv += exponent
			n >>= exponent
		}

		exponent >>= 1
	}

	return rv
}
