// Package blockchain provides Bitcoin blockchain management functionality for Teranode.
//
// The service handles block validation, storage, retrieval, and state management
// with FSM coordination. It operates as both gRPC server and client for
// distributed blockchain operations across Teranode instances.
package blockchain

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/google/uuid"
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
//
// Client provides a gRPC-based interface for communicating with the blockchain service,
// enabling remote operations and managing connection lifecycle with subscription support.
type Client struct {
	client                blockchain_api.BlockchainAPIClient // gRPC client for blockchain service
	logger                ulogger.Logger                     // Logger instance
	settings              *settings.Settings                 // Configuration settings
	running               *atomic.Bool                       // Flag indicating if client is running
	conn                  *grpc.ClientConn                   // gRPC connection
	fmsState              atomic.Pointer[FSMStateType]       // Current FSM state
	subscribers           []clientSubscriber                 // List of subscribers
	subscribersMu         sync.Mutex                         // Mutex for subscribers list
	lastBlockNotification *blockchain_api.Notification       // Last block notification received
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
			MaxRetries:   tSettings.GRPCMaxRetries,
			RetryBackoff: tSettings.GRPCRetryBackoff,
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

				// c.logger.Debugf("[Blockchain] Received notification for %s: %s", source, notification.Stringify())

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
					// Store the last block notification for new subscribers
					if notification.Type == model.NotificationType_Block {
						c.lastBlockNotification = notification
					}

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
func (c *Client) AddBlock(ctx context.Context, block *model.Block, peerID string, opts ...options.StoreBlockOption) error {
	storeBlockOptions := options.ProcessStoreBlockOptions(opts...)

	external := peerID != ""
	req := &blockchain_api.AddBlockRequest{
		Header:            block.Header.Bytes(),
		CoinbaseTx:        block.CoinbaseTx.Bytes(),
		SubtreeHashes:     make([][]byte, 0, len(block.Subtrees)),
		TransactionCount:  block.TransactionCount,
		SizeInBytes:       block.SizeInBytes,
		External:          external,
		PeerId:            peerID,
		OptionMinedSet:    storeBlockOptions.MinedSet,
		OptionSubtreesSet: storeBlockOptions.SubtreesSet,
		OptionInvalid:     storeBlockOptions.Invalid,
		OptionID:          storeBlockOptions.ID,
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

	return model.NewBlock(header, coinbaseTx, subtreeHashes, resp.TransactionCount, resp.SizeInBytes, resp.Height, resp.Id)
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
		block, err := model.NewBlockFromBytes(blockBytes)
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

// GetNextBlockID retrieves the next available block ID.
func (c *Client) GetNextBlockID(ctx context.Context) (uint64, error) {
	resp, err := c.client.GetNextBlockID(ctx, &emptypb.Empty{})
	if err != nil {
		return 0, errors.UnwrapGRPC(err)
	}

	return resp.NextBlockId, nil
}

// blockFromResponse converts a gRPC GetBlockResponse into a model.Block.
// This helper method deserializes the various components of a block response
// from the blockchain service and reconstructs them into the internal Block model.
//
// The method performs the following conversions:
// - Deserializes the block header from bytes to BlockHeader
// - Deserializes the coinbase transaction from bytes to Transaction
// - Converts subtree hashes from byte arrays to Hash objects
// - Reconstructs the complete Block with all components and metadata
//
// This conversion is essential for maintaining type safety and providing
// a consistent internal representation of blocks regardless of the transport
// mechanism (gRPC, direct calls, etc.).
//
// Parameters:
//   - resp: The gRPC response containing serialized block data
//
// Returns:
//   - *model.Block: The reconstructed block with all components
//   - error: Any error encountered during deserialization or reconstruction
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

	return model.NewBlock(header, coinbaseTx, subtreeHashes, resp.TransactionCount, resp.SizeInBytes, resp.Height, resp.Id)
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

// GetLastNInvalidBlocks retrieves the most recent N invalid blocks from the blockchain.
// This method provides access to blocks that have been rejected or invalidated during
// the validation process, which is essential for debugging, monitoring, and network
// analysis purposes.
//
// Invalid blocks are those that failed validation checks such as:
// - Proof of work validation failures
// - Block structure or format errors
// - Transaction validation failures within the block
// - Consensus rule violations
// - Timestamp or difficulty target issues
// - Merkle root mismatches or other cryptographic failures
//
// The returned blocks are ordered by their processing time, with the most recently
// invalidated blocks appearing first in the list. This information is valuable for:
// - Network monitoring and attack detection
// - Debugging blockchain synchronization issues
// - Analyzing network health and block rejection patterns
// - Forensic analysis of potential malicious activity
// - Performance analysis of validation processes
//
// The method communicates with the blockchain service via gRPC to retrieve
// invalid block information from the underlying blockchain store.
//
// Parameters:
//   - ctx: Context for the operation with timeout and cancellation support
//   - n: Maximum number of invalid blocks to retrieve (must be positive)
//
// Returns:
//   - []*model.BlockInfo: Slice of invalid block information, ordered by recency
//   - error: Any error encountered during the retrieval process
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
	depthUint32, err := safeconversion.IntToUint32(depth)
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

// GetLatestBlockHeaderFromBlockLocator retrieves the latest block header from a block locator.
func (c *Client) GetLatestBlockHeaderFromBlockLocator(ctx context.Context, bestBlockHash *chainhash.Hash, blockLocator []chainhash.Hash) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	if len(blockLocator) == 0 {
		return nil, nil, errors.NewProcessingError("blockLocator cannot be empty")
	}

	blockLocatorBytes := make([][]byte, len(blockLocator))
	for i, h := range blockLocator {
		blockLocatorBytes[i] = h[:]
	}

	resp, err := c.client.GetLatestBlockHeaderFromBlockLocator(ctx, &blockchain_api.GetLatestBlockHeaderFromBlockLocatorRequest{
		BestBlockHash:      bestBlockHash[:],
		BlockLocatorHashes: blockLocatorBytes,
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

// GetBlockHeadersFromOldest retrieves block headers starting from the oldest block.
func (c *Client) GetBlockHeadersFromOldest(ctx context.Context, chainTipHash, targetHash *chainhash.Hash, numberOfHeaders uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	resp, err := c.client.GetBlockHeadersFromOldest(ctx, &blockchain_api.GetBlockHeadersFromOldestRequest{
		ChainTipHash:    chainTipHash.CloneBytes(),
		TargetHash:      targetHash.CloneBytes(),
		NumberOfHeaders: numberOfHeaders,
	})
	if err != nil {
		return nil, nil, errors.UnwrapGRPC(err)
	}

	return c.returnBlockHeaders(resp)
}

// GetNextWorkRequired calculates the required proof of work for the next block.
func (c *Client) GetNextWorkRequired(ctx context.Context, previousBlockHash *chainhash.Hash, currentBlockTime int64) (*model.NBit, error) {
	if currentBlockTime == 0 {
		return nil, errors.NewProcessingError("currentBlockTime cannot be zero")
	}

	resp, err := c.client.GetNextWorkRequired(ctx, &blockchain_api.GetNextWorkRequiredRequest{
		PreviousBlockHash: previousBlockHash[:],
		CurrentBlockTime:  currentBlockTime,
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

// GetChainTips retrieves information about all known tips in the block tree.
func (c *Client) GetChainTips(ctx context.Context) ([]*model.ChainTip, error) {
	c.logger.Debugf("[Blockchain Client] Getting chain tips")

	resp, err := c.client.GetChainTips(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, errors.UnwrapGRPC(err)
	}

	chainTips := make([]*model.ChainTip, 0, len(resp.Tips))

	for _, tip := range resp.Tips {
		chainTip := &model.ChainTip{
			Height:    tip.Height,
			Hash:      tip.Hash,
			Branchlen: tip.Branchlen,
			Status:    tip.Status,
		}
		chainTips = append(chainTips, chainTip)
	}

	return chainTips, nil
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
		ID:          resp.Id,
		Height:      resp.Height,
		TxCount:     resp.TxCount,
		SizeInBytes: resp.SizeInBytes,
		Miner:       resp.Miner,
		PeerID:      resp.PeerId,
		BlockTime:   resp.BlockTime,
		Timestamp:   resp.Timestamp,
		ChainWork:   resp.ChainWork,
		MinedSet:    resp.MinedSet,
		SubtreesSet: resp.SubtreesSet,
		Invalid:     resp.Invalid,
	}

	if resp.ProcessedAt != nil {
		t := resp.ProcessedAt.AsTime()
		meta.ProcessedAt = &t
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

// GetBlockHeadersToCommonAncestor retrieves block headers from a target hash back to a common ancestor.
// This method implements the Bitcoin protocol's block locator algorithm to find the common
// ancestor between the local chain and a remote peer's chain, then returns the headers
// from the target hash back to that common point.
//
// The method uses a block locator array (similar to Bitcoin's getblocks message) to
// efficiently find the common ancestor without requiring the transmission of the entire
// chain. The locator contains hashes at exponentially increasing intervals, allowing
// for efficient binary-search-like discovery of the fork point.
//
// This functionality is essential for:
// - Blockchain synchronization between peers
// - Identifying chain reorganizations and forks
// - Efficient header-first synchronization protocols
// - P2P network block discovery and validation
//
// The method communicates with the blockchain service via gRPC to perform the
// ancestor search and header retrieval from the underlying blockchain store.
//
// Parameters:
//   - ctx: Context for the operation with timeout and cancellation support
//   - hashTarget: The target block hash to start searching backwards from
//   - blockLocatorHashes: Array of block hashes used to locate the common ancestor
//   - maxHeaders: Maximum number of headers to return (prevents excessive responses)
//
// Returns:
//   - []*model.BlockHeader: Array of block headers from target back to common ancestor
//   - []*model.BlockHeaderMeta: Corresponding metadata for each header
//   - error: Any error encountered during the ancestor search or header retrieval
func (c *Client) GetBlockHeadersToCommonAncestor(ctx context.Context, hashTarget *chainhash.Hash, blockLocatorHashes []*chainhash.Hash, maxHeaders uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	locatorBytes := make([][]byte, 0, len(blockLocatorHashes))
	for _, hash := range blockLocatorHashes {
		locatorBytes = append(locatorBytes, hash.CloneBytes())
	}

	resp, err := c.client.GetBlockHeadersToCommonAncestor(ctx, &blockchain_api.GetBlockHeadersToCommonAncestorRequest{
		TargetHash:         hashTarget.CloneBytes(),
		BlockLocatorHashes: locatorBytes,
		MaxHeaders:         maxHeaders,
	})
	if err != nil {
		return nil, nil, errors.UnwrapGRPC(err)
	}

	return c.returnBlockHeaders(resp)
}

// GetBlockHeadersFromCommonAncestor retrieves block headers from a common ancestor to a target hash.
// This method is used to retrieve block headers starting from a common ancestor
// up to a specified target block hash. It is particularly useful for synchronizing
// block headers between nodes in a peer-to-peer network, allowing them to
// efficiently catch up with the latest blocks without needing to download the entire chain.
//
// The method uses a block locator array to find the common ancestor and then
// retrieves the headers from that point up to the target hash. The block locator
// is a list of block hashes that represent the path from the common ancestor to the target,
// allowing the method to efficiently traverse the chain and return the relevant headers.
//
// This functionality is essential for:
// - Efficiently synchronizing block headers between nodes
// - Implementing header-first synchronization protocols
// - Identifying chain reorganizations and forks
// - Supporting P2P network block discovery and validation
//
// The method communicates with the blockchain service via gRPC to perform the
// ancestor search and header retrieval from the underlying blockchain store.
//
// Parameters:
//   - ctx: Context for the operation with timeout and cancellation support
//   - hashTarget: The target block hash to retrieve headers up to
//   - blockLocatorHashes: Array of block hashes used to locate the common ancestor
//   - maxHeaders: Maximum number of headers to return (prevents excessive responses)
//
// Returns:
//   - []*model.BlockHeader: Array of block headers from common ancestor to target
//   - []*model.BlockHeaderMeta: Corresponding metadata for each header
//   - error: Any error encountered during the ancestor search or header retrieval
func (c *Client) GetBlockHeadersFromCommonAncestor(ctx context.Context, hashTarget *chainhash.Hash, blockLocatorHashes []chainhash.Hash, maxHeaders uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	locatorBytes := make([][]byte, 0, len(blockLocatorHashes))
	for _, hash := range blockLocatorHashes {
		locatorBytes = append(locatorBytes, hash.CloneBytes())
	}

	resp, err := c.client.GetBlockHeadersFromCommonAncestor(ctx, &blockchain_api.GetBlockHeadersFromCommonAncestorRequest{
		TargetHash:         hashTarget.CloneBytes(),
		BlockLocatorHashes: locatorBytes,
		MaxHeaders:         maxHeaders,
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
// This method converts a gRPC GetBlockHeadersResponse into slices of BlockHeader
// and BlockHeaderMeta objects, handling the deserialization of both the headers
// and their associated metadata.
//
// The method performs the following operations:
// - Deserializes each block header from bytes to BlockHeader objects
// - Deserializes each metadata entry from bytes to BlockHeaderMeta objects
// - Maintains the order and correspondence between headers and metadata
// - Handles any deserialization errors gracefully
//
// This helper function is used by multiple client methods that retrieve block
// headers (GetBlockHeaders, GetBlockHeadersToCommonAncestor, etc.) to provide
// consistent response processing and error handling.
//
// Parameters:
//   - resp: The gRPC response containing serialized block headers and metadata
//
// Returns:
//   - []*model.BlockHeader: Array of deserialized block headers
//   - []*model.BlockHeaderMeta: Array of corresponding metadata objects
//   - error: Any error encountered during deserialization
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

// GetBlocksByHeight retrieves full blocks between two specified heights.
// This method fetches complete blocks within a height range in a single efficient operation,
// which is particularly useful for operations like subtree searching where full block data
// including subtree hashes is needed for multiple consecutive blocks.
//
// The method communicates with the blockchain service via gRPC to retrieve the blocks
// from the underlying blockchain store, then deserializes each block into the internal
// model representation with all components (headers, subtrees, transaction metadata).
//
// Parameters:
//   - ctx: Context for the operation with timeout and cancellation support
//   - startHeight: Starting height for block retrieval (inclusive)
//   - endHeight: Ending height for block retrieval (inclusive)
//
// Returns:
//   - []*model.Block: Array of complete blocks in ascending height order
//   - error: Any error encountered during retrieval or deserialization
func (c *Client) GetBlocksByHeight(ctx context.Context, startHeight, endHeight uint32) ([]*model.Block, error) {
	resp, err := c.client.GetBlocksByHeight(ctx, &blockchain_api.GetBlocksByHeightRequest{
		StartHeight: startHeight,
		EndHeight:   endHeight,
	})
	if err != nil {
		return nil, errors.UnwrapGRPC(err)
	}

	blocks := make([]*model.Block, 0, len(resp.Blocks))

	for _, blockBytes := range resp.Blocks {
		block, err := model.NewBlockFromBytes(blockBytes)
		if err != nil {
			return nil, err
		}

		blocks = append(blocks, block)
	}

	return blocks, nil
}

func (c *Client) FindBlocksContainingSubtree(ctx context.Context, subtreeHash *chainhash.Hash, maxBlocks uint32) ([]*model.Block, error) {
	resp, err := c.client.FindBlocksContainingSubtree(ctx, &blockchain_api.FindBlocksContainingSubtreeRequest{
		SubtreeHash: subtreeHash.CloneBytes(),
		MaxBlocks:   maxBlocks,
	})
	if err != nil {
		return nil, errors.UnwrapGRPC(err)
	}

	blocks := make([]*model.Block, 0, len(resp.Blocks))

	for _, blockBytes := range resp.Blocks {
		block, err := model.NewBlockFromBytes(blockBytes)
		if err != nil {
			return nil, err
		}

		blocks = append(blocks, block)
	}

	return blocks, nil
}

// InvalidateBlock marks a block as invalid in the blockchain.
func (c *Client) InvalidateBlock(ctx context.Context, blockHash *chainhash.Hash) ([]chainhash.Hash, error) {
	resp, err := c.client.InvalidateBlock(ctx, &blockchain_api.InvalidateBlockRequest{
		BlockHash: blockHash.CloneBytes(),
	})
	if err != nil {
		return nil, errors.UnwrapGRPC(err)
	}

	if resp == nil {
		return nil, errors.NewProcessingError("invalidate block did not return a valid response")
	}

	invalidatedHashes := make([]chainhash.Hash, 0, len(resp.InvalidatedBlocks))
	for _, hashBytes := range resp.InvalidatedBlocks {
		hash, err := chainhash.NewHash(hashBytes)
		if err != nil {
			return nil, err
		}

		invalidatedHashes = append(invalidatedHashes, *hash)
	}

	return invalidatedHashes, nil
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
// This method fetches a sequence of block header identifiers (uint32 IDs) from
// the blockchain service, beginning with the block identified by the provided
// hash and continuing for the requested number of headers.
//
// Block header IDs are lightweight identifiers used internally by the blockchain
// store for efficient indexing and referencing. This method is particularly
// useful for:
// - Bulk operations that need to reference many blocks efficiently
// - Internal synchronization processes
// - Performance-optimized block processing pipelines
// - Reducing memory overhead when only block references are needed
//
// The returned IDs correspond to the internal database identifiers and can be
// used with other methods that accept block IDs for faster lookups compared
// to hash-based operations.
//
// Parameters:
//   - ctx: Context for the operation with timeout and cancellation support
//   - blockHash: Hash of the starting block for ID retrieval
//   - numberOfHeaders: Number of consecutive block header IDs to retrieve
//
// Returns:
//   - []uint32: Array of block header IDs in sequential order
//   - error: Any error encountered during the ID retrieval operation
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

	// Send the last block notification to the new subscriber if available
	// This ensures new subscribers get the current state immediately
	if c.lastBlockNotification != nil {
		lastNotification := c.lastBlockNotification
		go func() {
			utils.SafeSend(ch, lastNotification)
			c.logger.Debugf("[Blockchain] Sent initial block notification to new subscriber %s", source)
		}()
	}
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
// This method creates a persistent connection to the blockchain service for receiving
// real-time notifications about blockchain events. It manages reconnection attempts
// and notification forwarding to provide reliable event streaming.
//
// The method establishes a gRPC streaming connection to receive notifications about:
// - New block additions to the blockchain
// - Block invalidations and reorganizations
// - FSM state transitions
// - Mining status updates
// - Other blockchain service events
//
// Key features of this subscription mechanism:
// - Automatic reconnection handling for network failures
// - Graceful shutdown when context is cancelled
// - Connection lifecycle management
// - Error handling and logging for debugging
//
// The returned channel will receive notifications until the context is cancelled
// or an unrecoverable error occurs. The method handles connection cleanup
// automatically when the subscription ends.
//
// This is typically used by services that need to react to blockchain events
// in real-time, such as mining coordinators, transaction processors, or
// monitoring systems.
//
// Parameters:
//   - ctx: Context for the operation with timeout and cancellation support
//   - source: Identifier for the subscribing service (used for logging and tracking)
//
// Returns:
//   - chan *blockchain_api.Notification: Channel for receiving blockchain notifications
//   - error: Any error encountered during subscription establishment
func (c *Client) SubscribeToServer(ctx context.Context, source string) (chan *blockchain_api.Notification, error) {
	// Use a buffered channel to prevent blocking on sends
	ch := make(chan *blockchain_api.Notification, 100)

	// Use sync.Once to ensure channel is closed exactly once
	var closeOnce sync.Once

	// Create a done channel to coordinate goroutine shutdown
	done := make(chan struct{})

	go func() {
		<-ctx.Done()
		c.logger.Infof("[Blockchain] server context done, closing subscription: %s", source)
		c.running.Store(false)

		err := c.conn.Close()
		if err != nil {
			c.logger.Errorf("[Blockchain] failed to close connection %v", err)
		}

		// Wait for sender goroutine to exit before closing channel
		<-done

		// Close the channel when context is done
		closeOnce.Do(func() {
			close(ch)
		})
	}()

	go func() {
		defer func() {
			// Signal that sender goroutine is done
			close(done)
		}()

		for c.running.Load() {
			c.logger.Infof("[Blockchain] Subscribing to blockchain service: %s", source)

			stream, err := c.client.Subscribe(ctx, &blockchain_api.SubscribeRequest{
				Source: source,
			})
			if err != nil {
				if !c.running.Load() {
					return
				}

				c.logger.Warnf("[Blockchain] failed to create subscription stream: %v", err)
				time.Sleep(1 * time.Second)
				continue
			}

			for c.running.Load() {
				resp, err := stream.Recv()
				if err != nil {
					if !c.running.Load() || ctx.Err() != nil {
						// Context cancelled or client stopped, exit gracefully
						return
					}

					if !strings.Contains(err.Error(), context.Canceled.Error()) {
						c.logger.Warnf("[Blockchain] failed to receive notification: %v", err)
					}

					c.logger.Infof("[Blockchain] retrying subscription in 1 second")
					time.Sleep(1 * time.Second)
					break
				}

				hash, err := chainhash.NewHash(resp.Hash)
				if err != nil {
					c.logger.Errorf("[Blockchain] failed to parse hash: %v", err)
					continue
				}

				notification := &blockchain_api.Notification{
					Type:     resp.Type,
					Hash:     hash[:],
					Base_URL: resp.Base_URL,
					Metadata: resp.Metadata,
				}

				// Use a timeout for sending to prevent blocking
				select {
				case ch <- notification:
					// Successfully sent
				case <-time.After(5 * time.Second):
					c.logger.Warnf("[Blockchain] timeout sending notification for %s, channel may be blocked", source)
				case <-ctx.Done():
					return
				}
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

// GetBlockIsMined checks whether a specific block has been marked as mined.
// This method queries the blockchain service to determine if a block has been
// processed through the mining pipeline and marked as successfully mined.
//
// In the Teranode architecture, blocks go through several processing stages:
// 1. Initial validation and storage
// 2. Subtree processing and organization
// 3. Mining confirmation and marking
//
// This method specifically checks stage 3 - whether the block has been marked
// as mined, indicating that it has been fully processed and confirmed by the
// mining subsystem. This status is important for:
// - Block processing pipeline monitoring
// - Mining operation coordination
// - Ensuring blocks are ready for network propagation
// - Recovery operations and state consistency checks
// - Performance analysis and bottleneck identification
// - Ensuring all blocks complete the full processing workflow
//
// The method communicates with the blockchain service via gRPC to retrieve
// the mining status from the underlying blockchain store.
//
// Parameters:
//   - ctx: Context for the operation with timeout and cancellation support
//   - blockHash: Hash of the block to check for mining status
//
// Returns:
//   - bool: True if the block has been marked as mined, false otherwise
//   - error: Any error encountered during the status check operation
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
// This method updates the blockchain store to indicate that a specific block
// has been successfully processed through the mining pipeline and is ready
// for network propagation and inclusion in the active blockchain.
//
// In the Teranode architecture, this method represents the final step in the
// block processing workflow:
// 1. Block validation and initial storage
// 2. Subtree processing and organization
// 3. Mining confirmation and validation
// 4. Marking as mined (this method)
//
// Setting the mined status is critical for:
// - Coordinating block propagation to network peers
// - Ensuring blocks are ready for inclusion in subsequent mining operations
// - Tracking mining pipeline completion for monitoring and metrics
// - Maintaining consistency between mining and blockchain services
//
// The method communicates with the blockchain service via gRPC to update
// the mining status in the underlying blockchain store.
//
// Parameters:
//   - ctx: Context for the operation with timeout and cancellation support
//   - blockHash: Hash of the block to mark as mined
//
// Returns:
//   - error: Any error encountered during the mining status update operation
func (c *Client) SetBlockMinedSet(ctx context.Context, blockHash *chainhash.Hash) error {
	_, err := c.client.SetBlockMinedSet(ctx, &blockchain_api.SetBlockMinedSetRequest{
		BlockHash: blockHash[:],
	})
	if err != nil {
		return errors.UnwrapGRPC(err)
	}

	return nil
}

func (c *Client) ClearBlockMinedSet(ctx context.Context, blockHash *chainhash.Hash) error {
	_, err := c.client.ClearBlockMinedSet(ctx, &blockchain_api.ClearBlockMinedSetRequest{
		BlockHash: blockHash[:],
	})
	if err != nil {
		return errors.UnwrapGRPC(err)
	}

	return nil
}

// GetBlocksMinedNotSet retrieves blocks that haven't been marked as mined.
// This method identifies and returns blocks that have been processed and stored
// but have not yet completed the mining pipeline and been marked as mined.
//
// In the Teranode architecture, blocks progress through multiple stages:
// 1. Initial validation and storage
// 2. Subtree processing and organization
// 3. Mining confirmation and marking
//
// This method returns blocks that are in stages 1 or 2 but not yet in stage 3,
// indicating that their mining processing is incomplete. This is essential for:
// - Monitoring block processing pipeline health and progress
// - Identifying blocks that need mining completion
// - Recovery operations after service restarts or failures
// - Performance analysis and bottleneck identification
// - Ensuring all blocks complete the full processing workflow
//
// The returned blocks are fully deserialized and can be used by mining services
// or other components to complete the processing pipeline. This method is
// particularly useful for operational monitoring and automated recovery systems.
//
// Parameters:
//   - ctx: Context for the operation with timeout and cancellation support
//
// Returns:
//   - []*model.Block: Array of blocks that haven't been marked as mined
//   - error: Any error encountered during block retrieval or deserialization
func (c *Client) GetBlocksMinedNotSet(ctx context.Context) ([]*model.Block, error) {
	resp, err := c.client.GetBlocksMinedNotSet(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, errors.UnwrapGRPC(err)
	}

	blocks := make([]*model.Block, 0, len(resp.BlockBytes))

	for _, blockBytes := range resp.BlockBytes {
		block, err := model.NewBlockFromBytes(blockBytes)
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
// This method identifies and returns blocks that have been processed and stored
// but whose corresponding subtree data has not yet been marked as complete in
// the blockchain store.
//
// In the Teranode architecture, blocks undergo multi-stage processing:
// 1. Initial block validation and storage
// 2. Subtree generation and processing
// 3. Subtree storage and organization
// 4. Marking subtrees as "set" to indicate completion
//
// This method returns blocks that are in stages 1-3 but not yet in stage 4,
// indicating that their subtree processing is incomplete. This is crucial for:
// - Monitoring subtree processing pipeline health and progress
// - Identifying blocks that need subtree completion
// - Recovery operations after service restarts or failures
// - Performance analysis of subtree processing bottlenecks
// - Ensuring all blocks complete the full subtree workflow
//
// The returned blocks are fully deserialized and can be used by subtree
// processing services or other components to complete the processing pipeline.
// This method is essential for maintaining data consistency and operational
// visibility in the Teranode system.
//
// Parameters:
//   - ctx: Context for the operation with timeout and cancellation support
//
// Returns:
//   - []*model.Block: Array of blocks whose subtrees haven't been set
//   - error: Any error encountered during block retrieval or deserialization
func (c *Client) GetBlocksSubtreesNotSet(ctx context.Context) ([]*model.Block, error) {
	resp, err := c.client.GetBlocksSubtreesNotSet(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, errors.UnwrapGRPC(err)
	}

	blocks := make([]*model.Block, 0, len(resp.BlockBytes))

	for _, blockBytes := range resp.BlockBytes {
		block, err := model.NewBlockFromBytes(blockBytes)
		if err != nil {
			return nil, err
		}

		blocks = append(blocks, block)
	}

	return blocks, nil
}

// FSM related endpoints

// GetFSMCurrentState retrieves the current state of the finite state machine.
// This method provides access to the blockchain service's operational state,
// which is managed by a finite state machine (FSM) that coordinates the
// service's lifecycle and processing modes.
//
// The FSM manages critical blockchain service states such as:
// - IDLE: Service is idle and ready for operations
// - RUNNING: Service is actively processing blocks
// - SYNCING: Service is synchronizing with the network
// - STOPPING: Service is gracefully shutting down
// - ERROR: Service has encountered an error condition
//
// The current state information is essential for:
// - Coordinating operations between blockchain components
// - Monitoring service health and operational status
// - Implementing proper shutdown and startup sequences
// - Debugging service state transitions and issues
// - Ensuring proper service coordination in distributed systems
//
// The method first checks the locally cached state for performance, and if
// not available, queries the blockchain service directly via gRPC to retrieve
// the authoritative current state.
//
// Parameters:
//   - ctx: Context for the operation with timeout and cancellation support
//
// Returns:
//   - *FSMStateType: Current state of the blockchain service FSM
//   - error: Any error encountered during state retrieval
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

// WaitUntilFSMTransitionFromIdleState waits for the FSM to transition from the IDLE state.
// This method blocks until the blockchain service's finite state machine transitions
// away from the IDLE state to any other operational state, providing synchronization
// for coordinated service startup and operational workflows.
//
// The method is essential for:
//   - Coordinating service startup sequences where components must wait for the
//     blockchain service to become active before proceeding
//   - Implementing proper initialization order in distributed systems
//   - Ensuring dependent services don't start processing before blockchain is ready
//   - Synchronizing mining operations with blockchain service availability
//   - Managing graceful service recovery after maintenance or restarts
//
// The method continuously polls the FSM state until it detects a transition away
// from IDLE, then logs the successful transition and returns. This provides a
// reliable synchronization mechanism for service coordination.
//
// Common use cases include:
// - Mining services waiting for blockchain to be ready
// - Transaction processors waiting for blockchain availability
// - Monitoring systems coordinating with blockchain lifecycle
// - Integration tests requiring blockchain service readiness
//
// Parameters:
//   - ctx: Context for the operation with timeout and cancellation support
//
// Returns:
//   - error: Any error encountered during the wait operation or context cancellation
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

// IsFullyReady checks if the blockchain service is fully operational.
// This method verifies that the blockchain service is ready for normal operations,
// which includes both the FSM being in a non-IDLE state and the subscription
// infrastructure being fully initialized.
func (c *Client) IsFullyReady(ctx context.Context) (bool, error) {
	// Get the current FSM state using our existing method
	// This already considers subscription readiness on the server side
	currentState, err := c.GetFSMCurrentState(ctx)
	if err != nil {
		return false, err
	}

	// Service is ready if FSM is not in IDLE state
	// (The server-side GetFSMCurrentState already ensures subscription readiness)
	isReady := currentState != nil && *currentState != FSMStateIDLE

	c.logger.Debugf("[Blockchain Client] IsFullyReady check - State: %v, Ready: %v", currentState, isReady)
	return isReady, nil
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
// This method initiates a blockchain synchronization process by transitioning the
// blockchain service's finite state machine to the CATCHING_BLOCKS state, which
// triggers the service to synchronize with the network and catch up on any
// missing blocks.
//
// The method is essential for:
// - Recovering from network disconnections or service downtime
// - Ensuring the local blockchain is synchronized with the network
// - Handling blockchain reorganizations and chain updates
// - Maintaining consensus with the Bitcoin SV network
// - Coordinating synchronization across distributed Teranode components
//
// The method first checks if the FSM is already in the CATCHING_BLOCKS state
// to avoid unnecessary state transitions. If not already catching up, it sends
// the appropriate FSM event to trigger the synchronization process.
//
// This operation is typically used during:
// - Service startup when the local chain may be behind
// - Recovery from network partitions or connectivity issues
// - Manual synchronization requests from operators
// - Automated catch-up processes in distributed deployments
//
// The method communicates with the blockchain service via gRPC to send the
// FSM event that initiates the block catching process.
//
// Parameters:
//   - ctx: Context for the operation with timeout and cancellation support
//
// Returns:
//   - error: Any error encountered during the FSM event transmission
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

// ReportPeerFailure reports a peer download failure to the blockchain service.
func (c *Client) ReportPeerFailure(ctx context.Context, hash *chainhash.Hash, peerID string, failureType string, reason string) error {
	req := &blockchain_api.ReportPeerFailureRequest{
		Hash:        hash[:],
		PeerId:      peerID,
		FailureType: failureType,
		Reason:      reason,
	}

	_, err := c.client.ReportPeerFailure(ctx, req)
	if err != nil {
		return errors.UnwrapGRPC(err)
	}

	return nil
}

// LegacySync sends a legacy sync FSM event to the blockchain service.
// This method initiates a legacy synchronization process by transitioning the
// blockchain service's finite state machine to the LEGACY_SYNCING state, which
// triggers compatibility mode synchronization with older Bitcoin SV network
// protocols and legacy blockchain implementations.
//
// The legacy sync mode is essential for:
// - Maintaining compatibility with older Bitcoin SV network nodes
// - Synchronizing with legacy blockchain implementations
// - Supporting migration scenarios from older Teranode versions
// - Ensuring interoperability across diverse network topologies
// - Handling edge cases in blockchain synchronization protocols
//
// The method first checks if the FSM is already in the LEGACY_SYNCING state
// to avoid unnecessary state transitions. If not already in legacy sync mode,
// it sends the appropriate FSM event to trigger the legacy synchronization
// process.
//
// Legacy synchronization differs from standard sync by:
// - Using older protocol versions for network communication
// - Implementing backward-compatible block validation rules
// - Supporting legacy transaction formats and structures
// - Maintaining compatibility with pre-upgrade network nodes
//
// This operation is typically used during:
// - Network upgrades and migration periods
// - Integration with legacy Bitcoin SV infrastructure
// - Debugging synchronization issues with older nodes
// - Ensuring network-wide compatibility during protocol transitions
//
// The method communicates with the blockchain service via gRPC to send the
// FSM event that initiates the legacy synchronization process.
//
// Parameters:
//   - ctx: Context for the operation with timeout and cancellation support
//
// Returns:
//   - error: Any error encountered during the FSM event transmission
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

// Idle transitions the blockchain service to the idle state via FSM event.
// This method sends an IDLE event to the blockchain service's finite state machine,
// causing it to transition to the IDLE state where it stops active processing
// and waits for further commands.
//
// The method first checks if the FSM is already in the IDLE state to avoid
// unnecessary transitions. If the current state is already IDLE, it returns
// immediately without sending the event.
//
// The IDLE state is used for:
// - Graceful shutdown preparation
// - Pausing blockchain processing temporarily
// - Testing scenarios where controlled state transitions are needed
// - Maintenance operations that require the service to be quiescent
//
// This operation is part of the FSM control interface that allows external
// services to coordinate blockchain service state transitions for operational
// and testing purposes.
//
// Parameters:
//   - ctx: Context for the operation with timeout and cancellation support
//
// Returns:
//   - error: Any error encountered during the FSM event transmission
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
// This method generates a Bitcoin block locator, which is a compact representation
// of the blockchain structure used for efficient synchronization between nodes.
// The locator contains strategically selected block hashes that allow peers to
// quickly identify common ancestors and synchronization points.
//
// A block locator is constructed using an exponential backoff algorithm:
// - Recent blocks are included with high density (every block)
// - Older blocks are included with decreasing density (exponential spacing)
// - This creates an efficient logarithmic structure for chain comparison
//
// The block locator is essential for:
// - Efficient blockchain synchronization between network peers
// - Identifying common ancestors during chain reorganizations
// - Minimizing network traffic during sync operations
// - Supporting the Bitcoin protocol's getblocks and getheaders messages
// - Enabling fast chain comparison without transmitting full block data
//
// The method starts from the specified block header hash and height, then
// works backward through the blockchain to construct the locator array.
// The resulting locator can be used by other nodes to efficiently determine
// which blocks they need to synchronize.
//
// This implementation follows the standard Bitcoin protocol for block locators,
// ensuring compatibility with the broader Bitcoin SV network.
//
// Parameters:
//   - ctx: Context for the operation with timeout and cancellation support
//   - blockHeaderHash: Hash of the block header to start the locator from
//   - blockHeaderHeight: Height of the starting block header
//
// Returns:
//   - []*chainhash.Hash: Array of block hashes forming the block locator
//   - error: Any error encountered during locator generation
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
// This method implements the Bitcoin protocol's getheaders functionality,
// using a block locator to efficiently find and return a sequence of block
// headers starting from a common ancestor. This is a fundamental operation
// for blockchain synchronization between network peers.
//
// The method works by:
// 1. Taking a block locator (array of strategically selected block hashes)
// 2. Finding the most recent common block hash in the locator
// 3. Returning subsequent block headers up to the specified limits
//
// The locator parameter should be generated using GetBlockLocator() and
// represents the requesting node's view of the blockchain. The method
// finds the latest block hash in the locator that exists in the local
// blockchain, then returns headers starting from the next block.
//
// This operation is essential for:
// - Blockchain synchronization between network peers
// - Implementing the Bitcoin protocol's getheaders message
// - Efficient chain comparison and synchronization
// - Supporting SPV (Simplified Payment Verification) clients
// - Enabling lightweight blockchain synchronization
//
// The method respects both the maxHashes limit and the hashStop parameter:
// - maxHashes limits the total number of headers returned
// - hashStop provides an optional stopping point for the header sequence
// - This prevents excessive network traffic and resource usage
//
// The returned headers are fully deserialized and ready for validation
// and processing by the requesting node.
//
// Parameters:
//   - ctx: Context for the operation with timeout and cancellation support
//   - locator: Block locator array identifying the requesting node's chain state
//   - hashStop: Optional hash where header retrieval should stop (can be nil)
//   - maxHashes: Maximum number of block headers to return
//
// Returns:
//   - []*model.BlockHeader: Array of block headers starting from the common ancestor
//   - error: Any error encountered during header location and retrieval
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
// This function provides an efficient bit manipulation algorithm to compute the
// floor of the base-2 logarithm of a 32-bit unsigned integer without using
// floating-point operations or expensive mathematical functions.
//
// The algorithm uses a series of bit masks defined in log2FloorMasks to perform
// a binary search-like operation that determines the position of the most
// significant bit in constant time. This is particularly useful for:
// - Block locator generation in blockchain synchronization
// - Efficient power-of-2 calculations
// - Performance-critical bit manipulation operations
// - Reducing memory overhead when only block references are needed
//
// The method works by testing increasingly smaller bit ranges using predefined
// masks, accumulating the position of the highest set bit. This approach
// provides O(1) time complexity regardless of the input value.
//
// Parameters:
//   - n: The 32-bit unsigned integer to compute the floor log2 for
//
// Returns:
//   - uint8: The floor of log2(n), or 0 if n is 0
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

// SetBlockProcessedAt sets or clears the processed_at timestamp for a block.
// This method manages the processing timestamp metadata for blocks in the blockchain,
// which is used to track when blocks have been fully processed by the system.
//
// The processed_at timestamp serves several important purposes:
// - Tracking block processing completion for monitoring and metrics
// - Identifying blocks that may need reprocessing after system restarts
// - Supporting recovery operations and state consistency checks
// - Enabling performance analysis of block processing pipelines
//
// The method supports two modes of operation:
// 1. Setting timestamp: Records the current time as the processing completion time
// 2. Clearing timestamp: Removes the timestamp to mark the block as unprocessed
//
// This functionality is particularly important in the Teranode architecture where
// blocks undergo multiple processing stages and the system needs to track which
// blocks have completed all processing steps.
//
// Parameters:
//   - ctx: Context for the operation with timeout and cancellation support
//   - blockHash: Hash of the block to update the processing timestamp for
//   - clear: Optional boolean parameter; if true, clears the timestamp instead of setting it
//
// Returns:
//   - error: Any error encountered during the timestamp update operation
func (c *Client) SetBlockProcessedAt(ctx context.Context, blockHash *chainhash.Hash, clear ...bool) error {
	c.logger.Debugf("[Blockchain Client] Setting block processed at timestamp for %s, clear=%v", blockHash, clear)

	req := &blockchain_api.SetBlockProcessedAtRequest{
		BlockHash: blockHash[:],
	}

	if len(clear) > 0 {
		req.Clear = clear[0]
	}

	_, err := c.client.SetBlockProcessedAt(ctx, req)
	if err != nil {
		return errors.UnwrapGRPC(err)
	}

	return nil
}

// SetBlockPersistedAt updates the persisted_at timestamp for a block.
// This marks the block as having been persisted to blob storage.
func (c *Client) SetBlockPersistedAt(ctx context.Context, blockHash *chainhash.Hash) error {
	_, err := c.client.SetBlockPersistedAt(ctx, &blockchain_api.SetBlockPersistedAtRequest{
		BlockHash: blockHash.CloneBytes(),
	})

	return err
}

// GetBlocksNotPersisted retrieves blocks that haven't been persisted to blob storage yet.
func (c *Client) GetBlocksNotPersisted(ctx context.Context, limit int) ([]*model.Block, error) {
	resp, err := c.client.GetBlocksNotPersisted(ctx, &blockchain_api.GetBlocksNotPersistedRequest{
		Limit: int32(limit),
	})
	if err != nil {
		return nil, err
	}

	blocks := make([]*model.Block, len(resp.BlockBytes))

	for i, blockBytes := range resp.BlockBytes {
		block, err := model.NewBlockFromBytes(blockBytes)
		if err != nil {
			return nil, errors.NewProcessingError("failed to deserialize block", err)
		}
		blocks[i] = block
	}

	return blocks, nil
}
