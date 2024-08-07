package blockchain

import (
	"context"
	"strings"
	"sync/atomic"
	"time"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/blockchain/blockchain_api"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Client struct {
	client  blockchain_api.BlockchainAPIClient
	logger  ulogger.Logger
	running *atomic.Bool
	conn    *grpc.ClientConn
	// currFSMstate blockchain_api.FSMStateType
}

// client subscribes to server notifications and updates currFSMState.
//

type BestBlockHeader struct {
	Header *model.BlockHeader
	Height uint32
}

func NewClient(ctx context.Context, logger ulogger.Logger) (ClientI, error) {
	logger = logger.New("blkcC")

	blockchainGrpcAddress, ok := gocore.Config().Get("blockchain_grpcAddress")
	if !ok {
		return nil, errors.NewConfigurationError("no blockchain_grpcAddress setting found")
	}

	var err error
	var baConn *grpc.ClientConn
	var baClient blockchain_api.BlockchainAPIClient

	// retry a few times to connect to the blockchain service
	maxRetries, _ := gocore.Config().GetInt("blockchain_maxRetries", 3)
	retrySleep, _ := gocore.Config().GetInt("blockchain_retrySleep", 1000)

	retries := 0
	for {
		baConn, err = util.GetGRPCClient(ctx, blockchainGrpcAddress, &util.ConnectionOptions{
			MaxRetries: 3,
		})
		if err != nil {
			return nil, errors.NewServiceError("failed to init blockchain service connection", err)
		}

		baClient = blockchain_api.NewBlockchainAPIClient(baConn)

		_, err = baClient.HealthGRPC(ctx, &emptypb.Empty{})
		if err != nil {
			if retries < maxRetries {
				retries++
				backoff := time.Duration(retries*retrySleep) * time.Millisecond
				logger.Debugf("[Blockchain] failed to connect to blockchain service, retrying %d in %s: %v", retries, backoff, err)
				time.Sleep(backoff)
				continue
			}

			logger.Errorf("[Blockchain] failed to connect to blockchain service, retried %d times: %v", maxRetries, err)
			return nil, err
		}
		break
	}

	running := atomic.Bool{}
	running.Store(true)

	return &Client{
		client:  baClient,
		logger:  logger,
		running: &running,
		conn:    baConn,
	}, nil
}

func NewClientWithAddress(ctx context.Context, logger ulogger.Logger, address string) (ClientI, error) {
	baConn, err := util.GetGRPCClient(ctx, address, &util.ConnectionOptions{
		MaxRetries: 3,
	})
	if err != nil {
		return nil, err
	}

	running := atomic.Bool{}
	running.Store(true)

	return &Client{
		client:  blockchain_api.NewBlockchainAPIClient(baConn),
		logger:  logger,
		running: &running,
	}, nil
}

func (c *Client) Health(ctx context.Context) (*blockchain_api.HealthResponse, error) {
	return c.client.HealthGRPC(ctx, &emptypb.Empty{})
}

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
		return err
	}

	return nil
}

func (c *Client) GetBlock(ctx context.Context, blockHash *chainhash.Hash) (*model.Block, error) {
	resp, err := c.client.GetBlock(ctx, &blockchain_api.GetBlockRequest{
		Hash: blockHash[:],
	})
	if err != nil {
		return nil, errors.UnwrapGRPC(err)
	}

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

	return model.NewBlock(header, coinbaseTx, subtreeHashes, resp.TransactionCount, resp.SizeInBytes, resp.Height)
}

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

func (c *Client) GetBlockByHeight(ctx context.Context, height uint32) (*model.Block, error) {
	resp, err := c.client.GetBlockByHeight(ctx, &blockchain_api.GetBlockByHeightRequest{
		Height: height,
	})
	if err != nil {
		return nil, errors.UnwrapGRPC(err)
	}

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

	return model.NewBlock(header, coinbaseTx, subtreeHashes, resp.TransactionCount, resp.SizeInBytes, resp.Height)
}

func (c *Client) GetBlockStats(ctx context.Context) (*model.BlockStats, error) {
	resp, err := c.client.GetBlockStats(ctx, &emptypb.Empty{})
	return resp, errors.UnwrapGRPC(err)
}

func (c *Client) GetBlockGraphData(ctx context.Context, periodMillis uint64) (*model.BlockDataPoints, error) {
	resp, err := c.client.GetBlockGraphData(ctx, &blockchain_api.GetBlockGraphDataRequest{
		PeriodMillis: periodMillis,
	})

	return resp, errors.UnwrapGRPC(err)
}

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
func (c *Client) GetSuitableBlock(ctx context.Context, blockHash *chainhash.Hash) (*model.SuitableBlock, error) {
	resp, err := c.client.GetSuitableBlock(ctx, &blockchain_api.GetSuitableBlockRequest{
		Hash: blockHash[:],
	})
	if err != nil {
		return nil, errors.UnwrapGRPC(err)
	}

	return resp.Block, nil
}

func (c *Client) GetHashOfAncestorBlock(ctx context.Context, blockHash *chainhash.Hash, depth int) (*chainhash.Hash, error) {
	resp, err := c.client.GetHashOfAncestorBlock(ctx, &blockchain_api.GetHashOfAncestorBlockRequest{
		Hash:  blockHash[:],
		Depth: uint32(depth),
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

func (c *Client) GetNextWorkRequired(ctx context.Context, blockHash *chainhash.Hash) (*model.NBit, error) {
	resp, err := c.client.GetNextWorkRequired(ctx, &blockchain_api.GetNextWorkRequiredRequest{
		BlockHash: blockHash[:],
	})
	if err != nil {
		return nil, err
	}
	bits := model.NewNBitFromSlice(resp.Bits)

	return &bits, nil
}

func (c *Client) GetBlockExists(ctx context.Context, blockHash *chainhash.Hash) (bool, error) {
	resp, err := c.client.GetBlockExists(ctx, &blockchain_api.GetBlockRequest{
		Hash: blockHash[:],
	})
	if err != nil {
		return false, errors.UnwrapGRPC(err)
	}

	return resp.Exists, nil
}

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
	}

	return header, meta, nil
}

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

func (c *Client) GetBlockHeaders(ctx context.Context, blockHash *chainhash.Hash, numberOfHeaders uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	resp, err := c.client.GetBlockHeaders(ctx, &blockchain_api.GetBlockHeadersRequest{
		StartHash:       blockHash.CloneBytes(),
		NumberOfHeaders: numberOfHeaders,
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
		header, err := model.NewBlockHeaderMetaFromBytes(metaBytes)
		if err != nil {
			return nil, nil, err
		}
		metas = append(metas, header)
	}

	return headers, metas, nil
}

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

func (c *Client) InvalidateBlock(ctx context.Context, blockHash *chainhash.Hash) error {
	_, err := c.client.InvalidateBlock(ctx, &blockchain_api.InvalidateBlockRequest{
		BlockHash: blockHash.CloneBytes(),
	})

	return errors.UnwrapGRPC(err)
}

func (c *Client) RevalidateBlock(ctx context.Context, blockHash *chainhash.Hash) error {
	_, err := c.client.RevalidateBlock(ctx, &blockchain_api.RevalidateBlockRequest{
		BlockHash: blockHash.CloneBytes(),
	})

	return errors.UnwrapGRPC(err)
}

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

func (c *Client) SendFSMEvent(ctx context.Context, event blockchain_api.FSMEventType) error {
	// All services started successfully
	// create a new blockchain notification with Run event
	notification := &model.Notification{
		Type:    model.NotificationType_FSMEvent,
		Hash:    &chainhash.Hash{}, // not relevant for FSMEvent notifications
		BaseURL: "",                // not relevant for FSMEvent notifications
		Metadata: model.NotificationMetadata{
			Metadata: map[string]string{
				"event": event.String(),
			},
		},
	}
	// send FSMEvent notification to the blockchain client. FSM will transition to state Running
	if err := c.SendNotification(ctx, notification); err != nil {
		//logger.Errorf("[Main] failed to send RUN notification [%v]", err)
		//panic(err)
		return err
	}

	return nil
}

func (c *Client) SendNotification(ctx context.Context, notification *model.Notification) error {
	blockchainNotification := &blockchain_api.Notification{
		Type:    notification.Type,
		Hash:    notification.Hash[:],
		BaseUrl: notification.BaseURL,
		Metadata: &blockchain_api.NotificationMetadata{
			Metadata: notification.Metadata.Metadata,
		},
	}
	if notification.Type == model.NotificationType_FSMEvent {
		c.logger.Infof("[Blockchain Client] Sending FSMevent notification, metadata: %v", blockchainNotification.Metadata.Metadata)
	}
	_, err := c.client.SendNotification(ctx, blockchainNotification)

	if err != nil {
		return err
	}

	return nil
}

func (c *Client) Subscribe(ctx context.Context, source string) (chan *model.Notification, error) {
	ch := make(chan *model.Notification)

	go func() {
		<-ctx.Done()
		c.logger.Infof("[Blockchain] context done, closing subscription: %s", source)
		c.running.Store(false)
		err := c.conn.Close()
		if err != nil {
			c.logger.Errorf("[Blockchain] failed to close connection", err)
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

				utils.SafeSend(ch, &model.Notification{
					Type: resp.Type,
					Hash: hash,
				})
			}
		}
	}()

	return ch, nil
}

func (c *Client) GetState(ctx context.Context, key string) ([]byte, error) {
	resp, err := c.client.GetState(ctx, &blockchain_api.GetStateRequest{
		Key: key,
	})
	if err != nil {
		return nil, errors.UnwrapGRPC(err)
	}

	return resp.Data, nil
}

func (c *Client) SetState(ctx context.Context, key string, data []byte) error {
	_, err := c.client.SetState(ctx, &blockchain_api.SetStateRequest{
		Key:  key,
		Data: data,
	})
	if err != nil {
		return err
	}

	return nil
}

func (c *Client) SetBlockMinedSet(ctx context.Context, blockHash *chainhash.Hash) error {
	_, err := c.client.SetBlockMinedSet(ctx, &blockchain_api.SetBlockMinedSetRequest{
		BlockHash: blockHash[:],
	})
	if err != nil {
		return err
	}

	return nil
}

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

func (c *Client) SetBlockSubtreesSet(ctx context.Context, blockHash *chainhash.Hash) error {
	_, err := c.client.SetBlockSubtreesSet(ctx, &blockchain_api.SetBlockSubtreesSetRequest{
		BlockHash: blockHash[:],
	})
	if err != nil {
		return err
	}

	return nil
}

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

func (c *Client) GetFSMCurrentState(ctx context.Context) (*blockchain_api.FSMStateType, error) {

	state, err := c.client.GetFSMCurrentState(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, err
	}

	return &state.State, nil
}

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
