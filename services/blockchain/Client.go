package blockchain

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/blockchain/blockchain_api"
	"github.com/bitcoin-sv/ubsv/ubsverrors"
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
}

type BestBlockHeader struct {
	Header *model.BlockHeader
	Height uint32
}

func NewClient(ctx context.Context, logger ulogger.Logger) (ClientI, error) {
	logger = logger.New("blkcC")

	blockchainGrpcAddress, ok := gocore.Config().Get("blockchain_grpcAddress")
	if !ok {
		return nil, fmt.Errorf("no blockchain_grpcAddress setting found")
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
			OpenTracing: gocore.Config().GetBool("use_open_tracing", true),
			Prometheus:  gocore.Config().GetBool("use_prometheus_grpc_metrics", true),
			MaxRetries:  3,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to init blockchain service connection: %v", err)
		}

		baClient = blockchain_api.NewBlockchainAPIClient(baConn)

		_, err = baClient.HealthGRPC(ctx, &emptypb.Empty{})
		if err != nil {
			if retries < maxRetries {
				retries++
				backoff := time.Duration(retries*retrySleep) * time.Millisecond
				logger.Warnf("[Blockchain] failed to connect to blockchain service, retrying %d in %s: %v", retries, backoff, err)
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
		OpenTracing: gocore.Config().GetBool("use_open_tracing", true),
		Prometheus:  gocore.Config().GetBool("use_prometheus_grpc_metrics", true),
		MaxRetries:  3,
	})
	if err != nil {
		return nil, err
	}

	return &Client{
		client: blockchain_api.NewBlockchainAPIClient(baConn),
		logger: logger,
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
		return nil, ubsverrors.UnwrapGRPC(err)
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

	return model.NewBlock(header, coinbaseTx, subtreeHashes, resp.TransactionCount, resp.SizeInBytes)
}

func (c *Client) GetBlockByHeight(ctx context.Context, height uint32) (*model.Block, error) {
	resp, err := c.client.GetBlockByHeight(ctx, &blockchain_api.GetBlockByHeightRequest{
		Height: height,
	})
	if err != nil {
		return nil, ubsverrors.UnwrapGRPC(err)
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

	return model.NewBlock(header, coinbaseTx, subtreeHashes, resp.TransactionCount, resp.SizeInBytes)
}

func (c *Client) GetBlockStats(ctx context.Context) (*model.BlockStats, error) {
	resp, err := c.client.GetBlockStats(ctx, &emptypb.Empty{})
	return resp, ubsverrors.UnwrapGRPC(err)
}

func (c *Client) GetBlockGraphData(ctx context.Context, periodMillis uint64) (*model.BlockDataPoints, error) {
	resp, err := c.client.GetBlockGraphData(ctx, &blockchain_api.GetBlockGraphDataRequest{
		PeriodMillis: periodMillis,
	})

	return resp, ubsverrors.UnwrapGRPC(err)
}

func (c *Client) GetLastNBlocks(ctx context.Context, n int64, includeOrphans bool, fromHeight uint32) ([]*model.BlockInfo, error) {
	resp, err := c.client.GetLastNBlocks(ctx, &blockchain_api.GetLastNBlocksRequest{
		NumberOfBlocks: n,
		IncludeOrphans: includeOrphans,
		FromHeight:     fromHeight,
	})
	if err != nil {
		return nil, ubsverrors.UnwrapGRPC(err)
	}

	return resp.Blocks, nil
}
func (c *Client) GetSuitableBlock(ctx context.Context, blockHash *chainhash.Hash) (*model.SuitableBlock, error) {
	resp, err := c.client.GetSuitableBlock(ctx, &blockchain_api.GetSuitableBlockRequest{
		Hash: blockHash[:],
	})
	if err != nil {
		return nil, ubsverrors.UnwrapGRPC(err)
	}

	return resp.Block, nil
}
func (c *Client) GetHashOfAncestorBlock(ctx context.Context, blockHash *chainhash.Hash, depth int) (*chainhash.Hash, error) {
	resp, err := c.client.GetHashOfAncestorBlock(ctx, &blockchain_api.GetHashOfAncestorBlockRequest{
		Hash:  blockHash[:],
		Depth: uint32(depth),
	})
	if err != nil {
		return nil, ubsverrors.UnwrapGRPC(err)
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
		return false, ubsverrors.UnwrapGRPC(err)
	}

	return resp.Exists, nil
}

func (c *Client) GetBestBlockHeader(ctx context.Context) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	resp, err := c.client.GetBestBlockHeader(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, nil, ubsverrors.UnwrapGRPC(err)
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
		return nil, nil, ubsverrors.UnwrapGRPC(err)
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

func (c *Client) GetBlockHeaders(ctx context.Context, blockHash *chainhash.Hash, numberOfHeaders uint64) ([]*model.BlockHeader, []uint32, error) {
	resp, err := c.client.GetBlockHeaders(ctx, &blockchain_api.GetBlockHeadersRequest{
		StartHash:       blockHash.CloneBytes(),
		NumberOfHeaders: numberOfHeaders,
	})
	if err != nil {
		return nil, nil, ubsverrors.UnwrapGRPC(err)
	}

	headers := make([]*model.BlockHeader, 0, len(resp.BlockHeaders))
	for _, headerBytes := range resp.BlockHeaders {
		header, err := model.NewBlockHeaderFromBytes(headerBytes)
		if err != nil {
			return nil, nil, err
		}
		headers = append(headers, header)
	}

	return headers, resp.Heights, nil
}

func (c *Client) GetBlockHeadersFromHeight(ctx context.Context, height, limit uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	resp, err := c.client.GetBlockHeadersFromHeight(ctx, &blockchain_api.GetBlockHeadersFromHeightRequest{
		StartHeight: height,
		Limit:       limit,
	})
	if err != nil {
		return nil, nil, ubsverrors.UnwrapGRPC(err)
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

	return ubsverrors.UnwrapGRPC(err)
}

func (c *Client) RevalidateBlock(ctx context.Context, blockHash *chainhash.Hash) error {
	_, err := c.client.RevalidateBlock(ctx, &blockchain_api.RevalidateBlockRequest{
		BlockHash: blockHash.CloneBytes(),
	})

	return ubsverrors.UnwrapGRPC(err)
}

func (c *Client) GetBlockHeaderIDs(ctx context.Context, blockHash *chainhash.Hash, numberOfHeaders uint64) ([]uint32, error) {
	resp, err := c.client.GetBlockHeaderIDs(ctx, &blockchain_api.GetBlockHeadersRequest{
		StartHash:       blockHash.CloneBytes(),
		NumberOfHeaders: numberOfHeaders,
	})
	if err != nil {
		return nil, ubsverrors.UnwrapGRPC(err)
	}

	return resp.Ids, nil
}

func (c *Client) SendNotification(ctx context.Context, notification *model.Notification) error {
	_, err := c.client.SendNotification(ctx, &blockchain_api.Notification{
		Type: notification.Type,
		Hash: notification.Hash[:],
	})

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
		return nil, ubsverrors.UnwrapGRPC(err)
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
		return nil, ubsverrors.UnwrapGRPC(err)
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
		return nil, ubsverrors.UnwrapGRPC(err)
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
