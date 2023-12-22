package txmeta

import (
	"context"

	"github.com/bitcoin-sv/ubsv/services/txmeta/txmeta_api"
	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Client struct {
	client txmeta_api.TxMetaAPIClient
	logger ulogger.Logger
}

func NewClient(ctx context.Context, logger ulogger.Logger) (*Client, error) {
	txmeta_grpcAddress, _ := gocore.Config().Get("txmeta_grpcAddress")
	conn, err := util.GetGRPCClient(ctx, txmeta_grpcAddress, &util.ConnectionOptions{
		OpenTracing: gocore.Config().GetBool("use_open_tracing", true),
		Prometheus:  gocore.Config().GetBool("use_prometheus_grpc_metrics", true),
		MaxRetries:  3,
	})
	if err != nil {
		return nil, err
	}

	client := txmeta_api.NewTxMetaAPIClient(conn)

	return &Client{
		client: client,
		logger: logger,
	}, nil
}

func (c *Client) Health(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*txmeta_api.HealthResponse, error) {
	resp, err := c.client.HealthGRPC(ctx, in, opts...)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (c *Client) GetMeta(ctx context.Context, hash *chainhash.Hash) (*txmeta.Data, error) {
	return c.Get(ctx, hash)
}

func (c *Client) Get(ctx context.Context, hash *chainhash.Hash) (*txmeta.Data, error) {
	resp, err := c.client.Get(ctx, &txmeta_api.GetRequest{
		Hash: hash[:],
	})
	if err != nil {
		return nil, err
	}

	var parentTxHashes []*chainhash.Hash
	parentTxHashes, err = getChainHashesFromBytes(resp.ParentTxHashes)
	if err != nil {
		return nil, err
	}

	return &txmeta.Data{
		Fee:            resp.Fee,
		ParentTxHashes: parentTxHashes,
		BlockIDs:       resp.BlockIDs,
	}, nil
}

func (c *Client) Create(ctx context.Context, tx *bt.Tx) (*txmeta.Data, error) {

	txMeta, err := util.TxMetaDataFromTx(tx)
	if err != nil {
		return nil, err
	}
	var parentTxHashesBytes [][]byte
	for _, parentTxHash := range txMeta.ParentTxHashes {
		parentTxHashesBytes = append(parentTxHashesBytes, parentTxHash[:])
	}

	_, err = c.client.Create(ctx, &txmeta_api.CreateRequest{
		Tx:             tx.ExtendedBytes(),
		Fee:            txMeta.Fee,
		SizeInBytes:    txMeta.SizeInBytes,
		ParentTxHashes: parentTxHashesBytes,
	})
	if err != nil {
		return txMeta, err
	}

	return txMeta, nil
}

func (c *Client) SetMined(ctx context.Context, hash *chainhash.Hash, blockID uint32) error {
	_, err := c.client.SetMined(ctx, &txmeta_api.SetMinedRequest{
		Hash:    hash[:],
		BlockId: blockID,
	})
	if err != nil {
		return err
	}

	return nil
}

func (c *Client) SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash, blockID uint32) error {
	for _, hash := range hashes {
		if err := c.SetMined(ctx, hash, blockID); err != nil {
			return err
		}
	}

	return nil
}

func (c *Client) Delete(_ context.Context, _ *chainhash.Hash) error {
	return nil // do not allow to Delete through grpc
}

func getChainHashesFromBytes(hashes [][]byte) (chainHashes []*chainhash.Hash, err error) {
	if len(hashes) > 0 {
		chainHashes = make([]*chainhash.Hash, len(hashes))
		var hash *chainhash.Hash
		for index, hashBytes := range hashes {
			hash, err = chainhash.NewHash(hashBytes)
			if err != nil {
				return nil, err
			}
			chainHashes[index] = hash
		}
	}

	return
}
