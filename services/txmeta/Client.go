package txmeta

import (
	"context"

	"github.com/bitcoin-sv/ubsv/services/txmeta/txmeta_api"
	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Client struct {
	client txmeta_api.TxMetaAPIClient
	logger utils.Logger
}

func NewClient(ctx context.Context, logger utils.Logger) (*Client, error) {
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
	resp, err := c.client.Health(ctx, in, opts...)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (c *Client) Get(ctx context.Context, hash *chainhash.Hash) (*txmeta.Data, error) {
	resp, err := c.client.Get(ctx, &txmeta_api.GetRequest{
		Hash: hash[:],
	})
	if err != nil {
		return nil, err
	}

	var utxoHashes []*chainhash.Hash
	utxoHashes, err = getChainHashesFromBytes(resp.UtxoHashes)
	if err != nil {
		return nil, err
	}

	var parentTxHashes []*chainhash.Hash
	parentTxHashes, err = getChainHashesFromBytes(resp.ParentTxHashes)
	if err != nil {
		return nil, err
	}

	var blockHashes []*chainhash.Hash
	blockHashes, err = getChainHashesFromBytes(resp.BlockHashes)
	if err != nil {
		return nil, err
	}

	return &txmeta.Data{
		Status:         txmeta.TxStatus(resp.Status),
		Fee:            resp.Fee,
		UtxoHashes:     utxoHashes,
		ParentTxHashes: parentTxHashes,
		FirstSeen:      resp.FirstSeen,
		BlockHashes:    blockHashes,
		BlockHeight:    resp.BlockHeight,
	}, nil
}

func (c *Client) Create(ctx context.Context, tx *bt.Tx, hash *chainhash.Hash, fee uint64, sizeInBytes uint64, parentTxHashes []*chainhash.Hash,
	utxoHashes []*chainhash.Hash, nLockTime uint32) error {

	var parentTxHashesBytes [][]byte
	for _, parentTxHash := range parentTxHashes {
		parentTxHashesBytes = append(parentTxHashesBytes, parentTxHash[:])
	}

	var utxoHashesBytes [][]byte
	for _, utxoHash := range utxoHashes {
		utxoHashesBytes = append(utxoHashesBytes, utxoHash[:])
	}

	_, err := c.client.Create(ctx, &txmeta_api.CreateRequest{
		Tx:             tx.Bytes(),
		Hash:           hash[:],
		Fee:            fee,
		SizeInBytes:    sizeInBytes,
		ParentTxHashes: parentTxHashesBytes,
		UtxoHashes:     utxoHashesBytes,
		LockTime:       nLockTime,
	})
	if err != nil {
		return err
	}

	return nil
}

func (c *Client) SetMined(ctx context.Context, hash *chainhash.Hash, blockHash *chainhash.Hash) error {
	_, err := c.client.SetMined(ctx, &txmeta_api.SetMinedRequest{
		Hash:      hash[:],
		BlockHash: blockHash[:],
	})
	if err != nil {
		return err
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
		for index, utxoHashBytes := range hashes {
			hash, err = chainhash.NewHash(utxoHashBytes)
			if err != nil {
				return nil, err
			}
			chainHashes[index] = hash
		}
	}

	return
}
