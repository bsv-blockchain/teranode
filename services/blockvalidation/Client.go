package blockvalidation

import (
	"context"
	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/model"
	"time"

	"github.com/bitcoin-sv/ubsv/services/blockvalidation/blockvalidation_api"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/stores/utxo/meta"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
)

type Client struct {
	apiClient   blockvalidation_api.BlockValidationAPIClient
	httpAddress string
	logger      ulogger.Logger
}

func NewClient(ctx context.Context, logger ulogger.Logger) *Client {
	blockValidationGrpcAddress, ok := gocore.Config().Get("blockvalidation_grpcAddress")
	if !ok {
		panic("no blockvalidation_grpcAddress setting found")
	}
	baConn, err := util.GetGRPCClient(ctx, blockValidationGrpcAddress, &util.ConnectionOptions{
		OpenTracing: gocore.Config().GetBool("use_open_tracing", true),
		Prometheus:  gocore.Config().GetBool("use_prometheus_grpc_metrics", true),
		MaxRetries:  3,
	})
	if err != nil {
		panic(err)
	}

	client := &Client{
		apiClient: blockvalidation_api.NewBlockValidationAPIClient(baConn),
		logger:    logger,
	}

	httpAddress, ok := gocore.Config().Get("blockvalidation_httpAddress")
	if ok {
		client.httpAddress = httpAddress
	}

	return client
}

func (s *Client) Health(ctx context.Context) (bool, error) {
	_, err := s.apiClient.HealthGRPC(ctx, &blockvalidation_api.EmptyMessage{})
	if err != nil {
		return false, err
	}

	return true, nil
}

func (s *Client) BlockFound(ctx context.Context, blockHash *chainhash.Hash, baseUrl string, waitToComplete bool) error {
	req := &blockvalidation_api.BlockFoundRequest{
		Hash:           blockHash.CloneBytes(),
		BaseUrl:        baseUrl,
		WaitToComplete: waitToComplete,
	}

	_, err := s.apiClient.BlockFound(ctx, req)
	if err != nil {
		return err
	}

	return nil
}

func (s *Client) ProcessBlock(ctx context.Context, block *model.Block, blockHeight uint32) error {
	blockBytes, err := block.Bytes()
	if err != nil {
		return err
	}

	req := &blockvalidation_api.ProcessBlockRequest{
		Block:  blockBytes,
		Height: blockHeight,
	}

	_, err = s.apiClient.ProcessBlock(ctx, req)
	if err != nil {
		return err
	}

	return nil
}

func (s *Client) SubtreeFound(ctx context.Context, subtreeHash *chainhash.Hash, baseUrl string) error {
	req := &blockvalidation_api.SubtreeFoundRequest{
		Hash:    subtreeHash.CloneBytes(),
		BaseUrl: baseUrl,
	}

	_, err := s.apiClient.SubtreeFound(ctx, req)
	if err != nil {
		return err
	}

	return nil
}

func (s *Client) Get(ctx context.Context, subtreeHash []byte) ([]byte, error) {
	if s.httpAddress != "" {
		// try the http endpoint first, if that fails we can try the grpc endpoint
		subtreeBytes, err := util.DoHTTPRequest(ctx, s.httpAddress+"/subtree/"+utils.ReverseAndHexEncodeSlice(subtreeHash), nil)
		if err != nil {
			s.logger.Warnf("error getting subtree %x from blockvalidation http endpoint: %s", subtreeHash, err)
		} else if subtreeBytes != nil {
			return subtreeBytes, nil
		}
	}

	req := &blockvalidation_api.GetSubtreeRequest{
		Hash: subtreeHash,
	}

	response, err := s.apiClient.Get(ctx, req)
	if err != nil {
		return nil, err
	}

	return response.Subtree, nil
}

func (s *Client) Exists(ctx context.Context, subtreeHash []byte) (bool, error) {
	req := &blockvalidation_api.ExistsSubtreeRequest{
		Hash: subtreeHash,
	}

	response, err := s.apiClient.Exists(ctx, req)
	if err != nil {
		return false, err
	}

	return response.Exists, nil
}

func (s *Client) Set(_ context.Context, _ []byte, _ []byte, _ ...options.Options) error {
	return errors.NewError("not implemented")
}

func (s *Client) SetTTL(_ context.Context, _ []byte, _ time.Duration) error {
	return errors.NewError("not implemented")
}

func (s *Client) SetTxMeta(ctx context.Context, txMetaData []*meta.Data) error {
	func() {
		// can throw a segmentation violation when the blockvalidation service is not available :-(
		err := recover()
		if err != nil {
			s.logger.Errorf("Recovered from panic: %v", err)
		}
	}()

	txMetaDataSlice := make([][]byte, 0, len(txMetaData))

	for _, data := range txMetaData {
		hash := data.Tx.TxIDChainHash()

		b := hash.CloneBytes()

		temp := data.Tx
		data.Tx = nil // clear the tx from the data so we don't send it over the wire
		b = append(b, data.MetaBytes()...)
		data.Tx = temp // restore the tx, incase we need to try again

		txMetaDataSlice = append(txMetaDataSlice, b)
	}

	_, err := s.apiClient.SetTxMeta(ctx, &blockvalidation_api.SetTxMetaRequest{
		Data: txMetaDataSlice,
	})
	if err != nil {
		return err
	}

	return nil
}

func (s *Client) DelTxMeta(ctx context.Context, hash *chainhash.Hash) error {
	_, err := s.apiClient.DelTxMeta(ctx, &blockvalidation_api.DelTxMetaRequest{
		Hash: hash.CloneBytes(),
	})
	if err != nil {
		return err
	}

	return nil
}

func (s *Client) SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash, blockID uint32) (err error) {
	req := &blockvalidation_api.SetMinedMultiRequest{
		Hashes:  make([][]byte, 0, len(hashes)),
		BlockId: blockID,
	}

	for _, hash := range hashes {
		req.Hashes = append(req.Hashes, hash.CloneBytes())
	}

	_, err = s.apiClient.SetMinedMulti(ctx, req)
	if err != nil {
		return err
	}

	return nil
}
