package blockvalidation

import (
	"context"
	"fmt"
	"time"

	"github.com/bitcoin-sv/ubsv/services/blockvalidation/blockvalidation_api"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	txmeta_store "github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
)

type Client struct {
	apiClient  blockvalidation_api.BlockValidationAPIClient
	frpcClient *blockvalidation_api.Client
	logger     ulogger.Logger
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

	go client.connectFRPC(ctx)

	return client
}

func (s *Client) connectFRPC(ctx context.Context) {
	func() {
		err := recover()
		if err != nil {
			s.logger.Errorf("Error connecting to blockvalidation fRPC: %s", err)
		}
	}()

	// we cannot start the frpc client connection immediately, since it relies on the blockvalidation server being up
	// in the meantime, everything will be sent over grpc, which should be fine
	time.Sleep(10 * time.Second)

	blockvalidationFRPCAddress, ok := gocore.Config().Get("blockvalidation_frpcAddress")
	if ok {
		maxRetries := 3
		retryInterval := 5 * time.Second

		for i := 0; i < maxRetries; i++ {
			s.logger.Infof("attempting to create fRPC connection to blockvalidation, attempt %d", i+1)

			client, err := blockvalidation_api.NewClient(nil, nil)
			if err != nil {
				s.logger.Fatalf("error creating new fRPC client in blockvalidation: %s", err)
			}

			err = client.Connect(blockvalidationFRPCAddress)
			if err != nil {
				s.logger.Warnf("error connecting to fRPC server in blockvalidation: %s", err)
				if i+1 == maxRetries {
					break
				}
				time.Sleep(retryInterval)
				retryInterval *= 2
			} else {
				s.logger.Infof("connected to blockvalidation fRPC server")
				s.frpcClient = client
				break
			}
		}

		if s.frpcClient == nil {
			s.logger.Fatalf("failed to connect to blockvalidation fRPC server after %d attempts", maxRetries)
		} else {
			/* listen for close channel and reconnect */
			s.logger.Infof("Listening for close channel on fRPC client")
			go func() {
				for {
					select {
					case <-ctx.Done():
						s.logger.Infof("fRPC client context done, closing channel")
						return
					case <-s.frpcClient.CloseChannel():
						s.logger.Infof("fRPC client close channel received, reconnecting...")
						s.connectFRPC(ctx)
					}
				}
			}()
		}
	}
}

func (s *Client) Health(ctx context.Context) (bool, error) {
	_, err := s.apiClient.Health(ctx, &blockvalidation_api.EmptyMessage{})
	if err != nil {
		return false, err
	}

	return true, nil
}

func (s *Client) BlockFound(ctx context.Context, blockHash *chainhash.Hash, baseUrl string) error {
	req := &blockvalidation_api.BlockFoundRequest{
		Hash:    blockHash.CloneBytes(),
		BaseUrl: baseUrl,
	}

	_, err := s.apiClient.BlockFound(ctx, req)
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
	req := &blockvalidation_api.GetSubtreeRequest{
		Hash: subtreeHash,
	}

	response, err := s.apiClient.Get(ctx, req)
	if err != nil {
		return nil, err
	}

	return response.Subtree, nil
}

func (s *Client) Set(ctx context.Context, key []byte, value []byte, opts ...options.Options) error {
	return fmt.Errorf("not implemented")
}

func (s *Client) SetTTL(ctx context.Context, key []byte, ttl time.Duration) error {
	return fmt.Errorf("not implemented")
}

func (s *Client) SetTxMeta(ctx context.Context, txMetaData []*txmeta_store.Data) error {
	txMetaDataSlice := make([][]byte, 0, len(txMetaData))

	for _, data := range txMetaData {
		hash := data.Tx.TxIDChainHash()

		b := hash.CloneBytes()

		data.Tx = nil
		b = append(b, data.MetaBytes()...)

		txMetaDataSlice = append(txMetaDataSlice, b)
	}

	if s.frpcClient != nil {
		_, err := s.frpcClient.BlockValidationAPI.SetTxMeta(ctx, &blockvalidation_api.BlockvalidationApiSetTxMetaRequest{
			Data: txMetaDataSlice,
		})
		if err != nil {
			return err
		}
		return nil
	}

	_, err := s.apiClient.SetTxMeta(ctx, &blockvalidation_api.SetTxMetaRequest{
		Data: txMetaDataSlice,
	})
	if err != nil {
		return err
	}

	return nil
}
