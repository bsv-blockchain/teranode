package utxo

import (
	"context"
	"time"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/services/utxo/utxostore_api"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/tracing"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// UTXOStore type carries the logger within it
type UTXOStore struct {
	utxostore_api.UnsafeUtxoStoreAPIServer
	logger utils.Logger
	store  utxostore.Interface
}

func Enabled() bool {
	_, found := gocore.Config().Get("utxostore_grpcAddress")
	return found
}

// New will return a server instance with the logger stored within it
func New(logger utils.Logger, s utxostore.Interface, opts ...Options) *UTXOStore {
	initPrometheusMetrics()

	if len(opts) > 0 {
		for _, opt := range opts {
			opt(s)
		}
	}

	return &UTXOStore{
		logger: logger,
		store:  s,
	}
}

func (u *UTXOStore) Init(_ context.Context) error {
	return nil
}

// Start function
func (u *UTXOStore) Start(ctx context.Context) error {

	// get the latest block height to compare against lock time utxos
	blockchainClient, err := blockchain.NewClient(ctx)
	if err != nil {
		return err
	}
	blockchainSubscriptionCh, err := blockchainClient.Subscribe(ctx, "UTXOServer")
	if err != nil {
		return err
	}

	_, height, err := blockchainClient.GetBestBlockHeader(ctx)
	if err != nil {
		u.logger.Errorf("[UTXOServer] error getting best block header: %v", err)
	} else {
		u.logger.Debugf("[UTXOServer] setting block height to %d", height)
		_ = u.store.SetBlockHeight(height)
	}

	u.logger.Infof("[UTXOServer] starting block height subscription")
	go func() {
		for {
			select {
			case <-ctx.Done():
				u.logger.Infof("[UTXOServer] shutting down block height subscription")
				return
			case notification := <-blockchainSubscriptionCh:
				if notification.Type == model.NotificationType_Block {
					_, height, err = blockchainClient.GetBestBlockHeader(ctx)
					if err != nil {
						u.logger.Errorf("[UTXOServer] error getting best block header: %v", err)
						continue
					}
					u.logger.Debugf("[UTXOServer] setting block height to %d", height)
					_ = u.store.SetBlockHeight(height)
				}
			}
		}
	}()

	// this will block
	if err = util.StartGRPCServer(ctx, u.logger, "utxo", func(server *grpc.Server) {
		utxostore_api.RegisterUtxoStoreAPIServer(server, u)
	}); err != nil {
		return err
	}

	return nil
}

func (u *UTXOStore) Stop(_ context.Context) error {
	return nil
}

func (u *UTXOStore) Health(_ context.Context, _ *emptypb.Empty) (*utxostore_api.HealthResponse, error) {
	prometheusUtxoGet.Inc()

	return &utxostore_api.HealthResponse{
		Ok:        true,
		Details:   "GRPC Store",
		Timestamp: timestamppb.New(time.Now()),
	}, nil
}

func (u *UTXOStore) Store(ctx context.Context, req *utxostore_api.StoreRequest) (*utxostore_api.StoreResponse, error) {
	traceSpan := tracing.Start(ctx, "UTXOStore:Store")
	defer traceSpan.Finish()

	utxoHash, err := chainhash.NewHash(req.UtxoHash)
	if err != nil {
		return nil, err
	}

	resp, err := u.store.Store(traceSpan.Ctx, utxoHash, req.LockTime)
	if err != nil {
		return nil, err
	}

	prometheusUtxoStore.Inc()

	return &utxostore_api.StoreResponse{
		Status: utxostore_api.Status(resp.Status),
	}, nil
}

func (u *UTXOStore) BatchStore(ctx context.Context, req *utxostore_api.BatchStoreRequest) (*utxostore_api.BatchStoreResponse, error) {
	traceSpan := tracing.Start(ctx, "UTXOStore:BatchStore")
	defer traceSpan.Finish()

	var err error
	utxoHashes := make([]*chainhash.Hash, len(req.UtxoHashes))
	for idx, hash := range req.UtxoHashes {
		utxoHashes[idx], err = chainhash.NewHash(hash)
		if err != nil {
			return nil, err
		}
	}

	resp, err := u.store.BatchStore(traceSpan.Ctx, utxoHashes, req.LockTime)
	if err != nil {
		return nil, err
	}

	prometheusUtxoStore.Inc()

	return &utxostore_api.BatchStoreResponse{
		Status: utxostore_api.Status(resp.Status),
	}, nil
}

func (u *UTXOStore) Spend(ctx context.Context, req *utxostore_api.SpendRequest) (*utxostore_api.SpendResponse, error) {
	traceSpan := tracing.Start(ctx, "UTXOStore:Spend")
	defer traceSpan.Finish()

	utxoHash, err := chainhash.NewHash(req.UxtoHash)
	if err != nil {
		return nil, err
	}

	spendingHash, err := chainhash.NewHash(req.SpendingTxid)
	if err != nil {
		return nil, err
	}

	resp, err := u.store.Spend(traceSpan.Ctx, utxoHash, spendingHash)
	if err != nil {
		return nil, err
	}

	prometheusUtxoSpend.Inc()

	var spendingTxID []byte
	if resp.SpendingTxID != nil {
		spendingTxID = resp.SpendingTxID[:]
	}

	return &utxostore_api.SpendResponse{
		Status:       utxostore_api.Status(resp.Status),
		SpendingTxid: spendingTxID,
	}, nil
}

func (u *UTXOStore) Reset(ctx context.Context, req *utxostore_api.ResetRequest) (*utxostore_api.ResetResponse, error) {
	traceSpan := tracing.Start(ctx, "UTXOStore:Reset")
	defer traceSpan.Finish()

	utxoHash, err := chainhash.NewHash(req.UxtoHash)
	if err != nil {
		return nil, err
	}

	resp, err := u.store.Reset(traceSpan.Ctx, utxoHash)
	if err != nil {
		return nil, err
	}

	prometheusUtxoReset.Inc()

	return &utxostore_api.ResetResponse{
		Status: utxostore_api.Status(resp.Status),
	}, nil
}

func (u *UTXOStore) Delete(ctx context.Context, req *utxostore_api.DeleteRequest) (*utxostore_api.DeleteResponse, error) {
	traceSpan := tracing.Start(ctx, "UTXOStore:Delete")
	defer traceSpan.Finish()

	utxoHash, err := chainhash.NewHash(req.UxtoHash)
	if err != nil {
		return nil, err
	}

	resp, err := u.store.Delete(traceSpan.Ctx, utxoHash)
	if err != nil {
		return nil, err
	}

	prometheusUtxoReset.Inc()

	return &utxostore_api.DeleteResponse{
		Status: utxostore_api.Status(resp.Status),
	}, nil
}

func (u *UTXOStore) Get(ctx context.Context, req *utxostore_api.GetRequest) (*utxostore_api.GetResponse, error) {
	traceSpan := tracing.Start(ctx, "UTXOStore:Get")
	defer traceSpan.Finish()

	utxoHash, err := chainhash.NewHash(req.UxtoHash)
	if err != nil {
		return nil, err
	}

	resp, err := u.store.Get(traceSpan.Ctx, utxoHash)
	if err != nil {
		return nil, err
	}

	prometheusUtxoGet.Inc()

	r := &utxostore_api.GetResponse{
		Status: utxostore_api.Status(resp.Status),
	}

	if resp.SpendingTxID != nil {
		r.SpendingTxid = resp.SpendingTxID.CloneBytes()
	}

	return r, nil
}
