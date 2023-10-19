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
	"github.com/libsv/go-bt/v2"
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

func (u *UTXOStore) Store(ctx context.Context, req *utxostore_api.StoreRequest) (*emptypb.Empty, error) {
	traceSpan := tracing.Start(ctx, "UTXOStore:Store")
	defer traceSpan.Finish()

	tx, err := bt.NewTxFromBytes(req.Tx)
	if err != nil {
		return nil, err
	}

	err = u.store.Store(traceSpan.Ctx, tx, req.LockTime)
	if err != nil {
		return nil, err
	}

	prometheusUtxoStore.Inc()

	return &emptypb.Empty{}, nil
}

func (u *UTXOStore) Spend(ctx context.Context, req *utxostore_api.Request) (*emptypb.Empty, error) {
	traceSpan := tracing.Start(ctx, "UTXOStore:Spend")
	defer traceSpan.Finish()

	spend, err := u.getSpends(req)
	if err != nil {
		return nil, err
	}

	err = u.store.Spend(traceSpan.Ctx, spend)
	if err != nil {
		return nil, err
	}

	prometheusUtxoSpend.Inc()

	return &emptypb.Empty{}, nil
}

func (u *UTXOStore) Reset(ctx context.Context, req *utxostore_api.Request) (*emptypb.Empty, error) {
	traceSpan := tracing.Start(ctx, "UTXOStore:Reset")
	defer traceSpan.Finish()

	spend, err := u.getSpends(req)
	if err != nil {
		return nil, err
	}

	err = u.store.UnSpend(traceSpan.Ctx, spend)
	if err != nil {
		return nil, err
	}

	prometheusUtxoReset.Inc()

	return &emptypb.Empty{}, nil
}

func (u *UTXOStore) Delete(ctx context.Context, req *utxostore_api.Request) (*emptypb.Empty, error) {
	traceSpan := tracing.Start(ctx, "UTXOStore:Delete")
	defer traceSpan.Finish()

	spends, err := u.getSpends(req)
	if err != nil {
		return nil, err
	}

	err = u.store.Delete(traceSpan.Ctx, spends[0])
	if err != nil {
		return nil, err
	}

	prometheusUtxoReset.Inc()

	return &emptypb.Empty{}, nil
}

func (u *UTXOStore) Get(ctx context.Context, req *utxostore_api.Request) (*utxostore_api.GetResponse, error) {
	traceSpan := tracing.Start(ctx, "UTXOStore:Get")
	defer traceSpan.Finish()

	spends, err := u.getSpends(req)
	if err != nil {
		return nil, err
	}

	resp, err := u.store.Get(traceSpan.Ctx, spends[0])
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

func (u *UTXOStore) getSpends(req *utxostore_api.Request) ([]*utxostore.Spend, error) {
	txHash, err := chainhash.NewHash(req.GetTxId())
	if err != nil {
		return nil, err
	}

	utxoHash, err := chainhash.NewHash(req.GetUxtoHash())
	if err != nil {
		return nil, err
	}

	var spendingHash *chainhash.Hash
	spendingTxID := req.GetSpendingTxid()
	if spendingTxID != nil {
		spendingHash, err = chainhash.NewHash(spendingTxID)
		if err != nil {
			return nil, err
		}
	}

	spend := []*utxostore.Spend{{
		TxID:         txHash,
		Vout:         req.GetVout(),
		Hash:         utxoHash,
		SpendingTxID: spendingHash,
	}}

	return spend, nil
}
