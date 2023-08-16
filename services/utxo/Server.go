package utxo

import (
	"context"
	"time"

	"github.com/bitcoin-sv/ubsv/services/utxo/utxostore_api"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/tracing"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	prometheusUtxoGet   prometheus.Counter
	prometheusUtxoStore prometheus.Counter
	// prometheusUtxoReStore    prometheus.Counter
	// prometheusUtxoStoreSpent prometheus.Counter
	prometheusUtxoSpend prometheus.Counter
	// prometheusUtxoReSpend    prometheus.Counter
	// prometheusUtxoSpendSpent prometheus.Counter
	prometheusUtxoReset prometheus.Counter
)

func init() {
	prometheusUtxoGet = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "utxostore_utxo_get",
			Help: "Number of utxo get calls done to utxostore",
		},
	)
	prometheusUtxoStore = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "utxostore_utxo_store",
			Help: "Number of utxo store calls done to utxostore",
		},
	)
	//prometheusUtxoStoreSpent = promauto.NewCounter(
	//	prometheus.CounterOpts{
	//		Name: "utxostore_utxo_store_spent",
	//		Help: "Number of utxo store calls that were already spent to utxostore",
	//	},
	//)
	//prometheusUtxoReStore = promauto.NewCounter(
	//	prometheus.CounterOpts{
	//		Name: "utxostore_utxo_restore",
	//		Help: "Number of utxo restore calls done to utxostore",
	//	},
	//)
	prometheusUtxoSpend = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "utxostore_utxo_spend",
			Help: "Number of utxo spend calls done to utxostore",
		},
	)
	//prometheusUtxoReSpend = promauto.NewCounter(
	//	prometheus.CounterOpts{
	//		Name: "utxostore_utxo_respend",
	//		Help: "Number of utxo respend calls done to utxostore",
	//	},
	//)
	//prometheusUtxoSpendSpent = promauto.NewCounter(
	//	prometheus.CounterOpts{
	//		Name: "utxostore_utxo_spend_spent",
	//		Help: "Number of utxo spend calls that were already spent done to utxostore",
	//	},
	//)
	prometheusUtxoReset = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "utxostore_utxo_reset",
			Help: "Number of utxo reset calls done to utxostore",
		},
	)
}

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
	// this will block
	if err := util.StartGRPCServer(ctx, u.logger, "utxo", func(server *grpc.Server) {
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
		Timestamp: timestamppb.New(time.Now()),
	}, nil
}

func (u *UTXOStore) Store(ctx context.Context, req *utxostore_api.StoreRequest) (*utxostore_api.StoreResponse, error) {
	traceSpan := tracing.Start(ctx, "Interface:Store")
	defer traceSpan.Finish()

	utxoHash, err := chainhash.NewHash(req.UxtoHash)
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

func (u *UTXOStore) Spend(ctx context.Context, req *utxostore_api.SpendRequest) (*utxostore_api.SpendResponse, error) {
	traceSpan := tracing.Start(ctx, "Interface:Spend")
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
	traceSpan := tracing.Start(ctx, "Interface:MoveUpBlock")
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

func (u *UTXOStore) Get(ctx context.Context, req *utxostore_api.GetRequest) (*utxostore_api.GetResponse, error) {
	traceSpan := tracing.Start(ctx, "Interface:Get")
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
