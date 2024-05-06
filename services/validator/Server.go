package validator

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/bitcoin-sv/ubsv/services/validator/validator_api"
	txmetastore "github.com/bitcoin-sv/ubsv/stores/txmeta"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/tracing"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/ordishs/gocore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type subscriber struct {
	subscription validator_api.ValidatorAPI_SubscribeServer
	source       string
	done         chan struct{}
}

// Server type carries the logger within it
type Server struct {
	validator_api.UnsafeValidatorAPIServer
	validator         Interface
	logger            ulogger.Logger
	utxoStore         utxostore.Interface
	txMetaStore       txmetastore.Store
	kafkaSignal       chan os.Signal
	newSubscriptions  chan subscriber
	deadSubscriptions chan subscriber
	subscribers       map[subscriber]bool
	stats             *gocore.Stat
}

// NewServer will return a server instance with the logger stored within it
func NewServer(logger ulogger.Logger, utxoStore utxostore.Interface, txMetaStore txmetastore.Store) *Server {
	initPrometheusMetrics()

	return &Server{
		logger:            logger,
		utxoStore:         utxoStore,
		txMetaStore:       txMetaStore,
		newSubscriptions:  make(chan subscriber, 10),
		deadSubscriptions: make(chan subscriber, 10),
		subscribers:       make(map[subscriber]bool),
		stats:             gocore.NewStat("validator"),
	}
}

func (v *Server) Health(ctx context.Context) (int, string, error) {
	return 0, "", nil
}

func (v *Server) Init(ctx context.Context) (err error) {
	v.validator, err = New(ctx, v.logger, v.utxoStore, v.txMetaStore)
	if err != nil {
		return fmt.Errorf("could not create validator [%w]", err)
	}

	return nil
}

// Start function
func (v *Server) Start(ctx context.Context) error {

	go func() {
		for {
			select {
			case <-ctx.Done():
				v.logger.Infof("[Validator] Stopping channel listeners go routine")
				for sub := range v.subscribers {
					safeClose(sub.done)
				}
				return
			case s := <-v.newSubscriptions:
				v.subscribers[s] = true
				v.logger.Infof("[Validator] New Subscription received from %s (Total=%d).", s.source, len(v.subscribers))

			case s := <-v.deadSubscriptions:
				delete(v.subscribers, s)
				safeClose(s.done)
				v.logger.Infof("[Validator] Subscription removed (Total=%d).", len(v.subscribers))
			}
		}
	}()

	// this will block
	if err := util.StartGRPCServer(ctx, v.logger, "validator", func(server *grpc.Server) {
		validator_api.RegisterValidatorAPIServer(server, v)
	}); err != nil {
		return err
	}

	return nil
}

func (v *Server) Stop(_ context.Context) error {
	if v.kafkaSignal != nil {
		v.kafkaSignal <- syscall.SIGTERM
	}

	return nil
}

func (v *Server) HealthGRPC(_ context.Context, _ *validator_api.EmptyMessage) (*validator_api.HealthResponse, error) {
	start := gocore.CurrentTime()
	defer func() {
		prometheusHealth.Inc()
		v.stats.NewStat("Health", true).AddTime(start)
	}()

	var sb strings.Builder
	errs := make([]error, 0)

	blockHeight, err := v.validator.GetBlockHeight()
	if err != nil {
		errs = append(errs, err)
		_, _ = sb.WriteString(fmt.Sprintf("BlockHeight: BAD: %v\n", err))
	} else {
		_, _ = sb.WriteString(fmt.Sprintf("BlockHeight: GOOD: %d\n", blockHeight))
	}

	if blockHeight <= 0 {
		errs = append(errs, errors.New("blockHeight <= 0"))
		_, _ = sb.WriteString(fmt.Sprintf("BlockHeight: BAD: %d\n", blockHeight))
	} else {
		_, _ = sb.WriteString(fmt.Sprintf("BlockHeight: GOOD: %d\n", blockHeight))
	}

	if len(errs) > 0 {
		return &validator_api.HealthResponse{
			Ok:        false,
			Details:   sb.String(),
			Timestamp: uint32(time.Now().Unix()),
		}, err
	}

	return &validator_api.HealthResponse{
		Ok:        true,
		Timestamp: uint32(time.Now().Unix()),
	}, nil
}

func (v *Server) ValidateTransactionStream(stream validator_api.ValidatorAPI_ValidateTransactionStreamServer) error {
	start := gocore.CurrentTime()
	defer func() {
		v.stats.NewStat("ValidateTransactionStream", true).AddTime(start)
	}()

	transactionData := bytes.Buffer{}

	for {
		log.Print("waiting to receive more data")

		req, err := stream.Recv()
		if err == io.EOF {
			log.Print("no more data")
			break
		}
		if err != nil {
			prometheusInvalidTransactions.Inc()
			return status.Errorf(codes.Unknown, "cannot receive chunk data: %v", err)
		}

		chunk := req.GetTransactionData()

		_, err = transactionData.Write(chunk)
		if err != nil {
			prometheusInvalidTransactions.Inc()
			return status.Errorf(codes.Internal, "cannot write chunk data: %v", err)
		}
	}

	var tx bt.Tx
	if _, err := tx.ReadFrom(bytes.NewReader(transactionData.Bytes())); err != nil {
		prometheusInvalidTransactions.Inc()
		return status.Errorf(codes.Internal, "cannot read transaction data: %v", err)
	}

	// increment prometheus counter
	prometheusProcessedTransactions.Inc()

	return stream.SendAndClose(&validator_api.ValidateTransactionResponse{
		Valid: true,
	})
}

func (v *Server) ValidateTransaction(cntxt context.Context, req *validator_api.ValidateTransactionRequest) (*validator_api.ValidateTransactionResponse, error) {
	start, stat, ctx := util.NewStatFromContext(cntxt, "ValidateTransaction", v.stats)
	defer func() {
		stat.AddTime(start)
		prometheusProcessedTransactions.Inc()
		prometheusTransactionDuration.Observe(float64(time.Since(start).Microseconds()) / 1_000_000)
	}()

	traceSpan := tracing.Start(ctx, "Validator:ValidateTransaction")
	defer traceSpan.Finish()

	transactionData := req.GetTransactionData()
	tx, err := bt.NewTxFromBytes(transactionData)
	if err != nil {
		prometheusInvalidTransactions.Inc()
		traceSpan.RecordError(err)
		return &validator_api.ValidateTransactionResponse{
			Valid: false,
		}, status.Errorf(codes.Internal, "cannot read transaction data: %v", err)
	}

	err = v.validator.Validate(ctx, tx, req.BlockHeight)
	if err != nil {
		prometheusInvalidTransactions.Inc()
		traceSpan.RecordError(err)
		v.sendInvalidTxNotification(tx.TxID(), err.Error())
		return &validator_api.ValidateTransactionResponse{
			Valid: false,
		}, status.Errorf(codes.Internal, "transaction %s is invalid: %v", tx.TxID(), err)
	}

	prometheusTransactionSize.Observe(float64(len(transactionData)))

	return &validator_api.ValidateTransactionResponse{
		Valid: true,
	}, nil
}

func (v *Server) ValidateTransactionBatch(cntxt context.Context, req *validator_api.ValidateTransactionBatchRequest) (*validator_api.ValidateTransactionBatchResponse, error) {
	start, stat, ctx := util.NewStatFromContext(cntxt, "ValidateTransactionBatch", v.stats)
	defer func() {
		stat.AddTime(start)
		prometheusTransactionValidateBatch.Observe(float64(time.Since(start).Microseconds()) / 1_000_000)
	}()

	errReasons := make([]*validator_api.ValidateTransactionError, 0, len(req.GetTransactions()))
	for _, reqItem := range req.GetTransactions() {
		tx, err := v.ValidateTransaction(ctx, reqItem)
		if err != nil {
			v.sendInvalidTxNotification(tx.String(), tx.Reason)

			errReasons = append(errReasons, &validator_api.ValidateTransactionError{
				TxId:   tx.String(),
				Reason: tx.Reason,
			})
		}
	}

	return &validator_api.ValidateTransactionBatchResponse{
		Valid:   true,
		Reasons: errReasons,
	}, nil
}

func (v *Server) GetBlockHeight(_ context.Context, _ *validator_api.EmptyMessage) (*validator_api.GetBlockHeightResponse, error) {
	blockHeight, err := v.validator.GetBlockHeight()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "cannot get block height: %v", err)
	}

	return &validator_api.GetBlockHeightResponse{
		Height: blockHeight,
	}, nil
}

func (v *Server) Subscribe(req *validator_api.SubscribeRequest, sub validator_api.ValidatorAPI_SubscribeServer) error {
	// prometheusBlockchainSubscribe.Inc()
	v.logger.Debugf("subscribe request from %s", req.Source)
	// Keep this subscription alive without endless loop - use a channel that blocks forever.
	ch := make(chan struct{})

	v.newSubscriptions <- subscriber{
		subscription: sub,
		done:         ch,
		source:       req.Source,
	}

	for {
		select {
		case <-sub.Context().Done():
			// Client disconnected.
			v.logger.Infof("[Validator] GRPC client disconnected: %s", req.Source)
			return nil
		case <-ch:
			// Subscription ended.
			return nil
		}
	}
}

func (v *Server) sendInvalidTxNotification(txId string, reason string) {
	notification := &validator_api.RejectedTxNotification{
		TxId:   txId,
		Reason: reason,
	}
	v.logger.Debugf("[Validator] Sending notification: %s", notification)
	for sub := range v.subscribers {
		v.logger.Debugf("[Validator] Sending notification to %s in background: %s", sub.source, notification)
		go func(s subscriber) {
			v.logger.Debugf("[Validator] Sending notification to %s: %s", s.source, notification)
			if err := s.subscription.Send(notification); err != nil {
				v.logger.Errorf("[Validator] Error sending notification to %s: %s", s.source, err)
				v.deadSubscriptions <- s
			}
		}(sub)
	}
}

func safeClose[T any](ch chan T) {
	defer func() {
		_ = recover()
	}()

	close(ch)
}
