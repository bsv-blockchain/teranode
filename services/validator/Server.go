package validator

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	"github.com/bitcoin-sv/ubsv/services/validator/validator_api"
	txmetastore "github.com/bitcoin-sv/ubsv/stores/txmeta"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/tracing"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"storj.io/drpc/drpcmux"
	"storj.io/drpc/drpcserver"
)

var stats = gocore.NewStat("validator")

type subscriber struct {
	subscription validator_api.ValidatorAPI_SubscribeServer
	source       string
	done         chan struct{}
}

// Server type carries the logger within it
type Server struct {
	validator_api.UnsafeValidatorAPIServer
	validator           Interface
	logger              utils.Logger
	utxoStore           utxostore.Interface
	txMetaStore         txmetastore.Store
	kafkaSignal         chan os.Signal
	newSubscriptions    chan subscriber
	deadSubscriptions   chan subscriber
	subscribers         map[subscriber]bool
	subscriptionCtx     context.Context
	cancelSubscriptions context.CancelFunc
}

func Enabled() bool {
	_, found := gocore.Config().Get("validator_grpcListenAddress")
	return found
}

// NewServer will return a server instance with the logger stored within it
func NewServer(logger utils.Logger, utxoStore utxostore.Interface, txMetaStore txmetastore.Store) *Server {
	initPrometheusMetrics()
	subscriptionCtx, cancelSubscriptions := context.WithCancel(context.Background())

	return &Server{
		logger:              logger,
		utxoStore:           utxoStore,
		txMetaStore:         txMetaStore,
		newSubscriptions:    make(chan subscriber, 10),
		deadSubscriptions:   make(chan subscriber, 10),
		subscribers:         make(map[subscriber]bool),
		subscriptionCtx:     subscriptionCtx,
		cancelSubscriptions: cancelSubscriptions,
	}
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

	kafkaBrokers, ok := gocore.Config().Get("validator_kafkaBrokers")
	if ok {
		v.logger.Infof("[Validator] Starting Kafka validator on address: %s", kafkaBrokers)
		kafkaURL, err := url.Parse(kafkaBrokers)
		if err != nil {
			v.logger.Errorf("[Validator] Kafka validator failed to start: %s", err)
		} else {
			workers, _ := gocore.Config().GetInt("validator_kafkaWorkers", 100)
			v.logger.Infof("[Validator] Kafka consumer started with %d workers", workers)

			var clusterAdmin sarama.ClusterAdmin

			n := atomic.Uint64{}
			workerCh := make(chan []byte)
			for i := 0; i < workers; i++ {
				go func() {
					var response *validator_api.ValidateTransactionResponse
					for txBytes := range workerCh {
						response, err = v.ValidateTransaction(ctx, &validator_api.ValidateTransactionRequest{
							TransactionData: txBytes,
						})
						if err != nil {
							v.logger.Errorf("[Validator] Error validating transaction: %s", err)
						}
						if !response.Valid {
							v.logger.Errorf("[Validator] Invalid transaction: %s", response.Reason)
						}
						processedN := n.Add(1)
						if processedN%1000 == 0 {
							v.logger.Debugf("[Validator] Processed %d transactions", processedN)
						}
					}
				}()
			}

			go func() {
				clusterAdmin, _, err = util.ConnectToKafka(kafkaURL)
				if err != nil {
					log.Fatal("[Validator] unable to connect to kafka: ", err)
				}
				defer func() { _ = clusterAdmin.Close() }()

				topic := kafkaURL.Path[1:]

				var partitions int
				if partitions, err = strconv.Atoi(kafkaURL.Query().Get("partitions")); err != nil {
					log.Fatal("[Validator] unable to parse Kafka partitions: ", err)
				}

				var replicationFactor int
				if replicationFactor, err = strconv.Atoi(kafkaURL.Query().Get("replication")); err != nil {
					log.Fatal("[Validator] unable to parse Kafka replication factor: ", err)
				}

				_ = clusterAdmin.CreateTopic(topic, &sarama.TopicDetail{
					NumPartitions:     int32(partitions),
					ReplicationFactor: int16(replicationFactor),
				}, false)

				err = util.StartKafkaGroupListener(ctx, v.logger, kafkaURL, "validators", workerCh)
				if err != nil {
					v.logger.Errorf("[Validator] Kafka listener failed to start: %s", err)
				}
			}()
		}
	}

	// Experimental DRPC server - to test throughput at scale
	drpcAddress, ok := gocore.Config().Get("validator_drpcListenAddress")
	if ok {
		err := v.drpcServer(ctx, drpcAddress)
		if err != nil {
			v.logger.Errorf("failed to start DRPC server: %v", err)
		}
	}

	// Experimental fRPC server - to test throughput at scale
	frpcAddress, ok := gocore.Config().Get("validator_frpcListenAddress")
	if ok {
		err := v.frpcServer(ctx, frpcAddress)
		if err != nil {
			v.logger.Errorf("failed to start fRPC server: %v", err)
		}
	}

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

func (v *Server) Health(_ context.Context, _ *validator_api.EmptyMessage) (*validator_api.HealthResponse, error) {
	start := gocore.CurrentTime()
	defer func() {
		prometheusHealth.Inc()
		stats.NewStat("Health", true).AddTime(start)
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
		stats.NewStat("ValidateTransactionStream", true).AddTime(start)
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
	start, stat, ctx := util.NewStatFromContext(cntxt, "ValidateTransaction", stats)
	defer func() {
		stat.AddTime(start)
	}()

	prometheusProcessedTransactions.Inc()
	timeStart := time.Now()
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

	err = v.validator.Validate(ctx, tx)
	if err != nil {
		prometheusInvalidTransactions.Inc()
		traceSpan.RecordError(err)
		v.sendInvalidTxNotification(tx.TxID(), err.Error())
		return &validator_api.ValidateTransactionResponse{
			Valid: false,
		}, status.Errorf(codes.Internal, "transaction %s is invalid: %v", tx.TxID(), err)
	}

	prometheusTransactionSize.Observe(float64(len(transactionData)))
	prometheusTransactionDuration.Observe(float64(time.Since(timeStart).Microseconds()))

	return &validator_api.ValidateTransactionResponse{
		Valid: true,
	}, nil
}

func (v *Server) ValidateTransactionBatch(cntxt context.Context, req *validator_api.ValidateTransactionBatchRequest) (*validator_api.ValidateTransactionBatchResponse, error) {
	start, stat, ctx := util.NewStatFromContext(cntxt, "ValidateTransactionBatch", stats)
	defer func() {
		stat.AddTime(start)
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

func (v *Server) drpcServer(ctx context.Context, drpcAddress string) error {
	v.logger.Infof("Starting DRPC server on %s", drpcAddress)
	m := drpcmux.New()
	// register the proto-specific methods on the mux
	err := validator_api.DRPCRegisterValidatorAPI(m, v)
	if err != nil {
		return fmt.Errorf("failed to register DRPC service: %v", err)
	}
	// create the drpc server
	s := drpcserver.New(m)

	// listen on a tcp socket
	var lis net.Listener
	lis, err = net.Listen("tcp", drpcAddress)
	if err != nil {
		return fmt.Errorf("failed to listen on drpc server: %v", err)
	}

	// run the server
	// N.B.: if you want TLS, you need to wrap the net.Listener with
	// TLS before passing to Serve here.
	go func() {
		err = s.Serve(ctx, lis)
		if err != nil {
			v.logger.Errorf("failed to serve drpc: %v", err)
		}
	}()

	return nil
}

func (v *Server) frpcServer(ctx context.Context, frpcAddress string) error {
	v.logger.Infof("Starting fRPC server on %s", frpcAddress)

	frpcValidator := &fRPC_Validator{
		v: v,
	}

	s, err := validator_api.NewServer(frpcValidator, nil, nil)
	if err != nil {
		return fmt.Errorf("failed to create fRPC server: %v", err)
	}

	concurrency, ok := gocore.Config().GetInt("validator_frpcConcurrency")
	if ok {
		v.logger.Infof("Setting fRPC server concurrency to %d", concurrency)
		s.SetConcurrency(uint64(concurrency))
	}

	// run the server
	go func() {
		err = s.Start(frpcAddress)
		if err != nil {
			v.logger.Errorf("failed to serve frpc: %v", err)
		}
	}()

	go func() {
		<-ctx.Done()
		err = s.Shutdown()
		if err != nil {
			v.logger.Errorf("failed to shutdown frpc server: %v", err)
		}
	}()

	return nil
}

func (v *Server) Subscribe(req *validator_api.SubscribeRequest, sub validator_api.ValidatorAPI_SubscribeServer) error {
	// prometheusBlockchainSubscribe.Inc()

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
				v.deadSubscriptions <- s
			}
		}(sub)
	}
}
