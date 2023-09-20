package validator

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"strconv"
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
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Server type carries the logger within it
type Server struct {
	validator_api.UnsafeValidatorAPIServer
	validator   Interface
	logger      utils.Logger
	utxoStore   utxostore.Interface
	txMetaStore txmetastore.Store
	kafkaSignal chan os.Signal
}

func Enabled() bool {
	_, found := gocore.Config().Get("validator_grpcListenAddress")
	return found
}

// NewServer will return a server instance with the logger stored within it
func NewServer(logger utils.Logger, utxoStore utxostore.Interface, txMetaStore txmetastore.Store) *Server {
	return &Server{
		logger:      logger,
		utxoStore:   utxoStore,
		txMetaStore: txMetaStore,
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

func (v *Server) Health(_ context.Context, _ *emptypb.Empty) (*validator_api.HealthResponse, error) {
	prometheusHealth.Inc()
	return &validator_api.HealthResponse{
		Ok:        true,
		Timestamp: timestamppb.New(time.Now()),
	}, nil
}

func (v *Server) ValidateTransactionStream(stream validator_api.ValidatorAPI_ValidateTransactionStreamServer) error {
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
			return v.logError(status.Errorf(codes.Unknown, "cannot receive chunk data: %v", err))
		}

		chunk := req.GetTransactionData()

		_, err = transactionData.Write(chunk)
		if err != nil {
			prometheusInvalidTransactions.Inc()
			return v.logError(status.Errorf(codes.Internal, "cannot write chunk data: %v", err))
		}
	}

	var tx bt.Tx
	if _, err := tx.ReadFrom(bytes.NewReader(transactionData.Bytes())); err != nil {
		prometheusInvalidTransactions.Inc()
		return v.logError(status.Errorf(codes.Internal, "cannot read transaction data: %v", err))
	}

	// increment prometheus counter
	prometheusProcessedTransactions.Inc()

	return stream.SendAndClose(&validator_api.ValidateTransactionResponse{
		Valid: true,
	})
}

func (v *Server) ValidateTransaction(ctx context.Context, req *validator_api.ValidateTransactionRequest) (*validator_api.ValidateTransactionResponse, error) {
	prometheusProcessedTransactions.Inc()
	timeStart := time.Now()
	traceSpan := tracing.Start(ctx, "Validator:ValidateTransaction")
	defer traceSpan.Finish()

	tx, err := bt.NewTxFromBytes(req.TransactionData)
	if err != nil {
		prometheusInvalidTransactions.Inc()
		traceSpan.RecordError(err)
		return nil, v.logError(status.Errorf(codes.Internal, "cannot read transaction data: %v", err))
	}

	err = v.validator.Validate(traceSpan.Ctx, tx)
	if err != nil {
		prometheusInvalidTransactions.Inc()
		traceSpan.RecordError(err)
		return &validator_api.ValidateTransactionResponse{
			Valid:  false,
			Reason: fmt.Sprintf("transaction %s is invalid: %v", tx.TxID(), err),
		}, nil
	}

	prometheusTransactionSize.Observe(float64(len(req.TransactionData)))
	prometheusTransactionDuration.Observe(float64(time.Since(timeStart).Microseconds()))

	return &validator_api.ValidateTransactionResponse{
		Valid: true,
	}, nil
}

func (v *Server) logError(err error) error {
	if err != nil {
		v.logger.Errorf("%v", err)
	}
	return err
}
