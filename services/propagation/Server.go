package propagation

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/hex"
	"encoding/pem"
	"io"
	"log"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/services/legacy/wire"
	"github.com/bitcoin-sv/ubsv/services/propagation/propagation_api"
	"github.com/bitcoin-sv/ubsv/services/validator"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/tracing"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/bitcoin-sv/ubsv/util/health"
	"github.com/bitcoin-sv/ubsv/util/kafka"
	"github.com/bitcoin-sv/ubsv/util/retry"
	"github.com/libsv/go-bt/v2"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	http3 "github.com/quic-go/quic-go/http3"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (

	// ipv6 multicast constants
	maxDatagramSize = 512 // 100 * 1024 * 1024
	ipv6Port        = 9999
)

// PropagationServer type carries the logger within it
type PropagationServer struct {
	propagation_api.UnsafePropagationAPIServer
	logger                       ulogger.Logger
	stats                        *gocore.Stat
	txStore                      blob.Store
	validator                    validator.Interface
	blockchainClient             blockchain.ClientI
	validatorKafkaProducerClient *kafka.KafkaAsyncProducer
	kafkaHealthURL               *url.URL
}

// New will return a server instance with the logger stored within it
func New(logger ulogger.Logger, txStore blob.Store, validatorClient validator.Interface, blockchainClient blockchain.ClientI) *PropagationServer {
	initPrometheusMetrics()

	return &PropagationServer{
		logger:           logger,
		stats:            gocore.NewStat("propagation"),
		txStore:          txStore,
		validator:        validatorClient,
		blockchainClient: blockchainClient,
	}
}

func (ps *PropagationServer) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	if checkLiveness {
		// Add liveness checks here. Don't include dependency checks.
		// If the service is stuck return http.StatusServiceUnavailable
		// to indicate a restart is needed
		return http.StatusOK, "OK", nil
	}

	// Add readiness checks here. Include dependency checks.
	// If any dependency is not ready, return http.StatusServiceUnavailable
	// If all dependencies are ready, return http.StatusOK
	// A failed dependency check does not imply the service needs restarting
	checks := []health.Check{
		{Name: "BlockchainClient", Check: ps.blockchainClient.Health},
		{Name: "ValidatorClient", Check: ps.validator.Health},
		{Name: "TxStore", Check: ps.txStore.Health},
		{Name: "FSM", Check: blockchain.CheckFSM(ps.blockchainClient)},
		{Name: "Kafka", Check: kafka.HealthChecker(ctx, ps.kafkaHealthURL)},
	}

	return health.CheckAll(ctx, checkLiveness, checks)
}

func (ps *PropagationServer) HealthGRPC(ctx context.Context, _ *propagation_api.EmptyMessage) (*propagation_api.HealthResponse, error) {
	_, _, deferFn := tracing.StartTracing(ctx, "HealthGRPC",
		tracing.WithParentStat(ps.stats),
		tracing.WithHistogram(prometheusHealth),
		tracing.WithDebugLogMessage(ps.logger, "[HealthGRPC] called"),
	)
	defer deferFn()

	status, details, err := ps.Health(ctx, false)

	return &propagation_api.HealthResponse{
		Ok:        status == http.StatusOK,
		Details:   details,
		Timestamp: timestamppb.Now(),
	}, errors.WrapGRPC(err)
}

func (ps *PropagationServer) Init(_ context.Context) (err error) {
	return nil
}

// Start function
func (ps *PropagationServer) Start(ctx context.Context) (err error) {
	// Check if we need to Restore. If so, move FSM to the Restore state
	// Restore will block and wait for RUN event to be manually sent
	// TODO: think if we can automate transition to RUN state after restore is complete.
	fsmStateRestore := gocore.Config().GetBool("fsm_state_restore", false)
	if fsmStateRestore {
		// Send Restore event to FSM
		if err = ps.blockchainClient.Restore(ctx); err != nil {
			ps.logger.Errorf("[Faucet] failed to send Restore event [%v], this should not happen, FSM will continue without Restoring", err)
		}

		// Wait for node to finish Restoring.
		// this means FSM got a RUN event and transitioned to RUN state
		// this will block
		ps.logger.Infof("[Faucet] Node is restoring, waiting for FSM to transition to Running state")
		_ = ps.blockchainClient.WaitForFSMtoTransitionToGivenState(ctx, blockchain.FSMStateRUNNING)
		ps.logger.Infof("[Faucet] Node finished restoring and has transitioned to Running state, continuing to start Faucet service")
	}

	ipv6Addresses, ok := gocore.Config().Get("ipv6_addresses")
	if ok {
		err = ps.StartUDP6Listeners(ctx, ipv6Addresses)
		if err != nil {
			return errors.NewServiceError("error starting ipv6 listeners", err)
		}
	}

	// Experimental QUIC server - to test throughput at scale
	quicAddress, ok := gocore.Config().Get("propagation_quicListenAddress")
	if ok && quicAddress != "" {
		// Create an error channel
		errChan := make(chan error, 1) // Buffered channel

		// Context for the QUIC server
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Run the QUIC server in a goroutine
		go func() {
			err := ps.quicServer(ctx, quicAddress)
			if err != nil {
				errChan <- err // Send any errors to the error channel
			}

			close(errChan) // Close the channel when done
		}()

		go func() {
			if err := <-errChan; err != nil {
				ps.logger.Errorf("failed to start QUIC server: %v", err)
			}
		}()
	}

	//  kafka channel setup
	validatortxsKafkaURL, _, found := gocore.Config().GetURL("kafka_validatortxsConfig")
	if !found {
		return errors.New(errors.ERR_CONFIGURATION, "kafka_validatortxs Config not found, validator channel configuration is required")
	}

	ps.logger.Infof("[Propagation Server] connecting to kafka for sending txs at %s", validatortxsKafkaURL)

	workers, _ := gocore.Config().GetInt("validator_kafkaWorkers", 100)
	if workers > 0 {
		ps.kafkaHealthURL = validatortxsKafkaURL
		ps.validatorKafkaProducerClient, err = retry.Retry(ctx, ps.logger, func() (*kafka.KafkaAsyncProducer, error) {
			return kafka.NewKafkaAsyncProducer(ps.logger, validatortxsKafkaURL, make(chan *kafka.Message, 10_000))
		}, retry.WithMessage("[Propagation Server] error starting kafka producer"))
		if err != nil {
			ps.logger.Errorf("[Propagation Server] error starting kafka producer: %v", err)
			return
		}

		go ps.validatorKafkaProducerClient.Start(ctx)

		ps.logger.Infof("[Propagation Server] connected to kafka for sending transactions to validator at %s", validatortxsKafkaURL)
	}

	// this will block
	maxConnectionAge, _, _ := gocore.Config().GetDuration("propagation_grpcMaxConnectionAge", 90*time.Second)
	if err = util.StartGRPCServer(ctx, ps.logger, "propagation", func(server *grpc.Server) {
		propagation_api.RegisterPropagationAPIServer(server, ps)
	}, maxConnectionAge); err != nil {
		return err
	}

	return nil
}

func (ps *PropagationServer) StartUDP6Listeners(ctx context.Context, ipv6Addresses string) error {
	ps.logger.Infof("Starting UDP6 listeners on %s", ipv6Addresses)

	ipv6Interface, ok := gocore.Config().Get("ipv6_interface", "en0")
	if !ok {
		// default to en0
		ipv6Interface = "en0"
	}

	useInterface, err := net.InterfaceByName(ipv6Interface)
	if err != nil {
		return errors.NewConfigurationError("error resolving interface", err)
	}

	for _, ipv6Address := range strings.Split(ipv6Addresses, ",") {
		var conn *net.UDPConn

		conn, err = net.ListenMulticastUDP("udp6", useInterface, &net.UDPAddr{
			IP:   net.ParseIP(ipv6Address),
			Port: ipv6Port,
			Zone: useInterface.Name,
		})
		if err != nil {
			return errors.NewServiceError("error starting listener", err)
		}

		go func(conn *net.UDPConn) {
			// Loop forever reading from the socket
			var (
				// numBytes int
				src *net.UDPAddr
				// oobn int
				// flags int
				msg   wire.Message
				b     []byte
				oobB  []byte
				msgTx *wire.MsgExtendedTx
			)

			buffer := make([]byte, maxDatagramSize)

			for {
				_, _, _, src, err = conn.ReadMsgUDP(buffer, oobB)
				if err != nil {
					log.Fatal("ReadFromUDP failed:", err)
				}
				// ps.logger.Infof("read %d bytes from %s, out of bounds data len %d", len(buffer), src.String(), len(oobB))

				reader := bytes.NewReader(buffer)

				msg, b, err = wire.ReadMessage(reader, wire.ProtocolVersion, wire.MainNet)
				if err != nil {
					ps.logger.Errorf("wire.ReadMessage failed: %v", err)
				}

				ps.logger.Infof("read %d bytes into wire message from %s", len(b), src.String())
				// ps.logger.Infof("wire message type: %v", msg)

				msgTx, ok = msg.(*wire.MsgExtendedTx)
				if ok {
					ps.logger.Infof("received %d bytes from %s", len(b), src.String())

					txBytes := bytes.NewBuffer(nil)
					if err = msgTx.Serialize(txBytes); err != nil {
						ps.logger.Errorf("error serializing transaction: %v", err)
						continue
					}

					// Process the received bytes
					go func(txb []byte) {
						if _, err = ps.ProcessTransaction(ctx, &propagation_api.ProcessTransactionRequest{
							Tx: txb,
						}); err != nil {
							ps.logger.Errorf("error processing transaction: %v", err)
						}
					}(txBytes.Bytes())
				}
			}
		}(conn)
	}

	return nil
}

func (ps *PropagationServer) quicServer(_ context.Context, quicAddresses string) error {
	ps.logger.Infof("Starting QUIC listeners on %s", quicAddresses)

	tlsConfig, err := ps.generateTLSConfig()
	if err != nil {
		return errors.NewInvalidArgumentError("error generating TLS config", err)
	}

	server := http3.Server{
		Addr:      quicAddresses,
		TLSConfig: tlsConfig, // Assume generateTLSConfig() sets up your TLS
	}

	http.Handle("/tx", http.HandlerFunc(ps.handleStream))

	err = server.ListenAndServe() // Empty because certs are in TLSConfig
	if err != nil {
		ps.logger.Errorf("error starting HTTP server: %v", err)
		return err
	}

	return nil
}

func (ps *PropagationServer) handleStream(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var (
		txLength uint32
		err      error
		txData   []byte
	)

	for {
		// Read the size of the incoming transaction first
		err = binary.Read(r.Body, binary.BigEndian, &txLength)
		if err != nil {
			if err != io.EOF {
				ps.logger.Errorf("Error reading transaction length: %v\n", err)
			}

			break
		}

		if txLength == 0 {
			return
		}

		// Read the transaction data
		txData = make([]byte, txLength)

		_, err := io.ReadFull(r.Body, txData)
		if err != nil {
			ps.logger.Errorf("Error reading transaction data: %v\n", err)
			break
		}

		// Process the received bytes
		ctx := context.Background()
		go func(txb []byte) {
			if _, err = ps.ProcessTransaction(ctx, &propagation_api.ProcessTransactionRequest{
				Tx: txb,
			}); err != nil {
				ps.logger.Errorf("error processing transaction: %v", err)
			}
		}(txData)
	}
}

func (ps *PropagationServer) Stop(_ context.Context) error {
	return nil
}

func (ps *PropagationServer) ProcessTransactionHex(ctx context.Context, req *propagation_api.ProcessTransactionHexRequest) (*propagation_api.EmptyMessage, error) {
	start, stat, _ := tracing.NewStatFromContext(ctx, "ProcessTransactionHex", ps.stats)
	defer func() {
		stat.AddTime(start)
	}()

	txBytes, err := hex.DecodeString(req.Tx)
	if err != nil {
		return nil, errors.WrapGRPC(err)
	}

	return ps.ProcessTransaction(ctx, &propagation_api.ProcessTransactionRequest{
		Tx: txBytes,
	})
}

func (ps *PropagationServer) ProcessTransaction(ctx context.Context, req *propagation_api.ProcessTransactionRequest) (*propagation_api.EmptyMessage, error) {
	if err := ps.processTransaction(ctx, req); err != nil {
		return nil, errors.WrapGRPC(err)
	}

	return &propagation_api.EmptyMessage{}, nil
}

func (ps *PropagationServer) ProcessTransactionBatch(ctx context.Context, req *propagation_api.ProcessTransactionBatchRequest) (*propagation_api.ProcessTransactionBatchResponse, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "ProcessTransactionBatch",
		tracing.WithParentStat(ps.stats),
		tracing.WithHistogram(prometheusProcessedTransactionBatch),
		tracing.WithDebugLogMessage(ps.logger, "[ProcessTransactionBatch] called for %d transactions", len(req.Tx)),
	)
	defer deferFn()

	response := &propagation_api.ProcessTransactionBatchResponse{
		Error: make([]string, len(req.Tx)),
	}

	g, gCtx := errgroup.WithContext(ctx)

	for idx, tx := range req.Tx {
		idx := idx
		tx := tx

		g.Go(func() error {
			// just call the internal process transaction function for every transaction
			err := ps.processTransaction(gCtx, &propagation_api.ProcessTransactionRequest{
				Tx: tx,
			})
			if err != nil {
				// TODO how can we send the real error back and not just a string?
				response.Error[idx] = RemoveInvalidUTF8(err.Error())
			} else {
				response.Error[idx] = ""
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, errors.WrapGRPC(err)
	}

	return response, nil
}

func (ps *PropagationServer) processTransaction(ctx context.Context, req *propagation_api.ProcessTransactionRequest) error {
	_, _, deferFn := tracing.StartTracing(ctx, "processTransaction",
		tracing.WithParentStat(ps.stats),
	)
	defer deferFn()

	timeStart := time.Now()

	btTx, err := bt.NewTxFromBytes(req.Tx)
	if err != nil {
		prometheusInvalidTransactions.Inc()
		return errors.NewProcessingError("[ProcessTransaction] failed to parse transaction from bytes", err)
	}

	// Do not allow propagation of coinbase transactions
	if btTx.IsCoinbase() {
		prometheusInvalidTransactions.Inc()
		return errors.NewTxInvalidError("[ProcessTransaction][%s] received coinbase transaction", btTx.TxID())
	}

	if !btTx.IsExtended() {
		return errors.NewTxInvalidError("[ProcessTransaction][%s] transaction is not extended", btTx.TxID())
	}

	// decouple the tracing context to not cancel the context when the tx is being saved in the background
	callerSpan := tracing.DecoupleTracingSpan(ctx, "decoupleStoreTransaction")
	defer callerSpan.Finish()

	// we should store all transactions, if this fails we should not validate the transaction
	if err = ps.storeTransaction(callerSpan.Ctx, btTx); err != nil {
		return errors.NewStorageError("[ProcessTransaction][%s] failed to save transaction", btTx.TxIDChainHash(), err)
	}

	if ps.validatorKafkaProducerClient != nil {
		validatorData := &validator.TxValidationData{
			Tx:     req.Tx,
			Height: util.GenesisActivationHeight,
		}

		ps.logger.Debugf("[ProcessTransaction][%s] sending transaction to validator kafka channel", btTx.TxID())
		ps.validatorKafkaProducerClient.PublishChannel <- &kafka.Message{
			Value: validatorData.Bytes(),
		}
	} else {
		ps.logger.Debugf("[ProcessTransaction][%s] Calling validate function", btTx.TxID())

		// All transactions entering Teranode can be assumed to be after Genesis activation height
		if err = ps.validator.Validate(ctx, btTx, util.GenesisActivationHeight); err != nil {
			err = errors.NewServiceError("failed validating transaction", err)
			ps.logger.Errorf("[ProcessTransaction][%s] failed to validate transaction: %v", btTx.TxID(), err)

			prometheusInvalidTransactions.Inc()
			return err
		}
	}

	prometheusTransactionSize.Observe(float64(len(req.Tx)))
	prometheusProcessedTransactions.Observe(float64(time.Since(timeStart).Microseconds()) / 1_000_000)

	return nil
}

func (ps *PropagationServer) ProcessTransactionStream(stream propagation_api.PropagationAPI_ProcessTransactionStreamServer) error {
	start := gocore.CurrentTime()
	defer func() {
		ps.stats.NewStat("ProcessTransactionStream", true).AddTime(start)
	}()

	for {
		req, err := stream.Recv()
		if err != nil {
			return errors.WrapGRPC(err)
		}

		resp, err := ps.ProcessTransaction(stream.Context(), req)
		if err != nil {
			return errors.WrapGRPC(err)
		}

		if err = stream.Send(resp); err != nil {
			return errors.WrapGRPC(err)
		}
	}
}

func (ps *PropagationServer) ProcessTransactionDebug(ctx context.Context, req *propagation_api.ProcessTransactionRequest) (*propagation_api.EmptyMessage, error) {
	start := gocore.CurrentTime()
	defer func() {
		ps.stats.NewStat("ProcessTransactionDebug", true).AddTime(start)
	}()

	btTx, err := bt.NewTxFromBytes(req.Tx)
	if err != nil {
		return nil, errors.WrapGRPC(errors.NewProcessingError("failed to parse transaction from bytes", err))
	}

	if err = ps.storeTransaction(ctx, btTx); err != nil {
		return nil, errors.WrapGRPC(errors.NewStorageError("failed to save transaction %s", btTx.TxIDChainHash().String(), err))
	}
	return &propagation_api.EmptyMessage{}, nil
}

func (ps *PropagationServer) storeTransaction(ctx context.Context, btTx *bt.Tx) error {
	ctx, _, deferFn := tracing.StartTracing(ctx, "PropagationServer:Set:Store")
	defer deferFn()

	// TODO (Gokhan): add retry
	if err := ps.txStore.Set(ctx, btTx.TxIDChainHash().CloneBytes(), btTx.ExtendedBytes()); err != nil {
		// TODO make this resilient to errors
		// write it to secondary store (Kafka, Badger) and retry?
		return err
	}

	return nil
}

// Setup a bare-bones TLS config for the server
func (ps *PropagationServer) generateTLSConfig() (*tls.Config, error) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, errors.NewError("error generating rsa key", err)
	}

	template := x509.Certificate{SerialNumber: big.NewInt(1)}

	remoteAddress, err := utils.GetPublicIPAddress()
	if err != nil {
		return nil, errors.NewServiceError("failed to get public IP address", err)
	}
	// Add IP SANs
	template.IPAddresses = []net.IP{net.ParseIP(remoteAddress)}

	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		return nil, errors.NewError("error creating x509 certificate", err)
	}

	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})

	tlsCert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, errors.NewError("error generating x509 key pair", err)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{tlsCert},
		NextProtos:   []string{"txblaster2"},
		MinVersion:   tls.VersionTLS12,
	}, nil
}

// RemoveInvalidUTF8 returns a string with all invalid UTF-8 characters removed
func RemoveInvalidUTF8(s string) string {
	var buf []rune

	for _, r := range s {
		if r == utf8.RuneError {
			continue
		}

		buf = append(buf, r)
	}
	return string(buf)
}
