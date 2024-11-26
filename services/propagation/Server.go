// Package propagation implements Bitcoin SV transaction propagation and validation services.
// It provides functionality for processing, validating, and distributing BSV transactions
// across the network using multiple protocols including GRPC, UDP6 multicast, and QUIC.
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
	"strings"
	"time"
	"unicode/utf8"

	"github.com/bitcoin-sv/ubsv/chaincfg"
	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/services/legacy/wire"
	"github.com/bitcoin-sv/ubsv/services/propagation/propagation_api"
	"github.com/bitcoin-sv/ubsv/services/validator"
	"github.com/bitcoin-sv/ubsv/settings"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/tracing"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/bitcoin-sv/ubsv/util/health"
	"github.com/bitcoin-sv/ubsv/util/kafka"
	"github.com/libsv/go-bt/v2"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	http3 "github.com/quic-go/quic-go/http3"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (

	// maxDatagramSize defines the maximum size of UDP datagrams for IPv6 multicast
	maxDatagramSize = 512 // 100 * 1024 * 1024
	// ipv6Port defines the default port used for IPv6 multicast listeners
	ipv6Port = 9999
)

// PropagationServer implements the transaction propagation service for Bitcoin SV.
// It handles transaction validation, storage, and distribution across the network
// while managing connections to various backend services including validators,
// blockchain clients, and Kafka producers.
type PropagationServer struct {
	propagation_api.UnsafePropagationAPIServer
	logger                       ulogger.Logger
	settings                     *settings.Settings
	stats                        *gocore.Stat
	txStore                      blob.Store
	validator                    validator.Interface
	blockchainClient             blockchain.ClientI
	validatorKafkaProducerClient kafka.KafkaAsyncProducerI
}

// New creates a new PropagationServer instance with the specified dependencies.
// It initializes Prometheus metrics and configures the server with required services.
//
// Parameters:
//   - logger: logging interface for server operations
//   - txStore: storage interface for persisting transactions
//   - validatorClient: service for transaction validation
//   - blockchainClient: interface to blockchain operations
//   - validatorKafkaProducerClient: Kafka producer for async validation
//
// Returns:
//   - *PropagationServer: configured server instance
func New(logger ulogger.Logger, tSettings *settings.Settings, txStore blob.Store, validatorClient validator.Interface, blockchainClient blockchain.ClientI, validatorKafkaProducerClient kafka.KafkaAsyncProducerI) *PropagationServer {
	initPrometheusMetrics()

	return &PropagationServer{
		logger:                       logger,
		settings:                     tSettings,
		stats:                        gocore.NewStat("propagation"),
		txStore:                      txStore,
		validator:                    validatorClient,
		blockchainClient:             blockchainClient,
		validatorKafkaProducerClient: validatorKafkaProducerClient,
	}
}

// Health performs health checks on the server and its dependencies.
// When checkLiveness is true, it performs basic liveness checks.
// Otherwise, it performs readiness checks including dependency verification.
//
// Parameters:
//   - ctx: context for the health check operation
//   - checkLiveness: boolean indicating whether to perform liveness check
//
// Returns:
//   - int: HTTP status code indicating health status
//   - string: detailed health status message
//   - error: error if health check fails
func (ps *PropagationServer) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	if checkLiveness {
		// Add liveness checks here. Don't include dependency checks.
		// If the service is stuck return http.StatusServiceUnavailable
		// to indicate a restart is needed
		return http.StatusOK, "OK", nil
	}

	var brokersURL []string
	if ps.validatorKafkaProducerClient != nil { // tests may not set this
		brokersURL = ps.validatorKafkaProducerClient.BrokersURL()
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
		{Name: "Kafka", Check: kafka.HealthChecker(ctx, brokersURL)},
	}

	return health.CheckAll(ctx, checkLiveness, checks)
}

// HealthGRPC implements the gRPC health check endpoint for the propagation service.
// It performs readiness checks on the server and its dependencies, returning the results
// in a gRPC-friendly format.
//
// Parameters:
//   - ctx: context for the health check operation
//   - _: empty message parameter (unused)
//
// Returns:
//   - *propagation_api.HealthResponse: health check response including status and timestamp
//   - error: error if health check fails
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

// Init initializes the PropagationServer.
// Currently a no-op, reserved for future initialization needs.
//
// Parameters:
//   - ctx: context for initialization (unused)
//
// Returns:
//   - error: always returns nil in current implementation
func (ps *PropagationServer) Init(_ context.Context) (err error) {
	return nil
}

// Start initializes and starts the PropagationServer services including:
// - FSM state restoration if configured
// - UDP6 multicast listeners
// - QUIC server for high-throughput transaction processing
// - Kafka producer initialization
// - GRPC server setup
//
// The function blocks until the GRPC server is running or an error occurs.
//
// Parameters:
//   - ctx: context for the start operation
//
// Returns:
//   - error: error if server fails to start
func (ps *PropagationServer) Start(ctx context.Context) (err error) {
	// Check if we need to Restore. If so, move FSM to the Restore state
	// Restore will block and wait for RUN event to be manually sent
	// TODO: think if we can automate transition to RUN state after restore is complete.
	if ps.settings.BlockChain.FSMStateRestore {
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

	ipv6Addresses := ps.settings.Propagation.IPv6Addresses
	if ipv6Addresses != "" {
		err = ps.StartUDP6Listeners(ctx, ipv6Addresses)
		if err != nil {
			return errors.NewServiceError("error starting ipv6 listeners", err)
		}
	}

	// Experimental QUIC server - to test throughput at scale
	quicAddress := ps.settings.Propagation.QuicListenAddress
	if quicAddress != "" {
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

	if ps.validatorKafkaProducerClient != nil {
		ps.validatorKafkaProducerClient.Start(ctx, make(chan *kafka.Message, 10_000))
	}

	// this will block
	maxConnectionAge := ps.settings.Propagation.GRPCMaxConnectionAge
	if err = util.StartGRPCServer(ctx, ps.logger, "propagation", func(server *grpc.Server) {
		propagation_api.RegisterPropagationAPIServer(server, ps)
	}, maxConnectionAge); err != nil {
		return err
	}

	return nil
}

// StartUDP6Listeners initializes IPv6 multicast listeners for transaction propagation.
// It creates UDP listeners on specified interfaces and addresses, processing incoming
// transactions in separate goroutines.
//
// Parameters:
//   - ctx: context for the UDP listener operations
//   - ipv6Addresses: comma-separated list of IPv6 multicast addresses
//
// Returns:
//   - error: error if listeners fail to start
func (ps *PropagationServer) StartUDP6Listeners(ctx context.Context, ipv6Addresses string) error {
	ps.logger.Infof("Starting UDP6 listeners on %s", ipv6Addresses)

	ipv6Interface := ps.settings.Propagation.IPv6Interface
	if ipv6Interface == "" {
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
				var ok bool

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

// quicServer starts a QUIC protocol server for high-throughput transaction processing.
// It sets up TLS configuration and handles incoming transaction streams.
//
// Parameters:
//   - ctx: context for the QUIC server operation
//   - quicAddresses: address string to listen on
//
// Returns:
//   - error: error if server fails to start or encounters runtime errors
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

// handleStream processes incoming transaction streams over HTTP.
// It reads transaction length and data from the request body and processes
// each transaction asynchronously.
//
// Parameters:
//   - w: HTTP response writer
//   - r: HTTP request containing transaction data
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

// Stop gracefully stops the PropagationServer.
// Currently a no-op, reserved for future cleanup operations.
//
// Parameters:
//   - ctx: context for stop operation (unused)
//
// Returns:
//   - error: always returns nil in current implementation
func (ps *PropagationServer) Stop(_ context.Context) error {
	return nil
}

// ProcessTransactionHex processes a transaction provided in hexadecimal format.
// It converts the hex string to bytes and forwards to the main transaction processor.
//
// Parameters:
//   - ctx: context for the transaction processing
//   - req: request containing transaction in hex format
//
// Returns:
//   - *propagation_api.EmptyMessage: empty response on success
//   - error: error if processing fails
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

// ProcessTransaction validates and stores a single transaction.
// It performs the following steps:
// 1. Validates transaction format
// 2. Verifies it's not a coinbase transaction
// 3. Ensures transaction is extended
// 4. Stores the transaction
// 5. Triggers validation through validator service or Kafka
//
// Parameters:
//   - ctx: context for the transaction processing
//   - req: transaction processing request containing raw transaction data
//
// Returns:
//   - *propagation_api.EmptyMessage: empty response on success
//   - error: error if transaction processing fails
func (ps *PropagationServer) ProcessTransaction(ctx context.Context, req *propagation_api.ProcessTransactionRequest) (*propagation_api.EmptyMessage, error) {
	if err := ps.processTransaction(ctx, req); err != nil {
		return nil, errors.WrapGRPC(err)
	}

	return &propagation_api.EmptyMessage{}, nil
}

// ProcessTransactionBatch processes multiple transactions concurrently.
// It uses error groups to handle parallel processing while maintaining
// proper error handling and context cancellation.
//
// Parameters:
//   - ctx: context for the batch processing operation
//   - req: batch request containing multiple transactions
//
// Returns:
//   - *propagation_api.ProcessTransactionBatchResponse: response containing error status for each transaction
//   - error: error if batch processing fails
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

// processTransaction handles the core transaction processing logic.
// It validates, stores, and triggers async validation of a transaction,
// updating metrics throughout the process.
//
// Parameters:
//   - ctx: context for transaction processing
//   - req: transaction processing request
//
// Returns:
//   - error: error if any processing step fails
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
			Height: chaincfg.GenesisActivationHeight,
		}

		ps.logger.Debugf("[ProcessTransaction][%s] sending transaction to validator kafka channel", btTx.TxID())
		ps.validatorKafkaProducerClient.Publish(&kafka.Message{
			Value: validatorData.Bytes(),
		})
	} else {
		ps.logger.Debugf("[ProcessTransaction][%s] Calling validate function", btTx.TxID())

		// All transactions entering Teranode can be assumed to be after Genesis activation height
		if err = ps.validator.Validate(ctx, btTx, chaincfg.GenesisActivationHeight); err != nil {
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

// ProcessTransactionStream handles a bidirectional stream of transactions.
// It continuously receives transactions from the stream, processes them,
// and sends back results.
//
// Parameters:
//   - stream: bidirectional gRPC stream for transaction processing
//
// Returns:
//   - error: error if stream processing fails
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

// ProcessTransactionDebug provides a debug endpoint for transaction processing.
// It only performs basic transaction parsing and storage, skipping validation.
//
// Parameters:
//   - ctx: context for debug processing
//   - req: transaction processing request
//
// Returns:
//   - *propagation_api.EmptyMessage: empty response on success
//   - error: error if processing fails
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

// storeTransaction persists a transaction to the configured storage backend.
// It stores the transaction using its chain hash as the key and extended
// bytes as the value.
//
// Parameters:
//   - ctx: context for the storage operation
//   - btTx: Bitcoin transaction to store
//
// Returns:
//   - error: error if storage operation fails
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

// generateTLSConfig creates a TLS configuration for the QUIC server.
// It generates a self-signed certificate using the server's public IP address.
//
// Returns:
//   - *tls.Config: TLS configuration for QUIC server
//   - error: error if TLS configuration generation fails
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

// RemoveInvalidUTF8 sanitizes a string by removing invalid UTF-8 characters.
// This is used to clean error messages before sending them to clients.
//
// Parameters:
//   - s: string to sanitize
//
// Returns:
//   - string: sanitized string with valid UTF-8 characters only
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
