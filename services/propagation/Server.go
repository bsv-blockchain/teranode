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
	"errors"
	"fmt"
	"io"
	"log"
	"math/big"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/bitcoin-sv/ubsv/services/legacy/wire"
	"github.com/bitcoin-sv/ubsv/services/propagation/propagation_api"
	"github.com/bitcoin-sv/ubsv/services/validator"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/tracing"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/opentracing/opentracing-go"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	http3 "github.com/quic-go/quic-go/http3"
	"google.golang.org/grpc"
)

var (
	propagationStat = gocore.NewStat("propagation")

	// ipv6 multicast constants
	maxDatagramSize = 512 //100 * 1024 * 1024
	ipv6Port        = 9999

	ErrBadRequest = errors.New("PROPAGATION_BAD_REQUEST")
	ErrInternal   = errors.New("PROPAGATION_INTERNAL")
)

// PropagationServer type carries the logger within it
type PropagationServer struct {
	status atomic.Uint32
	propagation_api.UnsafePropagationAPIServer
	logger    ulogger.Logger
	txStore   blob.Store
	validator validator.Interface
}

// New will return a server instance with the logger stored within it
func New(logger ulogger.Logger, txStore blob.Store, validatorClient validator.Interface) *PropagationServer {
	initPrometheusMetrics()

	return &PropagationServer{
		logger:    logger,
		txStore:   txStore,
		validator: validatorClient,
	}
}

func (ps *PropagationServer) Health(ctx context.Context) (int, string, error) {
	return 0, "", nil
}

func (ps *PropagationServer) Init(_ context.Context) (err error) {
	ps.status.Store(1)
	return nil
}

// Start function
func (ps *PropagationServer) Start(ctx context.Context) (err error) {
	ipv6Addresses, ok := gocore.Config().Get("ipv6_addresses")
	if ok {
		err = ps.StartUDP6Listeners(ctx, ipv6Addresses)
		if err != nil {
			return fmt.Errorf("error starting ipv6 listeners: %v", err)
		}
	}

	// Experimental QUIC server - to test throughput at scale
	quicAddress, ok := gocore.Config().Get("propagation_quicListenAddress")
	if ok {
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

	ps.status.Store(2)

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
		return fmt.Errorf("error resolving interface: %v", err)
	}

	for _, ipv6Address := range strings.Split(ipv6Addresses, ",") {
		var conn *net.UDPConn
		conn, err = net.ListenMulticastUDP("udp6", useInterface, &net.UDPAddr{
			IP:   net.ParseIP(ipv6Address),
			Port: ipv6Port,
			Zone: useInterface.Name,
		})
		if err != nil {
			log.Fatalf("error starting listener: %v", err)
		}

		go func(conn *net.UDPConn) {
			// Loop forever reading from the socket
			//var numBytes int
			var src *net.UDPAddr
			//var oobn int
			//var flags int
			var msg wire.Message
			var b []byte
			var oobB []byte
			var msgTx *wire.MsgExtendedTx

			buffer := make([]byte, maxDatagramSize)
			for {
				_, _, _, src, err = conn.ReadMsgUDP(buffer, oobB)
				if err != nil {
					log.Fatal("ReadFromUDP failed:", err)
				}
				//ps.logger.Infof("read %d bytes from %s, out of bounds data len %d", len(buffer), src.String(), len(oobB))

				reader := bytes.NewReader(buffer)
				msg, b, err = wire.ReadMessage(reader, wire.ProtocolVersion, wire.MainNet)
				if err != nil {
					ps.logger.Errorf("wire.ReadMessage failed: %v", err)
				}
				ps.logger.Infof("read %d bytes into wire message from %s", len(b), src.String())
				//ps.logger.Infof("wire message type: %v", msg)

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

func (ps *PropagationServer) quicServer(ctx context.Context, quicAddresses string) error {
	ps.logger.Infof("Starting QUIC listeners on %s", quicAddresses)

	server := http3.Server{
		Addr:      quicAddresses,
		TLSConfig: ps.generateTLSConfig(), // Assume generateTLSConfig() sets up your TLS
	}

	http.Handle("/tx", http.HandlerFunc(ps.handleStream))

	err := server.ListenAndServe() // Empty because certs are in TLSConfig
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
	var txLength uint32
	var err error
	var txData []byte
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

func (ps *PropagationServer) storeHealth(ctx context.Context) (int, string, error) {
	var sb strings.Builder
	errs := make([]error, 0)

	code, details, err := ps.txStore.Health(ctx)
	if err != nil {
		errs = append(errs, err)
		_, _ = sb.WriteString(fmt.Sprintf("TxStore: BAD %d - %q: %v\n", code, details, err))
	} else {
		_, _ = sb.WriteString(fmt.Sprintf("TxStore: GOOD %d - %q\n", code, details))
	}

	code, details, err = ps.validator.Health(ctx)
	if err != nil {
		errs = append(errs, err)
		_, _ = sb.WriteString(fmt.Sprintf("Validator: BAD %d - %q: %v\n", code, details, err))
	} else {
		_, _ = sb.WriteString(fmt.Sprintf("Validator: GOOD %d - %q\n", code, details))
	}

	localValidator := gocore.Config().GetBool("useLocalValidator", false)
	if localValidator {
		blockHeight, err := ps.validator.GetBlockHeight()
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
	}

	if len(errs) > 0 {
		return -1, sb.String(), errors.New("Health errors occurred")
	}

	return 0, sb.String(), nil
}

func (ps *PropagationServer) HealthGRPC(ctx context.Context, _ *propagation_api.EmptyMessage) (*propagation_api.HealthResponse, error) {
	start := gocore.CurrentTime()
	defer func() {
		propagationStat.NewStat("Health", true).AddTime(start)
	}()

	prometheusHealth.Inc()

	status := ps.status.Load()

	if status != 2 {
		return &propagation_api.HealthResponse{
			Ok:        false,
			Details:   fmt.Sprintf("Propagation server is not ready (Status=%d)", status),
			Timestamp: uint32(time.Now().Unix()),
		}, nil
	}

	code, details, err := ps.storeHealth(ctx)
	if err != nil {
		return &propagation_api.HealthResponse{
			Ok:        false,
			Details:   details,
			Timestamp: uint32(time.Now().Unix()),
		}, err
	}

	if code != 0 {
		return &propagation_api.HealthResponse{
			Ok:        false,
			Details:   details,
			Timestamp: uint32(time.Now().Unix()),
		}, nil
	}

	return &propagation_api.HealthResponse{
		Ok:        true,
		Timestamp: uint32(time.Now().Unix()),
	}, nil
}

func (ps *PropagationServer) ProcessTransactionHex(cntxt context.Context, req *propagation_api.ProcessTransactionHexRequest) (*propagation_api.EmptyMessage, error) {
	start, stat, ctx := util.NewStatFromContext(cntxt, "ProcessTransactionHex", propagationStat)
	defer func() {
		stat.AddTime(start)
	}()

	txBytes, err := hex.DecodeString(req.Tx)
	if err != nil {
		return nil, err
	}

	return ps.ProcessTransaction(ctx, &propagation_api.ProcessTransactionRequest{
		Tx: txBytes,
	})
}

func (ps *PropagationServer) ProcessTransaction(cntxt context.Context, req *propagation_api.ProcessTransactionRequest) (*propagation_api.EmptyMessage, error) {
	start, stat, ctx := util.NewStatFromContext(cntxt, "ProcessTransaction", propagationStat)
	defer func() {
		stat.AddTime(start)
	}()

	prometheusProcessedTransactions.Inc()
	traceSpan := tracing.Start(ctx, "PropagationServer:Set")
	defer traceSpan.Finish()

	timeStart := time.Now()
	btTx, err := bt.NewTxFromBytes(req.Tx)
	if err != nil {
		prometheusInvalidTransactions.Inc()
		return nil, fmt.Errorf("[ProcessTransaction] failed to parse transaction from bytes: %w", err)
	}

	// Do not allow propagation of coinbase transactions
	if btTx.IsCoinbase() {
		prometheusInvalidTransactions.Inc()
		return nil, fmt.Errorf("[ProcessTransaction][%s] received coinbase transaction: %w", btTx.TxID(), ErrBadRequest)
	}

	if !btTx.IsExtended() {
		return nil, fmt.Errorf("[ProcessTransaction][%s] transaction is not extended: %w", btTx.TxID(), ErrBadRequest)
	}

	// decouple the tracing context to not cancel the context when the tx is being saved in the background
	callerSpan := opentracing.SpanFromContext(ctx)
	setCtx := opentracing.ContextWithSpan(context.Background(), callerSpan)

	// we should store all transactions, if this fails we should not validate the transaction
	if err = ps.storeTransaction(setCtx, btTx); err != nil {
		return nil, fmt.Errorf("[ProcessTransaction][%s] failed to save transaction: %v: %w", btTx.TxIDChainHash(), err, ErrInternal)
	}

	// All transactions entering Teranode can be assumed to be after Genesis activation height
	if err = ps.validator.Validate(ctx, btTx, util.GenesisActivationHeight); err != nil {
		if errors.Is(err, validator.ErrInternal) {
			err = fmt.Errorf("%v: %w", err, ErrInternal)
		} else if errors.Is(err, validator.ErrBadRequest) {
			err = fmt.Errorf("%v: %w", err, ErrBadRequest)
		}

		prometheusInvalidTransactions.Inc()
		return nil, err
	}

	prometheusTransactionSize.Observe(float64(len(req.Tx)))
	prometheusTransactionDuration.Observe(float64(time.Since(timeStart).Microseconds()) / 1_000_000)

	return &propagation_api.EmptyMessage{}, nil
}

func (ps *PropagationServer) ProcessTransactionStream(stream propagation_api.PropagationAPI_ProcessTransactionStreamServer) error {
	start := gocore.CurrentTime()
	defer func() {
		propagationStat.NewStat("ProcessTransactionStream", true).AddTime(start)
	}()

	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}

		resp, err := ps.ProcessTransaction(stream.Context(), req)
		if err != nil {
			return err
		}

		if err := stream.Send(resp); err != nil {
			return err
		}
	}
}

func (ps *PropagationServer) ProcessTransactionDebug(ctx context.Context, req *propagation_api.ProcessTransactionRequest) (*propagation_api.EmptyMessage, error) {
	start := gocore.CurrentTime()
	defer func() {
		propagationStat.NewStat("ProcessTransactionDebug", true).AddTime(start)
	}()

	btTx, err := bt.NewTxFromBytes(req.Tx)
	if err != nil {
		return nil, fmt.Errorf("failed to parse transaction from bytes: %s", err.Error())
	}
	if err := ps.storeTransaction(ctx, btTx); err != nil {
		return nil, fmt.Errorf("failed to save transaction %s: %s", btTx.TxIDChainHash().String(), err.Error())
	}
	return &propagation_api.EmptyMessage{}, nil
}

func (ps *PropagationServer) storeTransaction(setCtx context.Context, btTx *bt.Tx) error {
	span, spanCtx := opentracing.StartSpanFromContext(setCtx, "PropagationServer:Set:Store")
	defer span.Finish()

	if err := ps.txStore.Set(spanCtx, btTx.TxIDChainHash().CloneBytes(), btTx.ExtendedBytes()); err != nil {
		// TODO make this resilient to errors
		// write it to secondary store (Kafka, Badger) and retry?
		return err
	}

	return nil
}

// Setup a bare-bones TLS config for the server
func (ps *PropagationServer) generateTLSConfig() *tls.Config {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		ps.logger.Errorf("error generating rsa key: %s", err.Error())
		return nil
	}

	template := x509.Certificate{SerialNumber: big.NewInt(1)}

	remoteAddress, err := utils.GetPublicIPAddress()
	if err != nil {
		ps.logger.Fatalf("Failed to get public IP address: %v", err)
	}
	// Add IP SANs
	template.IPAddresses = []net.IP{net.ParseIP(remoteAddress)}

	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		ps.logger.Errorf("error creating x509 certificate: %s", err.Error())
		return nil
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	tlsCert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		ps.logger.Errorf("error generating x509 key pair: %s", err.Error())
		return nil
	}
	return &tls.Config{
		Certificates: []tls.Certificate{tlsCert},
		NextProtos:   []string{"txblaster2"},
		MinVersion:   tls.VersionTLS12,
	}
}
