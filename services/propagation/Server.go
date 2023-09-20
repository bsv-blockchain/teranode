package propagation

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/bitcoin-sv/ubsv/services/propagation/propagation_api"
	"github.com/bitcoin-sv/ubsv/services/validator"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/tracing"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-p2p/wire"
	"github.com/opentracing/opentracing-go"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	// ipv6 multicast constants
	maxDatagramSize = 512 //100 * 1024 * 1024
	ipv6Port        = 9999
)

// PropagationServer type carries the logger within it
type PropagationServer struct {
	propagation_api.UnsafePropagationAPIServer
	logger    utils.Logger
	txStore   blob.Store
	validator validator.Interface
}

func Enabled() bool {
	_, found := gocore.Config().Get("utxostore_grpcAddress")
	return found
}

// New will return a server instance with the logger stored within it
func New(logger utils.Logger, txStore blob.Store, validatorClient validator.Interface) *PropagationServer {
	initPrometheusMetrics()

	return &PropagationServer{
		logger:    logger,
		txStore:   txStore,
		validator: validatorClient,
	}
}

func (u *PropagationServer) Init(_ context.Context) (err error) {
	return nil
}

// Start function
func (u *PropagationServer) Start(ctx context.Context) (err error) {
	ipv6Addresses, ok := gocore.Config().Get("ipv6_addresses")
	if ok {
		err = u.StartUDP6Listeners(ctx, ipv6Addresses)
		if err != nil {
			return fmt.Errorf("error starting ipv6 listeners: %v", err)
		}
	}

	httpAddress, ok := gocore.Config().Get("propagation_httpAddress")
	if ok {
		var serverURL *url.URL
		serverURL, err = url.Parse(httpAddress)
		if err != nil {
			return fmt.Errorf("HTTP server failed to parse URL [%w]", err)
		}
		err = u.StartHTTPServer(ctx, serverURL)
		if err != nil {
			return fmt.Errorf("HTTP server failed [%w]", err)
		}
	}

	// this will block
	if err = util.StartGRPCServer(ctx, u.logger, "propagation", func(server *grpc.Server) {
		propagation_api.RegisterPropagationAPIServer(server, u)
	}); err != nil {
		return err
	}

	return nil
}

func (u *PropagationServer) StartUDP6Listeners(ctx context.Context, ipv6Addresses string) error {
	u.logger.Infof("Starting UDP6 listeners on %s", ipv6Addresses)

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
				//u.logger.Infof("read %d bytes from %s, out of bounds data len %d", len(buffer), src.String(), len(oobB))

				reader := bytes.NewReader(buffer)
				msg, b, err = wire.ReadMessage(reader, wire.ProtocolVersion, wire.MainNet)
				if err != nil {
					u.logger.Errorf("wire.ReadMessage failed: %v", err)
				}
				u.logger.Infof("read %d bytes into wire message from %s", len(b), src.String())
				//u.logger.Infof("wire message type: %v", msg)

				msgTx, ok = msg.(*wire.MsgExtendedTx)
				if ok {
					u.logger.Infof("received %d bytes from %s", len(b), src.String())

					txBytes := bytes.NewBuffer(nil)
					if err = msgTx.Serialize(txBytes); err != nil {
						u.logger.Errorf("error serializing transaction: %v", err)
						continue
					}

					// Process the received bytes
					go func(txb []byte) {
						if _, err = u.Set(ctx, &propagation_api.SetRequest{
							Tx: txb,
						}); err != nil {
							u.logger.Errorf("error processing transaction: %v", err)
						}
					}(txBytes.Bytes())
				}
			}
		}(conn)
	}

	return nil
}

func (u *PropagationServer) StartHTTPServer(ctx context.Context, serverURL *url.URL) error {
	// start a simple http listener that handles incoming transaction requests
	http.HandleFunc("/tx", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(err.Error()))
			return
		}
		if _, err = u.Set(ctx, &propagation_api.SetRequest{
			Tx: body,
		}); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(err.Error()))
			return
		}
	})

	go func() {
		if err := http.ListenAndServe(serverURL.Host, nil); err != nil {
			u.logger.Errorf("HTTP server failed [%s]", err)
		}
	}()

	return nil
}

func (u *PropagationServer) Stop(_ context.Context) error {
	return nil
}

func (u *PropagationServer) Health(_ context.Context, _ *emptypb.Empty) (*propagation_api.HealthResponse, error) {
	prometheusHealth.Inc()
	return &propagation_api.HealthResponse{
		Ok:        true,
		Timestamp: timestamppb.New(time.Now()),
	}, nil
}

func (u *PropagationServer) Get(ctx context.Context, req *propagation_api.GetRequest) (*propagation_api.GetResponse, error) {
	prometheusGetTransaction.Inc()

	tx, err := u.txStore.Get(ctx, req.Txid)
	if err != nil {
		return nil, err
	}

	return &propagation_api.GetResponse{
		Tx: tx,
	}, nil
}

func (u *PropagationServer) Set(ctx context.Context, req *propagation_api.SetRequest) (*emptypb.Empty, error) {
	prometheusProcessedTransactions.Inc()
	traceSpan := tracing.Start(ctx, "PropagationServer:Set")
	defer traceSpan.Finish()

	timeStart := time.Now()
	btTx, err := bt.NewTxFromBytes(req.Tx)
	if err != nil {
		prometheusInvalidTransactions.Inc()
		return &emptypb.Empty{}, fmt.Errorf("failed to parse transaction from bytes: %s", err.Error())
	}

	// Do not allow propagation of coinbase transactions
	if btTx.IsCoinbase() {
		prometheusInvalidTransactions.Inc()
		return &emptypb.Empty{}, fmt.Errorf("received coinbase transaction: %s", btTx.TxID())
	}

	if !util.IsExtended(btTx) {
		return &emptypb.Empty{}, fmt.Errorf("transaction is not extended: %s", btTx.TxID())
	}

	// decouple the tracing context to not cancel the context when the tx is being saved in the background
	callerSpan := opentracing.SpanFromContext(traceSpan.Ctx)
	setCtx := opentracing.ContextWithSpan(context.Background(), callerSpan)

	//go func() {
	if err = u.storeTransaction(setCtx, btTx); err != nil {
		u.logger.Errorf("failed to save transaction %s: %s", btTx.TxIDChainHash().String(), err.Error())
	}
	//}()

	if err = u.validator.Validate(traceSpan.Ctx, btTx); err != nil {
		// TODO send REJECT message to peers if invalid tx
		u.logger.Errorf("received invalid transaction: %s", err.Error())
		prometheusInvalidTransactions.Inc()
		return &emptypb.Empty{}, err
	}

	prometheusTransactionSize.Observe(float64(len(req.Tx)))
	prometheusTransactionDuration.Observe(float64(time.Since(timeStart).Microseconds()))

	return &emptypb.Empty{}, nil
}

func (u *PropagationServer) storeTransaction(setCtx context.Context, btTx *bt.Tx) error {
	span, spanCtx := opentracing.StartSpanFromContext(setCtx, "PropagationServer:Set:Store")
	defer span.Finish()

	if err := u.txStore.Set(spanCtx, btTx.TxIDChainHash().CloneBytes(), btTx.ExtendedBytes()); err != nil {
		// TODO make this resilient to errors
		// write it to secondary store (Kafka, Badger) and retry?
		return err
	}

	return nil
}
